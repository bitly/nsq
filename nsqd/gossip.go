package nsqd

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/registrationdb"
	"github.com/hashicorp/serf/serf"
)

type gossipEvent struct {
	Name    string `json:"n"`
	Topic   string `json:"t"`
	Channel string `json:"c"`
	Rnd     int64  `json:"r"`
}

func memberToProducer(member serf.Member) registrationdb.Producer {
	tcpPort, _ := strconv.Atoi(member.Tags["tp"])
	httpPort, _ := strconv.Atoi(member.Tags["hp"])
	return registrationdb.Producer{
		ID: member.Name,
		RemoteAddress: net.JoinHostPort(member.Addr.String(),
			strconv.Itoa(int(member.Port))),
		LastUpdate:       time.Now().UnixNano(),
		BroadcastAddress: member.Tags["ba"],
		Hostname:         member.Tags["h"],
		TCPPort:          tcpPort,
		HTTPPort:         httpPort,
		Version:          member.Tags["v"],
	}
}

func initSerf(opts *nsqdOptions,
	serfEventChan chan serf.Event,
	tcpAddr *net.TCPAddr, httpAddr *net.TCPAddr, httpsAddr *net.TCPAddr) (*serf.Serf, error) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	gossipAddr, err := net.ResolveTCPAddr("tcp", opts.GossipAddress)
	if err != nil {
		log.Fatal(err)
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.Init()
	serfConfig.Tags["role"] = "nsqd"
	serfConfig.Tags["tp"] = strconv.Itoa(tcpAddr.Port)
	serfConfig.Tags["hp"] = strconv.Itoa(httpAddr.Port)
	if httpsAddr != nil {
		serfConfig.Tags["hps"] = strconv.Itoa(httpsAddr.Port)
	}
	serfConfig.Tags["ba"] = opts.BroadcastAddress
	serfConfig.Tags["h"] = hostname
	serfConfig.Tags["v"] = util.BINARY_VERSION
	serfConfig.NodeName = net.JoinHostPort(opts.BroadcastAddress, strconv.Itoa(tcpAddr.Port))
	serfConfig.MemberlistConfig.BindAddr = gossipAddr.IP.String()
	serfConfig.MemberlistConfig.BindPort = gossipAddr.Port
	serfConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond
	serfConfig.MemberlistConfig.GossipNodes = 5
	serfConfig.EventCh = serfEventChan
	serfConfig.EventBuffer = 1024

	return serf.Create(serfConfig)
}

func (n *NSQD) serfMemberJoin(ev serf.Event) {
	memberEv := ev.(serf.MemberEvent)
	n.opts.Logger.Output(2, fmt.Sprintf("MEMBER EVENT: %+v - members: %+v",
		memberEv, memberEv.Members))
	for _, member := range memberEv.Members {
		producer := memberToProducer(member)
		r := registrationdb.Registration{"client", "", ""}
		n.rdb.AddProducer(r, producer)
		n.opts.Logger.Output(2, fmt.Sprintf(
			"DB: member(%s) REGISTER category:%s key:%s subkey:%s",
			producer.ID,
			r.Category,
			r.Key,
			r.SubKey))
	}
}

func (n *NSQD) serfMemberFailed(ev serf.Event) {
	memberEv := ev.(serf.MemberEvent)
	n.opts.Logger.Output(2, fmt.Sprintf("MEMBER EVENT: %+v - members: %+v",
		memberEv, memberEv.Members))
	for _, member := range memberEv.Members {
		registrations := n.rdb.LookupRegistrations(member.Name)
		for _, r := range registrations {
			if removed, _ := n.rdb.RemoveProducer(r, member.Name); removed {
				n.opts.Logger.Output(2, fmt.Sprintf(
					"DB: member(%s) UNREGISTER category:%s key:%s subkey:%s",
					member.Name,
					r.Category,
					r.Key,
					r.SubKey))
			}
		}
	}
}

func (n *NSQD) serfUserEvent(ev serf.Event) {
	var gev gossipEvent
	var member serf.Member

	userEv := ev.(serf.UserEvent)
	err := json.Unmarshal(userEv.Payload, &gev)
	if err != nil {
		n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to Unmarshal gossipEvent - %s", err))
		return
	}

	n.opts.Logger.Output(2, fmt.Sprintf("gossipEvent: %+v", gev))

	found := false
	for _, m := range n.serf.Members() {
		if m.Name == gev.Name {
			member = m
			found = true
		}
	}

	if !found {
		n.opts.Logger.Output(2, fmt.Sprintf(
			"ERROR: received gossipEvent for unknown node - %s",
			userEv.Name))
		return
	}

	producer := memberToProducer(member)
	operation := userEv.Name[len(userEv.Name)-1]
	switch operation {
	case '+', '=':
		n.gossipHandleCreateEvent(operation, producer, gev)
	case '-':
		n.gossipHandleDeleteEvent(operation, producer, gev)
	}
}

func (n *NSQD) gossipHandleCreateEvent(operation byte,
	producer registrationdb.Producer, gev gossipEvent) {
	var registrations []registrationdb.Registration

	if gev.Channel != "" {
		registrations = append(registrations, registrationdb.Registration{
			Category: "channel",
			Key:      gev.Topic,
			SubKey:   gev.Channel,
		})
	}

	registrations = append(registrations, registrationdb.Registration{
		Category: "topic",
		Key:      gev.Topic,
	})

	for _, r := range registrations {
		if n.rdb.AddProducer(r, producer) {
			n.opts.Logger.Output(2, fmt.Sprintf(
				"DB: member(%s) REGISTER category:%s key:%s subkey:%s",
				gev.Name,
				r.Category,
				r.Key,
				r.SubKey))
		}
		if operation == '=' && n.rdb.TouchRegistration(r.Category, r.Key, r.SubKey, producer.ID) {
			n.opts.Logger.Output(2, fmt.Sprintf(
				"DB: member(%s) TOUCH category:%s key:%s subkey:%s",
				gev.Name,
				r.Category,
				r.Key,
				r.SubKey))
		}
	}
}

func (n *NSQD) gossipHandleDeleteEvent(operation byte,
	producer registrationdb.Producer, gev gossipEvent) {
	if gev.Channel != "" {
		r := registrationdb.Registration{
			Category: "channel",
			Key:      gev.Topic,
			SubKey:   gev.Channel,
		}

		removed, left := n.rdb.RemoveProducer(r, producer.ID)
		if removed {
			n.opts.Logger.Output(2, fmt.Sprintf(
				"DB: member(%s) UNREGISTER category:%s key:%s subkey:%s",
				gev.Name,
				r.Category,
				r.Key,
				r.SubKey))
		}

		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(gev.Channel, "#ephemeral") {
			n.rdb.RemoveRegistration(r)
		}

		return
	}

	// no channel was specified so this is a topic unregistration
	// remove all of the channel registrations...
	// normally this shouldn't happen which is why we print a warning message
	// if anything is actually removed
	registrations := n.rdb.FindRegistrations("channel", gev.Topic, "*")
	for _, r := range registrations {
		if removed, _ := n.rdb.RemoveProducer(r, producer.ID); removed {
			n.opts.Logger.Output(2, fmt.Sprintf(
				"WARNING: client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
				gev.Name,
				r.Category,
				r.Key,
				r.SubKey))
		}
	}

	r := registrationdb.Registration{
		Category: "topic",
		Key:      gev.Topic,
		SubKey:   "",
	}
	if removed, _ := n.rdb.RemoveProducer(r, producer.ID); removed {
		n.opts.Logger.Output(2, fmt.Sprintf(
			"DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
			gev.Name,
			r.Category,
			r.Key,
			r.SubKey))
	}
}

func (n *NSQD) serfEventLoop() {
	for {
		select {
		case ev := <-n.serfEventChan:
			switch ev.EventType() {
			case serf.EventMemberJoin:
				n.serfMemberJoin(ev)
			case serf.EventMemberLeave:
				// nothing (should never happen)
			case serf.EventMemberFailed:
				n.serfMemberFailed(ev)
			case serf.EventMemberReap:
				// nothing
			case serf.EventUser:
				n.serfUserEvent(ev)
			case serf.EventQuery:
				// nothing
			case serf.EventMemberUpdate:
				// nothing
			default:
				n.opts.Logger.Output(2, fmt.Sprintf("WARNING: un-handled Serf event: %#v", ev))
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.opts.Logger.Output(2, fmt.Sprintf("SERF: exiting"))
}

func (n *NSQD) gossip(evName string, topicName string, channelName string) error {
	gev := gossipEvent{
		Name:    n.serf.LocalMember().Name,
		Topic:   topicName,
		Channel: channelName,
		Rnd:     time.Now().UnixNano(),
	}
	payload, err := json.Marshal(&gev)
	if err != nil {
		return err
	}
	return n.serf.UserEvent(evName, payload, false)
}

func (n *NSQD) gossipLoop() {
	var evName string
	var topicName string
	var channelName string

	regossipTicker := time.NewTicker(60 * time.Second)

	if len(n.opts.SeedNodeAddresses) > 0 {
		for {
			num, err := n.serf.Join(n.opts.SeedNodeAddresses, false)
			if err != nil {
				n.opts.Logger.Output(2, fmt.Sprintf("ERROR: failed to join serf - %s", err))
				select {
				case <-time.After(15 * time.Second):
					// keep trying
				case <-n.exitChan:
					goto exit
				}
			}
			if num > 0 {
				n.opts.Logger.Output(2, fmt.Sprintf("SERF: joined %d nodes", num))
				break
			}
		}
	}

	for {
		select {
		case <-regossipTicker.C:
			n.opts.Logger.Output(2, fmt.Sprintf("SERF: re-gossiping"))
			stats := n.GetStats()
			for _, topicStat := range stats {
				if len(topicStat.Channels) == 0 {
					// if there are no channels we just send a topic exists event
					err := n.gossip("topic=", topicStat.TopicName, "")
					if err != nil {
						n.opts.Logger.Output(2, fmt.Sprintf(
							"ERROR: failed to send Serf user event - %s", err))
					}
					continue
				}
				// otherwise we know that by sending over a channel for a topic the topic
				// will be accounted for as well
				for _, channelStat := range topicStat.Channels {
					err := n.gossip("channel=", topicStat.TopicName, channelStat.ChannelName)
					if err != nil {
						n.opts.Logger.Output(2, fmt.Sprintf(
							"ERROR: failed to send Serf user event - %s", err))
					}
				}
			}
		case v := <-n.gossipChan:
			switch v.(type) {
			case *Channel:
				channel := v.(*Channel)
				topicName = channel.topicName
				channelName = channel.name
				if !channel.Exiting() {
					evName = "channel+"
				} else {
					evName = "channel-"
				}
			case *Topic:
				topic := v.(*Topic)
				topicName = topic.name
				channelName = ""
				if !topic.Exiting() {
					evName = "topic+"
				} else {
					evName = "topic-"
				}
			}
			err := n.gossip(evName, topicName, channelName)
			if err != nil {
				n.opts.Logger.Output(2, fmt.Sprintf(
					"ERROR: failed to send Serf user event - %s", err))
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	regossipTicker.Stop()
	n.opts.Logger.Output(2, fmt.Sprintf("GOSSIP: exiting"))
}
