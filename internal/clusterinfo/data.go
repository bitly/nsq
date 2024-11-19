package clusterinfo

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/blang/semver"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/stringy"
)

type PartialErr interface {
	error
	Errors() []error
}

type ErrList []error

func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

func (l ErrList) Errors() []error {
	return l
}

type ClusterInfo struct {
	log    lg.AppLogFunc
	client *http_api.Client
}

func New(log lg.AppLogFunc, client *http_api.Client) *ClusterInfo {
	return &ClusterInfo{
		log:    log,
		client: client,
	}
}

func (c *ClusterInfo) logf(f string, args ...interface{}) {
	if c.log != nil {
		c.log(lg.INFO, f, args...)
	}
}

// GetVersion returns a semver.Version object by querying /info
func (c *ClusterInfo) GetVersion(addr string) (semver.Version, error) {
	endpoint := fmt.Sprintf("http://%s/info", addr)
	var resp struct {
		Version string `json:"version"`
	}
	err := c.client.GETV1(endpoint, &resp)
	if err != nil {
		return semver.Version{}, err
	}
	if resp.Version == "" {
		resp.Version = "unknown"
	}
	return semver.Parse(resp.Version)
}

// GetLookupdTopics returns a []string containing a union of all the topics
// from all the given nsqlookupd
func (c *ClusterInfo) GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []string `json:"topics"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/topics", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			topics = append(topics, resp.Topics...)
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}

	topics = stringy.Uniq(topics)
	sort.Strings(topics)

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetLookupdTopicChannels returns a []string containing a union of all the channels
// from all the given lookupd for the given topic
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	var channels []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Channels []string `json:"channels"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			channels = append(channels, resp.Channels...)
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}

	channels = stringy.Uniq(channels)
	sort.Strings(channels)

	if len(errs) > 0 {
		return channels, ErrList(errs)
	}
	return channels, nil
}

// GetLookupdProducers returns Producers of all the nsqd connected to the given lookupds
func (c *ClusterInfo) GetLookupdProducers(lookupdHTTPAddrs []string) (Producers, error) {
	var producers []*Producer
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	producersByAddr := make(map[string]*Producer)
	maxVersion, _ := semver.Parse("0.0.0")

	type respType struct {
		Producers []*Producer `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/nodes", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, producer := range resp.Producers {
				key := producer.TCPAddress()
				p, ok := producersByAddr[key]
				if !ok {
					producersByAddr[key] = producer
					producers = append(producers, producer)
					if maxVersion.LT(producer.VersionObj) {
						maxVersion = producer.VersionObj
					}
					sort.Sort(producer.Topics)
					p = producer
				}
				p.RemoteAddresses = append(p.RemoteAddresses,
					fmt.Sprintf("%s/%s", addr, producer.Address()))
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}

	for _, producer := range producersByAddr {
		if producer.VersionObj.LT(maxVersion) {
			producer.OutOfDate = true
		}
	}
	sort.Sort(ProducersByHost{producers})

	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetLookupdTopicProducers returns Producers of all the nsqd for a given topic by
// unioning the nodes returned from the given lookupd
func (c *ClusterInfo) GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Producers Producers `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, p := range resp.Producers {
				for _, pp := range producers {
					if p.HTTPAddress() == pp.HTTPAddress() {
						goto skip
					}
				}
				producers = append(producers, p)
			skip:
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqlookupd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDTopics returns a []string containing all the topics produced by the given nsqd
func (c *ClusterInfo) GetNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topics = stringy.Add(topics, topic.Name)
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Strings(topics)

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetNSQDProducers returns Producers of all the given nsqd
func (c *ClusterInfo) GetNSQDProducers(nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
		TopologyZone     string `json:"topology_zone,omitempty"`
		TopologyRegion   string `json:"topology_region,omitempty"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/info", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var infoResp infoRespType
			err := c.client.GETV1(endpoint, &infoResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			endpoint = fmt.Sprintf("http://%s/stats?format=json&include_clients=false", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err = c.client.GETV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			version, err := semver.Parse(infoResp.Version)
			if err != nil {
				version, _ = semver.Parse("0.0.0")
			}

			lock.Lock()
			defer lock.Unlock()
			producers = append(producers, &Producer{
				Version:          infoResp.Version,
				VersionObj:       version,
				BroadcastAddress: infoResp.BroadcastAddress,
				Hostname:         infoResp.Hostname,
				HTTPPort:         infoResp.HTTPPort,
				TCPPort:          infoResp.TCPPort,
				Topics:           producerTopics,
				TopologyZone:     infoResp.TopologyZone,
				TopologyRegion:   infoResp.TopologyRegion,
			})
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDTopicProducers returns Producers containing the addresses of all the nsqd
// that produce the given topic
func (c *ClusterInfo) GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
		TopologyZone     string `json:"topology_zone,omitempty"`
		TopologyRegion   string `json:"topology_region,omitempty"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json&topic=%s&include_clients=false",
				addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err := c.client.GETV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			for _, t := range statsResp.Topics {
				if t.Name == topic {
					endpoint := fmt.Sprintf("http://%s/info", addr)
					c.logf("CI: querying nsqd %s", endpoint)

					var infoResp infoRespType
					err := c.client.GETV1(endpoint, &infoResp)
					if err != nil {
						lock.Lock()
						errs = append(errs, err)
						lock.Unlock()
						return
					}

					version, err := semver.Parse(infoResp.Version)
					if err != nil {
						version, _ = semver.Parse("0.0.0")
					}

					// if BroadcastAddress/HTTPPort are missing, use the values from `addr` for
					// backwards compatibility

					if infoResp.BroadcastAddress == "" {
						var p string
						infoResp.BroadcastAddress, p, _ = net.SplitHostPort(addr)
						infoResp.HTTPPort, _ = strconv.Atoi(p)
					}
					if infoResp.Hostname == "" {
						infoResp.Hostname, _, _ = net.SplitHostPort(addr)
					}

					lock.Lock()
					producers = append(producers, &Producer{
						Version:          infoResp.Version,
						VersionObj:       version,
						BroadcastAddress: infoResp.BroadcastAddress,
						Hostname:         infoResp.Hostname,
						HTTPPort:         infoResp.HTTPPort,
						TCPPort:          infoResp.TCPPort,
						Topics:           producerTopics,
						TopologyZone:     infoResp.TopologyZone,
						TopologyRegion:   infoResp.TopologyRegion,
					})
					lock.Unlock()

					return
				}
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDStats returns aggregate topic and channel stats from the given Producers
//
// if selectedChannel is empty, this will return stats for topic/channel
// if selectedTopic is empty, this will return stats for *all* topic/channels
// if includeClients is false, this will *not* return client stats for channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func (c *ClusterInfo) GetNSQDStats(producers Producers,
	selectedTopic string, selectedChannel string,
	includeClients bool) ([]*TopicStats, map[string]*ChannelStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup
	var topicStatsList TopicStatsList
	var errs []error

	channelStatsMap := make(map[string]*ChannelStats)

	type respType struct {
		Topics []*TopicStats `json:"topics"`
	}

	for _, p := range producers {
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()

			addr := p.HTTPAddress()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			if selectedTopic != "" {
				endpoint += "&topic=" + url.QueryEscape(selectedTopic)
				if selectedChannel != "" {
					endpoint += "&channel=" + url.QueryEscape(selectedChannel)
				}
			}
			if !includeClients {
				endpoint += "&include_clients=false"
			}

			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topic.Node = addr
				topic.Hostname = p.Hostname
				topic.MemoryDepth = topic.Depth - topic.BackendDepth
				topic.DeliveryMsgCount = topic.ZoneLocalMsgCount + topic.RegionLocalMsgCount + topic.GlobalMsgCount
				if selectedTopic != "" && topic.TopicName != selectedTopic {
					continue
				}
				topicStatsList = append(topicStatsList, topic)

				for _, channel := range topic.Channels {
					channel.Node = addr
					channel.Hostname = p.Hostname
					channel.TopicName = topic.TopicName
					channel.MemoryDepth = channel.Depth - channel.BackendDepth
					channel.DeliveryMsgCount = channel.ZoneLocalMsgCount + channel.RegionLocalMsgCount + channel.GlobalMsgCount
					key := channel.ChannelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topic.TopicName, channel.ChannelName)
					}
					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							Node:        addr,
							TopicName:   topic.TopicName,
							ChannelName: channel.ChannelName,
						}
						channelStatsMap[key] = channelStats
					}
					for _, c := range channel.Clients {
						c.Node = addr
						c.NodeTopologyRegion = p.TopologyRegion
						c.NodeTopologyZone = p.TopologyZone
					}
					channelStats.Add(channel)
				}
			}
		}(p)
	}
	wg.Wait()

	if len(errs) == len(producers) {
		return nil, nil, fmt.Errorf("failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Sort(TopicStatsByHost{topicStatsList})

	if len(errs) > 0 {
		return topicStatsList, channelStatsMap, ErrList(errs)
	}
	return topicStatsList, channelStatsMap, nil
}

// TombstoneNodeForTopic tombstones the given node for the given topic on all the given nsqlookupd
// and deletes the topic from the node
func (c *ClusterInfo) TombstoneNodeForTopic(topic string, node string, lookupdHTTPAddrs []string) error {
	var errs []error

	// tombstone the topic on all the lookupds
	qs := fmt.Sprintf("topic=%s&node=%s", url.QueryEscape(topic), url.QueryEscape(node))
	err := c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/tombstone", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	producers, err := c.GetNSQDProducers([]string{node})
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// delete the topic on the producer
	qs = fmt.Sprintf("topic=%s", url.QueryEscape(topic))
	err = c.producersPOST(producers, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) CreateTopicChannel(topicName string, channelName string, lookupdHTTPAddrs []string) error {
	var errs []error

	// create the topic on all the nsqlookupd
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	err := c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/create", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(channelName) > 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))

		// create the channel on all the nsqlookupd
		err := c.nsqlookupdPOST(lookupdHTTPAddrs, "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}

		// create the channel on all the nsqd that produce the topic
		producers, err := c.GetLookupdTopicProducers(topicName, lookupdHTTPAddrs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
		err = c.producersPOST(producers, "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) DeleteTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	var errs []error

	// for topic removal, you need to get all the producers _first_
	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))

	// remove the topic from all the nsqlookupd
	err = c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// remove the topic from all the nsqd that produce this topic
	err = c.producersPOST(producers, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) DeleteChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	var errs []error

	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))

	// remove the channel from all the nsqlookupd
	err = c.nsqlookupdPOST(lookupdHTTPAddrs, "channel/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// remove the channel from all the nsqd that produce this topic
	err = c.producersPOST(producers, "channel/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) PauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/pause", qs)
}

func (c *ClusterInfo) UnPauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/unpause", qs)
}

func (c *ClusterInfo) PauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/pause", qs)
}

func (c *ClusterInfo) UnPauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/unpause", qs)
}

func (c *ClusterInfo) EmptyTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/empty", qs)
}

func (c *ClusterInfo) EmptyChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/empty", qs)
}

func (c *ClusterInfo) actionHelper(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string, uri string, qs string) error {
	var errs []error

	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	err = c.producersPOST(producers, uri, qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) GetProducers(lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		return c.GetLookupdProducers(lookupdHTTPAddrs)
	}
	return c.GetNSQDProducers(nsqdHTTPAddrs)
}

func (c *ClusterInfo) GetTopicProducers(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		return c.GetLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	}
	return c.GetNSQDTopicProducers(topicName, nsqdHTTPAddrs)
}

func (c *ClusterInfo) nsqlookupdPOST(addrs []string, uri string, qs string) error {
	var errs []error
	for _, addr := range addrs {
		endpoint := fmt.Sprintf("http://%s/%s?%s", addr, uri, qs)
		c.logf("CI: querying nsqlookupd %s", endpoint)
		err := c.client.POSTV1(endpoint, nil, nil)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) producersPOST(pl Producers, uri string, qs string) error {
	var errs []error
	for _, p := range pl {
		endpoint := fmt.Sprintf("http://%s/%s?%s", p.HTTPAddress(), uri, qs)
		c.logf("CI: querying nsqd %s", endpoint)
		err := c.client.POSTV1(endpoint, nil, nil)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}
