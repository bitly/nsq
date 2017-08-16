package clusterinfo

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/stringy"
	"github.com/blang/semver"
	"math"
	"sync/atomic"
	"errors"
)

var v1EndpointVersion semver.Version

func init() {
	v1EndpointVersion, _ = semver.Parse("0.2.29-alpha")
}

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

type logger interface {
	Output(maxdepth int, s string) error
}

type ClusterInfo struct {
	log    logger
	client *http_api.Client
}

func New(log logger, client *http_api.Client) *ClusterInfo {
	return &ClusterInfo{
		log:    log,
		client: client,
	}
}

func (c *ClusterInfo) logf(f string, args ...interface{}) {
	if c.log == nil {
		return
	}
	c.log.Output(2, fmt.Sprintf(f, args...))
}

// GetVersion returns a semver.Version object by querying /info
func (c *ClusterInfo) GetVersion(addr string) (semver.Version, error) {
	endpoint := fmt.Sprintf("http://%s/info", addr)
	var resp struct {
		Version string `json:"version"`
	}
	err := c.client.NegotiateV1(endpoint, &resp)
	if err != nil {
		return semver.Version{}, err
	}
	if resp.Version == "" {
		resp.Version = "unknown"
	}
	v, err := semver.Parse(resp.Version)
	if err != nil {
		c.logf("CI: parse version failed %s: %v", resp.Version, err)
	}
	return v, err
}

// GetLookupdTopics returns a []*TopicInfo containing a union of all the topics
// from all the given nsqlookupd
func (c *ClusterInfo) GetLookupdTopicsMeta(lookupdHTTPAddrs []string) ([]*TopicInfo, error) {
	var topics []*TopicInfo
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []string `json:"topics"`
		MetaInfo []*TopicInfo `json:"meta_info,omitempty"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/topics?metaInfo=true", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			if resp.MetaInfo != nil {
				topics = append(topics, resp.MetaInfo...)
			} else {
				for _, topic := range resp.Topics {
					topics = append(topics, &TopicInfo{
						TopicName:topic,
					})
				}
			}

		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	sort.Sort(TopicInfoSortByName(topics))

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
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
			err := c.client.NegotiateV1(endpoint, &resp)
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
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
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

			endpoint :=
				fmt.Sprintf("http://%s/channels?topic=%s", addr,
					url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
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
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
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
			err := c.client.NegotiateV1(endpoint, &resp)
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
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
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
func (c *ClusterInfo) GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) (Producers, map[string]Producers, error) {
	var producers Producers
	partitionProducers := make(map[string]Producers)
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Producers          Producers            `json:"producers"`
		PartitionProducers map[string]*Producer `json:"partitions"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			//c.logf("CI: querying nsqlookupd return %v, partitions: %v", resp, resp.PartitionProducers)
			lock.Lock()
			defer lock.Unlock()
			for _, p := range resp.Producers {
				version, err := semver.Parse(p.Version)
				if err != nil {
					c.logf("CI: parse version failed %s: %v", p.Version, err)
					version, _ = semver.Parse("0.0.0")
				}
				p.VersionObj = version

				for _, pp := range producers {
					if p.HTTPAddress() == pp.HTTPAddress() {
						goto skip
					}
				}
				producers = append(producers, p)
			skip:
			}
			for pid, p := range resp.PartitionProducers {
				version, err := semver.Parse(p.Version)
				if err != nil {
					c.logf("CI: parse version failed %s: %v", p.Version, err)
					version, _ = semver.Parse("0.0.0")
				}
				p.VersionObj = version

				partproducers := partitionProducers[pid]
				for _, pp := range partproducers {
					if p.HTTPAddress() == pp.HTTPAddress() {
						goto skip2
					}
				}
				partproducers = append(partproducers, p)
				partitionProducers[pid] = partproducers
			skip2:
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, partitionProducers, ErrList(errs)
	}
	return producers, partitionProducers, nil
}

type TopicInfo struct {
	TopicName	string	`json:"topic_name"`
	ExtSupport 	bool 	`json:"extend_support"`
	Ordered 	bool 	`json:"ordered"`
}

type TopicInfoSortByName []*TopicInfo

func (c TopicInfoSortByName) Less(i, j int) bool {
	return 	c[i].TopicName < c[j].TopicName
}

func (c TopicInfoSortByName) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c TopicInfoSortByName) Len() int {
	return len(c)
}

// GetNSQDTopics returns a []string containing all the topics produced by the given nsqd
func (c *ClusterInfo) GetNSQDTopics(nsqdHTTPAddrs []string) ([]*TopicInfo, error) {
	var topics []*TopicInfo
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []struct {
			Name string `json:"topic_name"`
			Ordered bool `json:"is_multi_ordered"`
			ExtSupport bool `json:"is_ext"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topics = append(topics,  &TopicInfo{
					TopicName: topic.Name,
					Ordered:   topic.Ordered,
					ExtSupport:topic.ExtSupport,
				})
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Sort(TopicInfoSortByName(topics));

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
			err := c.client.NegotiateV1(endpoint, &infoResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			endpoint = fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err = c.client.NegotiateV1(endpoint, &statsResp)
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
				c.logf("CI: parse version failed %s: %v", infoResp.Version, err)
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
			})
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
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

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err := c.client.NegotiateV1(endpoint, &statsResp)
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
					err := c.client.NegotiateV1(endpoint, &infoResp)
					if err != nil {
						lock.Lock()
						errs = append(errs, err)
						lock.Unlock()
						return
					}

					version, err := semver.Parse(infoResp.Version)
					if err != nil {
						c.logf("CI: parse version failed %s: %v", infoResp.Version, err)
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
					})
					lock.Unlock()

					return
				}
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

func (c *ClusterInfo) ListAllLookupdNodes(lookupdHTTPAddrs []string) (*LookupdNodes, error) {
	var errs []error
	var resp LookupdNodes

	for _, addr := range lookupdHTTPAddrs {
		endpoint := fmt.Sprintf("http://%s/listlookup", addr)
		c.logf("CI: querying nsqlookupd %s", endpoint)

		err := c.client.NegotiateV1(endpoint, &resp)
		if err != nil {
			c.logf("CI: querying nsqlookupd %s err: %v", endpoint, err)
			errs = append(errs, err)
			continue
		}
		break
	}

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	return &resp, nil
}

func (c *ClusterInfo) GetNSQDAllMessageHistoryStats(producers Producers) (map[string]int64, error) {
	var errs []error

	nodeHistoryStatsMap := make(map[string]int64)

	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, p := range producers {
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()
			addr := p.HTTPAddress()
			endpoint := fmt.Sprintf("http://%s/message/historystats", addr)
			var nodeHistoryStatsResp struct {
				HistoryStats []*NodeHourlyPubsize `json:"node_hourly_pub_size_stats"`
			}
			err := c.client.NegotiateV1(endpoint, &nodeHistoryStatsResp)
			//c.logf("CI: querying nsqd %s resp: %v", endpoint, nodeHistoryStatsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
			}
			for _, topicMsgStat := range nodeHistoryStatsResp.HistoryStats {
				lock.Lock()
				_, ok := nodeHistoryStatsMap[topicMsgStat.TopicName]
				if !ok {
					nodeHistoryStatsMap[topicMsgStat.TopicName] = topicMsgStat.HourlyPubSize
				} else {
					nodeHistoryStatsMap[topicMsgStat.TopicName] += topicMsgStat.HourlyPubSize
				}
				lock.Unlock()
			}
		}(p)
	}
	wg.Wait()
	if len(errs) == len(producers) {
		return nil, fmt.Errorf("Failed to query any nsqd for node topic message history: %s", ErrList(errs))
	}

	return nodeHistoryStatsMap, nil
}

func (c *ClusterInfo) GetNSQDMessageHistoryStats(nsqdHTTPAddr string, selectedTopic string, par string) ([]int64, error) {
	//aggregate partition dist data from producers
	endpoint := fmt.Sprintf("http://%s/message/historystats?topic=%s&partition=%s", nsqdHTTPAddr, selectedTopic, par)
	var historyStatsResp struct {
		HistoryStat []int64 `json:"hourly_pub_size"`
	}
	err := c.client.NegotiateV1(endpoint, &historyStatsResp)
	if err != nil {
		return nil, err
	}

	c.logf("CI: querying nsqd %s resp: %v", endpoint, historyStatsResp)

	return historyStatsResp.HistoryStat, nil
}

func (c *ClusterInfo) GetNSQDMessageByID(p Producer, selectedTopic string,
	part string, msgID int64) (string, int64, error) {
	if selectedTopic == "" {
		return "", 0, fmt.Errorf("missing topic while get message")
	}
	type msgInfo struct {
		ID        int64  `json:"id"`
		TraceID   uint64 `json:"trace_id"`
		Body      string `json:"body"`
		Timestamp int64  `json:"timestamp"`
		Attempts  uint16 `json:"attempts"`

		Offset        int64 `json:"offset"`
		QueueCntIndex int64 `json:"queue_cnt_index"`
	}

	addr := p.HTTPAddress()
	endpoint := fmt.Sprintf("http://%s/message/get?topic=%s&partition=%s&search_mode=id&search_pos=%d", addr,
		url.QueryEscape(selectedTopic), url.QueryEscape(part), msgID)
	c.logf("CI: querying nsqd %s", endpoint)

	var resp msgInfo
	_, err := c.client.GETV1(endpoint, &resp)
	if err != nil {
		return "", 0, err
	}
	return resp.Body, resp.Offset, nil
}

func (c *ClusterInfo) GetNSQDCoordStats(producers Producers, selectedTopic string, part string) (*CoordStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup
	var topicCoordStats CoordStats
	var errs []error

	for _, p := range producers {
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()

			addr := p.HTTPAddress()
			endpoint := fmt.Sprintf("http://%s/coordinator/stats?format=json", addr)
			if selectedTopic != "" {
				endpoint = fmt.Sprintf("http://%s/coordinator/stats?format=json&topic=%s", addr, url.QueryEscape(selectedTopic))
			}
			if part != "" {
				endpoint = fmt.Sprintf("http://%s/coordinator/stats?format=json&topic=%s&partition=%s",
					addr, url.QueryEscape(selectedTopic), url.QueryEscape(part))
			}
			c.logf("CI: querying nsqd %s", endpoint)

			var resp CoordStats
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			c.logf("CI: querying nsqd %s resp: %v", endpoint, resp)
			topicCoordStats.RpcStats = resp.RpcStats.Snapshot()
			for _, topicStat := range resp.TopicCoordStats {
				topicStat.Node = addr
				if selectedTopic != "" && topicStat.Name != selectedTopic {
					continue
				}
				topicCoordStats.TopicCoordStats = append(topicCoordStats.TopicCoordStats, topicStat)
			}
		}(p)
	}
	wg.Wait()

	if len(errs) == len(producers) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	if len(errs) > 0 {
		return &topicCoordStats, ErrList(errs)
	}
	return &topicCoordStats, nil
}

var INDEX = int32(0)

func (c *ClusterInfo) GetClusterInfo(lookupdAdresses []string) (*ClusterNodeInfo, error) {
	INDEX = atomic.AddInt32(&INDEX, 1) & math.MaxInt32
	c.logf("INDEX for picking lookup http address: %d", INDEX)
	lookupdAdress := lookupdAdresses[int(INDEX)%(len(lookupdAdresses))]
	c.logf("lookupd http address %s picked.", lookupdAdress)
	endpoint := fmt.Sprintf("http://%s/cluster/stats", lookupdAdress)

	var resp ClusterNodeInfo
	err := c.client.NegotiateV1(endpoint, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetNSQDStats returns aggregate topic and channel stats from the given Producers
//
// if selectedTopic is empty, this will return stats for *all* topic/channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func (c *ClusterInfo) GetNSQDStats(producers Producers, selectedTopic string, sortBy string, leaderOnly bool) ([]*TopicStats, map[string]*ChannelStats, error) {
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
			endpoint := fmt.Sprintf("http://%s/stats?format=json&leaderOnly=%t", addr, leaderOnly)
			if selectedTopic != "" {
				endpoint = fmt.Sprintf("http://%s/stats?format=json&topic=%s&leaderOnly=%t", addr, selectedTopic, leaderOnly)
			}
			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
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
				if selectedTopic != "" && topic.TopicName != selectedTopic {
					continue
				}
				if topic.StatsdName == "" {
					topic.StatsdName = topic.TopicName
				}
				topicStatsList = append(topicStatsList, topic)

				for _, channel := range topic.Channels {
					channel.Node = addr
					channel.Hostname = p.Hostname
					channel.TopicName = topic.TopicName
					channel.TopicPartition = topic.TopicPartition
					channel.StatsdName = topic.StatsdName
					channel.IsMultiOrdered = topic.IsMultiOrdered
					channel.IsExt = topic.IsExt
					channel.MemoryDepth = channel.Depth - channel.BackendDepth
					key := channel.ChannelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topic.TopicName, channel.ChannelName)
					}
					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							Node:           addr,
							TopicName:      topic.TopicName,
							TopicPartition: topic.TopicPartition,
							StatsdName:     topic.StatsdName,
							ChannelName:    channel.ChannelName,
							IsMultiOrdered: topic.IsMultiOrdered,
							RequeueCount:	channel.RequeueCount,
							TimeoutCount:	channel.TimeoutCount,
						}
						channelStatsMap[key] = channelStats
					}
					for _, c := range channel.Clients {
						c.Node = addr
					}
					channelStats.Add(channel)
					topic.TotalChannelDepth += channel.Depth
				}
			}
		}(p)
	}
	wg.Wait()

	if len(errs) == len(producers) {
		return nil, nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	if sortBy == "partition" {
		sort.Sort(TopicStatsByPartitionAndHost{topicStatsList})
	} else if sortBy == "channel-depth" {
		sort.Sort(TopicStatsByChannelDepth{topicStatsList})
	} else if sortBy == "message-count" {
		sort.Sort(TopicStatsByMessageCount{topicStatsList})
	} else {
		sort.Sort(TopicStatsByPartitionAndHost{topicStatsList})
	}

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
	lookupdNodes, _ := c.ListAllLookupdNodes(lookupdHTTPAddrs)
	for _, node := range lookupdNodes.AllNodes {
		lookupdHTTPAddrs = append(lookupdHTTPAddrs, net.JoinHostPort(node.NodeIP, node.HttpPort))
	}
	err := c.versionPivotNSQLookupd(lookupdHTTPAddrs, "tombstone_topic_producer", "topic/tombstone", qs)
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

func (c *ClusterInfo) CreateTopicChannelAfterTopicCreation(topicName string, channelName string, lookupdHTTPAddrs []string, partitionNum int) error {
	var errs []error

	//fetch nsqd from leader only
	lookupdNodes, err := c.ListAllLookupdNodes(lookupdHTTPAddrs)
	if err != nil {
		c.logf("failed to list lookupd nodes while create topic: %v", err)
		return err
	}
	leaderAddr := make([]string, 0)
	leaderAddr = append(leaderAddr, net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort))

	producers, partitionProducers, err := c.GetTopicProducers(topicName, leaderAddr, nil)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(producers) == 0 && len(partitionProducers) == 0 {
		c.logf(fmt.Sprintf("Producer:%d, PartitionProducers:%d", len(producers), len(partitionProducers)));
		text := fmt.Sprintf("no producer or partition producer found for Topic:%s, Channel:%s", topicName, channelName)
		return errors.New(text)
	}
	if len(producers) > 0 && len(partitionProducers) == 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
		err = c.versionPivotProducers(producers, "create_channel", "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		if len(partitionProducers) < partitionNum {
			text := fmt.Sprintf("Partition number: %v returned from leader lookup is less than expected partition number: %v", len(partitionProducers), partitionNum)
			return errors.New(text)
		}
		for pid, pp := range partitionProducers {
			qs := fmt.Sprintf("topic=%s&channel=%s&partition=%s", url.QueryEscape(topicName), url.QueryEscape(channelName), pid)
			err = c.versionPivotProducers(pp, "create_channel", "channel/create", qs)
			if err != nil {
				pe, ok := err.(PartialErr)
				if !ok {
					return err
				}
				errs = append(errs, pe.Errors()...)
			}
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) CreateTopicChannel(topicName string, channelName string, lookupdHTTPAddrs []string) error {
	var errs []error

	producers, partitionProducers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nil)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(partitionProducers) == 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
		err = c.versionPivotProducers(producers, "create_channel", "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		for pid, pp := range partitionProducers {
			qs := fmt.Sprintf("topic=%s&channel=%s&partition=%s", url.QueryEscape(topicName), url.QueryEscape(channelName), pid)
			err = c.versionPivotProducers(pp, "create_channel", "channel/create", qs)
			if err != nil {
				pe, ok := err.(PartialErr)
				if !ok {
					return err
				}
				errs = append(errs, pe.Errors()...)
			}
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) CreateTopic(topicName string, partitionNum int, replica int, syncDisk int,
	retentionDays string, orderedmulti string, ext string, lookupdHTTPAddrs []string) error {
	var errs []error

	// TODO: found the master lookup node first
	// create the topic on all the nsqlookupd
	qs := fmt.Sprintf("topic=%s&partition_num=%d&replicator=%d&syncdisk=%d&retention=%s&orderedmulti=%s&extend=%s",
		url.QueryEscape(topicName), partitionNum, replica, syncDisk, retentionDays, orderedmulti, ext)
	lookupdNodes, err := c.ListAllLookupdNodes(lookupdHTTPAddrs)
	if err != nil {
		c.logf("failed to list lookupd nodes while create topic: %v", err)
		return err
	}
	leaderAddr := make([]string, 0)
	leaderAddr = append(leaderAddr, net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort))
	err = c.versionPivotNSQLookupd(leaderAddr, "create_topic", "topic/create", qs)
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

// this will delete all partitions of topic on all nsqd node.
func (c *ClusterInfo) DeleteTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	var errs []error

	lookupdNodes, err := c.ListAllLookupdNodes(lookupdHTTPAddrs)
	if err != nil {
		c.logf("failed to list lookupd nodes while delete topic: %v", err)
		return err
	}
	leaderAddr := make([]string, 0)
	leaderAddr = append(leaderAddr, net.JoinHostPort(lookupdNodes.LeaderNode.NodeIP, lookupdNodes.LeaderNode.HttpPort))

	qs := fmt.Sprintf("topic=%s&partition=**", url.QueryEscape(topicName))
	// remove the topic from all the nsqlookupd
	err = c.versionPivotNSQLookupd(leaderAddr, "delete_topic", "topic/delete", qs)
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
	retry := true
	producers, partitionProducers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

channelDelete:
	if len(partitionProducers) == 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
		// remove the channel from all the nsqd that produce this topic
		err = c.versionPivotProducers(producers, "delete_channel", "channel/delete", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		for pid, pp := range partitionProducers {
			qs := fmt.Sprintf("topic=%s&channel=%s&partition=%s", url.QueryEscape(topicName), url.QueryEscape(channelName), pid)
			// remove the channel from all the nsqd that produce this topic
			err = c.versionPivotProducers(pp, "delete_channel", "channel/delete", qs)
			if err != nil {
				pe, ok := err.(PartialErr)
				if !ok {
					return err
				}
				errs = append(errs, pe.Errors()...)
			}
		}
	}

	_, allChannelStats, err := c.GetNSQDStats(producers, topicName, "partition", true)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if _, exist := allChannelStats[channelName]; exist {
		c.logf("channel %v are not completely deleted", channelName)
		if retry {
			//do delete again
			retry = false;
			goto channelDelete
		} else {
			c.logf("fail to delete channel %v completely", channelName)
		}
	} else {
		c.logf("channel %v deleted", channelName)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) PauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "pause_topic", "topic/pause", qs)
}

func (c *ClusterInfo) UnPauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "unpause_topic", "topic/unpause", qs)
}

func (c *ClusterInfo) PauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "pause_channel", "channel/pause", qs)
}

func (c *ClusterInfo) UnPauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "unpause_channel", "channel/unpause", qs)
}

func (c *ClusterInfo) SkipChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "skip_channel", "channel/skip", qs)
}

func (c *ClusterInfo) UnSkipChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "unskip_channel", "channel/unskip", qs)
}

func (c *ClusterInfo) EmptyTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "empty_topic", "topic/empty", qs)
}

func (c *ClusterInfo) EmptyChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "empty_channel", "channel/empty", qs)
}

func (c *ClusterInfo) actionHelper(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string, deprecatedURI string, v1URI string, qs string) error {
	var errs []error

	producers, partitionProducers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}
	c.logf("CI: got %v producer nodes %v partition producers for topic %v", len(producers), len(partitionProducers), topicName)
	if len(partitionProducers) == 0 {
		err = c.versionPivotProducers(producers, deprecatedURI, v1URI, qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	} else {
		for pid, pp := range partitionProducers {
			qsPart := qs + "&partition=" + pid
			err = c.versionPivotProducers(pp, deprecatedURI, v1URI, qsPart)
			if err != nil {
				pe, ok := err.(PartialErr)
				if !ok {
					return err
				}
				errs = append(errs, pe.Errors()...)
			}
		}
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

func (c *ClusterInfo) GetTopicProducers(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, map[string]Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		p, pp, err := c.GetLookupdTopicProducers(topicName, lookupdHTTPAddrs)
		return p, pp, err
	}
	p, err := c.GetNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	return p, nil, err
}

func (c *ClusterInfo) versionPivotNSQLookupd(addrs []string, deprecatedURI string, v1URI string, qs string) error {
	var errs []error

	for _, addr := range addrs {
		nodeVer, _ := c.GetVersion(addr)

		uri := deprecatedURI
		if nodeVer.NE(semver.Version{}) && nodeVer.GTE(v1EndpointVersion) {
			uri = v1URI
		}

		endpoint := fmt.Sprintf("http://%s/%s?%s", addr, uri, qs)
		c.logf("CI: querying nsqlookupd %s", endpoint)
		_, err := c.client.POSTV1(endpoint)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) versionPivotProducers(pl Producers, deprecatedURI string, v1URI string, qs string) error {
	var errs []error

	for _, p := range pl {
		uri := deprecatedURI
		if p.VersionObj.NE(semver.Version{}) && p.VersionObj.GTE(v1EndpointVersion) {
			uri = v1URI
		}

		endpoint := fmt.Sprintf("http://%s/%s?%s", p.HTTPAddress(), uri, qs)
		c.logf("CI: querying nsqd %s", endpoint)
		_, err := c.client.POSTV1(endpoint)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}
