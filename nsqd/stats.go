package nsqd

import (
	"runtime"
	"sort"
	"sync/atomic"

	"github.com/nsqio/nsq/internal/quantile"
)

type Stats struct {
	Topics    []TopicStats
	Producers []ClientStats
}

type ClientStats interface {
	String() string
}

type TopicStats struct {
	TopicName    string         `json:"topic_name"`
	Channels     []ChannelStats `json:"channels"`
	Depth        int64          `json:"depth"`
	BackendDepth int64          `json:"backend_depth"`
	MessageCount uint64         `json:"message_count"`
	MessageBytes uint64         `json:"message_bytes"`
	Paused       bool           `json:"paused"`

	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

func NewTopicStats(t *Topic, channels []ChannelStats) TopicStats {
	return TopicStats{
		TopicName:    t.name,
		Channels:     channels,
		Depth:        t.Depth(),
		BackendDepth: t.backend.Depth(),
		MessageCount: atomic.LoadUint64(&t.messageCount),
		MessageBytes: atomic.LoadUint64(&t.messageBytes),
		Paused:       t.IsPaused(),

		E2eProcessingLatency: t.AggregateChannelE2eProcessingLatency().Result(),
	}
}

type ChannelStats struct {
	ChannelName         string        `json:"channel_name"`
	Depth               int64         `json:"depth"`
	BackendDepth        int64         `json:"backend_depth"`
	InFlightCount       int           `json:"in_flight_count"`
	DeferredCount       int           `json:"deferred_count"`
	MessageCount        uint64        `json:"message_count"`
	ZoneLocalMsgCount   uint64        `json:"zone_local_msg_count,omitempty"`
	RegionLocalMsgCount uint64        `json:"region_local_msg_count,omitempty"`
	GlobalMsgCount      uint64        `json:"global_msg_count,omitempty"`
	RequeueCount        uint64        `json:"requeue_count"`
	TimeoutCount        uint64        `json:"timeout_count"`
	ClientCount         int           `json:"client_count"`
	Clients             []ClientStats `json:"clients"`
	Paused              bool          `json:"paused"`

	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

func NewChannelStats(c *Channel, clients []ClientStats, clientCount int) ChannelStats {
	c.inFlightMutex.Lock()
	inflight := len(c.inFlightMessages)
	c.inFlightMutex.Unlock()
	c.deferredMutex.Lock()
	deferred := len(c.deferredMessages)
	c.deferredMutex.Unlock()

	return ChannelStats{
		ChannelName:         c.name,
		Depth:               c.Depth(),
		BackendDepth:        c.backend.Depth(),
		InFlightCount:       inflight,
		DeferredCount:       deferred,
		MessageCount:        atomic.LoadUint64(&c.messageCount),
		ZoneLocalMsgCount:   atomic.LoadUint64(&c.zoneLocalMsgCount),
		RegionLocalMsgCount: atomic.LoadUint64(&c.regionLocalMsgCount),
		GlobalMsgCount:      atomic.LoadUint64(&c.globalMsgCount),
		RequeueCount:        atomic.LoadUint64(&c.requeueCount),
		TimeoutCount:        atomic.LoadUint64(&c.timeoutCount),
		ClientCount:         clientCount,
		Clients:             clients,
		Paused:              c.IsPaused(),

		E2eProcessingLatency: c.e2eProcessingLatencyStream.Result(),
	}
}

type Topics []*Topic

func (t Topics) Len() int      { return len(t) }
func (t Topics) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicsByName struct {
	Topics
}

func (t TopicsByName) Less(i, j int) bool { return t.Topics[i].name < t.Topics[j].name }

type Channels []*Channel

func (c Channels) Len() int      { return len(c) }
func (c Channels) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ChannelsByName struct {
	Channels
}

func (c ChannelsByName) Less(i, j int) bool { return c.Channels[i].name < c.Channels[j].name }

func (n *NSQD) GetStats(topic string, channel string, includeClients bool) Stats {
	var stats Stats

	n.RLock()
	var realTopics []*Topic
	if topic == "" {
		realTopics = make([]*Topic, 0, len(n.topicMap))
		for _, t := range n.topicMap {
			realTopics = append(realTopics, t)
		}
	} else if val, exists := n.topicMap[topic]; exists {
		realTopics = []*Topic{val}
	} else {
		n.RUnlock()
		return stats
	}
	n.RUnlock()
	sort.Sort(TopicsByName{realTopics})

	topics := make([]TopicStats, 0, len(realTopics))

	for _, t := range realTopics {
		t.RLock()
		var realChannels []*Channel
		if channel == "" {
			realChannels = make([]*Channel, 0, len(t.channelMap))
			for _, c := range t.channelMap {
				realChannels = append(realChannels, c)
			}
		} else if val, exists := t.channelMap[channel]; exists {
			realChannels = []*Channel{val}
		} else {
			t.RUnlock()
			continue
		}
		t.RUnlock()
		sort.Sort(ChannelsByName{realChannels})
		channels := make([]ChannelStats, 0, len(realChannels))
		for _, c := range realChannels {
			var clients []ClientStats
			var clientCount int
			c.RLock()
			if includeClients {
				clients = make([]ClientStats, 0, len(c.clients))
				for _, client := range c.clients {
					clients = append(clients, client.Stats(topic))
				}
			}
			clientCount = len(c.clients)
			c.RUnlock()
			channels = append(channels, NewChannelStats(c, clients, clientCount))
		}
		topics = append(topics, NewTopicStats(t, channels))
	}
	stats.Topics = topics

	if includeClients {
		var producerStats []ClientStats
		n.tcpServer.conns.Range(func(k, v interface{}) bool {
			c := v.(Client)
			if c.Type() == typeProducer {
				producerStats = append(producerStats, c.Stats(topic))
			}
			return true
		})
		stats.Producers = producerStats
	}

	return stats
}

type memStats struct {
	HeapObjects       uint64 `json:"heap_objects"`
	HeapIdleBytes     uint64 `json:"heap_idle_bytes"`
	HeapInUseBytes    uint64 `json:"heap_in_use_bytes"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	GCPauseUsec100    uint64 `json:"gc_pause_usec_100"`
	GCPauseUsec99     uint64 `json:"gc_pause_usec_99"`
	GCPauseUsec95     uint64 `json:"gc_pause_usec_95"`
	NextGCBytes       uint64 `json:"next_gc_bytes"`
	GCTotalRuns       uint32 `json:"gc_total_runs"`
}

func getMemStats() memStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// sort the GC pause array
	length := len(ms.PauseNs)
	if int(ms.NumGC) < length {
		length = int(ms.NumGC)
	}
	gcPauses := make(Uint64Slice, length)
	copy(gcPauses, ms.PauseNs[:length])
	sort.Sort(gcPauses)

	return memStats{
		ms.HeapObjects,
		ms.HeapIdle,
		ms.HeapInuse,
		ms.HeapReleased,
		percentile(100.0, gcPauses, len(gcPauses)) / 1000,
		percentile(99.0, gcPauses, len(gcPauses)) / 1000,
		percentile(95.0, gcPauses, len(gcPauses)) / 1000,
		ms.NextGC,
		ms.NumGC,
	}

}
