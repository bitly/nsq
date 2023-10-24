package nsqadmin

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

func maybeWarnMsg(msgs []string) string {
	if len(msgs) > 0 {
		return "WARNING: " + strings.Join(msgs, "; ")
	}
	return ""
}

// this is similar to httputil.NewSingleHostReverseProxy except it passes along basic auth
func NewSingleHostReverseProxy(target *url.URL, connectTimeout time.Duration, requestTimeout time.Duration) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if target.User != nil {
			passwd, _ := target.User.Password()
			req.SetBasicAuth(target.User.Username(), passwd)
		}
	}
	return &httputil.ReverseProxy{
		Director:  director,
		Transport: http_api.NewDeadlineTransport(connectTimeout, requestTimeout),
	}
}

type httpServer struct {
	nsqadmin     *NSQAdmin
	router       http.Handler
	client       *http_api.Client
	ci           *clusterinfo.ClusterInfo
	basePath     string
	devStaticDir string
}

func NewHTTPServer(nsqadmin *NSQAdmin) *httpServer {
	log := http_api.Log(nsqadmin.logf)

	client := http_api.NewClient(nsqadmin.httpClientTLSConfig, nsqadmin.getOpts().HTTPClientConnectTimeout,
		nsqadmin.getOpts().HTTPClientRequestTimeout)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(nsqadmin.logf)
	router.NotFound = http_api.LogNotFoundHandler(nsqadmin.logf)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(nsqadmin.logf)

	s := &httpServer{
		nsqadmin: nsqadmin,
		router:   router,
		client:   client,
		ci:       clusterinfo.New(nsqadmin.logf, client),

		basePath:     nsqadmin.getOpts().BasePath,
		devStaticDir: nsqadmin.getOpts().DevStaticDir,
	}

	bp := func(p string) string {
		return path.Join(s.basePath, p)
	}

	router.Handle("GET", bp("/"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/ping"), http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	router.Handle("GET", bp("/topics"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/topics/:topic"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/topics/:topic/:channel"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/nodes"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/nodes/:node"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/counter"), http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", bp("/lookup"), http_api.Decorate(s.indexHandler, log))

	router.Handle("GET", bp("/static/:asset"), http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	router.Handle("GET", bp("/fonts/:asset"), http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	if s.nsqadmin.getOpts().ProxyGraphite {
		proxy := NewSingleHostReverseProxy(nsqadmin.graphiteURL, nsqadmin.getOpts().HTTPClientConnectTimeout,
			nsqadmin.getOpts().HTTPClientRequestTimeout)
		router.Handler("GET", bp("/render"), proxy)
	}

	// v1 endpoints
	router.Handle("GET", bp("/api/topics"), http_api.Decorate(s.topicsHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/topics/:topic"), http_api.Decorate(s.topicHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/topics/:topic/:channel"), http_api.Decorate(s.channelHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/nodes"), http_api.Decorate(s.nodesHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/nodes/:node"), http_api.Decorate(s.nodeHandler, log, http_api.V1))
	router.Handle("POST", bp("/api/topics"), http_api.Decorate(s.createTopicChannelHandler, log, http_api.V1))
	router.Handle("POST", bp("/api/topics/:topic"), http_api.Decorate(s.topicActionHandler, log, http_api.V1))
	router.Handle("POST", bp("/api/topics/:topic/:channel"), http_api.Decorate(s.channelActionHandler, log, http_api.V1))
	router.Handle("DELETE", bp("/api/nodes/:node"), http_api.Decorate(s.tombstoneNodeForTopicHandler, log, http_api.V1))
	router.Handle("DELETE", bp("/api/topics/:topic"), http_api.Decorate(s.deleteTopicHandler, log, http_api.V1))
	router.Handle("DELETE", bp("/api/topics/:topic/:channel"), http_api.Decorate(s.deleteChannelHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/counter"), http_api.Decorate(s.counterHandler, log, http_api.V1))
	router.Handle("GET", bp("/api/graphite"), http_api.Decorate(s.graphiteHandler, log, http_api.V1))
	router.Handle("GET", bp("/config/:opt"), http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", bp("/config/:opt"), http_api.Decorate(s.doConfig, log, http_api.V1))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) indexHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	asset, _ := staticAsset("index.html")
	t, _ := template.New("index").Funcs(template.FuncMap{
		"basePath": func(p string) string {
			return path.Join(s.basePath, p)
		},
	}).Parse(string(asset))

	w.Header().Set("Content-Type", "text/html")
	t.Execute(w, struct {
		Version             string
		ProxyGraphite       bool
		GraphEnabled        bool
		GraphiteURL         string
		StatsdInterval      int
		StatsdCounterFormat string
		StatsdGaugeFormat   string
		StatsdPrefix        string
		NSQLookupd          []string
		IsAdmin             bool
	}{
		Version:             version.Binary,
		ProxyGraphite:       s.nsqadmin.getOpts().ProxyGraphite,
		GraphEnabled:        s.nsqadmin.getOpts().GraphiteURL != "",
		GraphiteURL:         s.nsqadmin.getOpts().GraphiteURL,
		StatsdInterval:      int(s.nsqadmin.getOpts().StatsdInterval / time.Second),
		StatsdCounterFormat: s.nsqadmin.getOpts().StatsdCounterFormat,
		StatsdGaugeFormat:   s.nsqadmin.getOpts().StatsdGaugeFormat,
		StatsdPrefix:        s.nsqadmin.getOpts().StatsdPrefix,
		NSQLookupd:          s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		IsAdmin:             s.isAuthorizedAdminRequest(req),
	})

	return nil, nil
}

func (s *httpServer) staticAssetHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	assetName := ps.ByName("asset")

	var (
		asset []byte
		err   error
	)
	if s.devStaticDir != "" {
		s.nsqadmin.logf(LOG_DEBUG, "using dev dir %q for static asset %q", s.devStaticDir, assetName)
		fsPath := path.Join(s.devStaticDir, assetName)
		asset, err = os.ReadFile(fsPath)
	} else {
		asset, err = staticAsset(assetName)
	}
	if err != nil {
		return nil, http_api.Err{404, "NOT_FOUND"}
	}

	ext := path.Ext(assetName)
	ct := mime.TypeByExtension(ext)
	if ct == "" {
		switch ext {
		case ".map":
			ct = "application/json"
		case ".svg":
			ct = "image/svg+xml"
		case ".woff":
			ct = "application/font-woff"
		case ".ttf":
			ct = "application/font-sfnt"
		case ".eot":
			ct = "application/vnd.ms-fontobject"
		case ".woff2":
			ct = "application/font-woff2"
		}
	}
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}

	return string(asset), nil
}

func (s *httpServer) topicsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	var topics []string
	if len(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses) != 0 {
		topics, err = s.ci.GetLookupdTopics(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	} else {
		topics, err = s.ci.GetNSQDTopics(s.nsqadmin.getOpts().NSQDHTTPAddresses)
	}
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get topics - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	inactive, _ := reqParams.Get("inactive")
	if inactive == "true" {
		topicChannelMap := make(map[string][]string)
		if len(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses) == 0 {
			goto respond
		}
		for _, topicName := range topics {
			producers, _ := s.ci.GetLookupdTopicProducers(
				topicName, s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
			if len(producers) == 0 {
				topicChannels, _ := s.ci.GetLookupdTopicChannels(
					topicName, s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
				topicChannelMap[topicName] = topicChannels
			}
		}
	respond:
		return struct {
			Topics  map[string][]string `json:"topics"`
			Message string              `json:"message"`
		}{topicChannelMap, maybeWarnMsg(messages)}, nil
	}

	return struct {
		Topics  []string `json:"topics"`
		Message string   `json:"message"`
	}{topics, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) topicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")

	producers, err := s.ci.GetTopicProducers(topicName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}
	topicStats, _, err := s.ci.GetNSQDStats(producers, topicName, "", false)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get topic metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	allNodesTopicStats := &clusterinfo.TopicStats{TopicName: topicName}
	for _, t := range topicStats {
		allNodesTopicStats.Add(t)
	}

	return struct {
		*clusterinfo.TopicStats
		Message string `json:"message"`
	}{allNodesTopicStats, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) channelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	producers, err := s.ci.GetTopicProducers(topicName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}
	_, channelStats, err := s.ci.GetNSQDStats(producers, topicName, channelName, true)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get channel metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	sort.Sort(clusterinfo.ClientStatsByNodeTopology{channelStats[channelName].Clients})

	return struct {
		*clusterinfo.ChannelStats
		Message string `json:"message"`
	}{channelStats[channelName], maybeWarnMsg(messages)}, nil
}

func (s *httpServer) nodesHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	producers, err := s.ci.GetProducers(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get nodes - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Nodes   clusterinfo.Producers `json:"nodes"`
		Message string                `json:"message"`
	}{producers, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) nodeHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	node := ps.ByName("node")

	producers, err := s.ci.GetProducers(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	producer := producers.Search(node)
	if producer == nil {
		return nil, http_api.Err{404, "NODE_NOT_FOUND"}
	}

	topicStats, _, err := s.ci.GetNSQDStats(clusterinfo.Producers{producer}, "", "", true)
	if err != nil {
		s.nsqadmin.logf(LOG_ERROR, "failed to get nsqd stats - %s", err)
		return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
	}

	var totalClients int64
	var totalMessages int64
	for _, ts := range topicStats {
		for _, cs := range ts.Channels {
			totalClients += int64(len(cs.Clients))
		}
		totalMessages += ts.MessageCount
	}

	return struct {
		Node          string                    `json:"node"`
		TopicStats    []*clusterinfo.TopicStats `json:"topics"`
		TotalMessages int64                     `json:"total_messages"`
		TotalClients  int64                     `json:"total_clients"`
		Message       string                    `json:"message"`
	}{
		Node:          node,
		TopicStats:    topicStats,
		TotalMessages: totalMessages,
		TotalClients:  totalClients,
		Message:       maybeWarnMsg(messages),
	}, nil
}

func (s *httpServer) tombstoneNodeForTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	node := ps.ByName("node")

	var body struct {
		Topic string `json:"topic"`
	}
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_BODY"}
	}

	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	err = s.ci.TombstoneNodeForTopic(body.Topic, node,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to tombstone node for topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("tombstone_topic_producer", body.Topic, "", node, req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) createTopicChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	var body struct {
		Topic   string `json:"topic"`
		Channel string `json:"channel"`
	}

	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	if len(body.Channel) > 0 && !protocol.IsValidChannelName(body.Channel) {
		return nil, http_api.Err{400, "INVALID_CHANNEL"}
	}

	err = s.ci.CreateTopicChannel(body.Topic, body.Channel,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to create topic/channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("create_topic", body.Topic, "", "", req)
	if len(body.Channel) > 0 {
		s.notifyAdminAction("create_channel", body.Topic, body.Channel, "", req)
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	topicName := ps.ByName("topic")

	err := s.ci.DeleteTopic(topicName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to delete topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("delete_topic", topicName, "", "", req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	err := s.ci.DeleteChannel(topicName, channelName,
		s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to delete channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("delete_channel", topicName, channelName, "", req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) topicActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	return s.topicChannelAction(req, topicName, "")
}

func (s *httpServer) channelActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")
	return s.topicChannelAction(req, topicName, channelName)
}

func (s *httpServer) topicChannelAction(req *http.Request, topicName string, channelName string) (interface{}, error) {
	var messages []string

	var body struct {
		Action string `json:"action"`
	}

	if !s.isAuthorizedAdminRequest(req) {
		return nil, http_api.Err{403, "FORBIDDEN"}
	}

	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	switch body.Action {
	case "pause":
		if channelName != "" {
			err = s.ci.PauseChannel(topicName, channelName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("pause_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.PauseTopic(topicName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("pause_topic", topicName, "", "", req)
		}
	case "unpause":
		if channelName != "" {
			err = s.ci.UnPauseChannel(topicName, channelName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("unpause_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.UnPauseTopic(topicName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("unpause_topic", topicName, "", "", req)
		}
	case "empty":
		if channelName != "" {
			err = s.ci.EmptyChannel(topicName, channelName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("empty_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.EmptyTopic(topicName,
				s.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("empty_topic", topicName, "", "", req)
		}
	default:
		return nil, http_api.Err{400, "INVALID_ACTION"}
	}

	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to %s topic/channel - %s", body.Action, err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

type counterStats struct {
	Node         string `json:"node"`
	TopicName    string `json:"topic_name"`
	ChannelName  string `json:"channel_name"`
	MessageCount int64  `json:"message_count"`
}

func (s *httpServer) counterHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string
	stats := make(map[string]*counterStats)

	producers, err := s.ci.GetProducers(s.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get counter producer list - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}
	_, channelStats, err := s.ci.GetNSQDStats(producers, "", "", false)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.nsqadmin.logf(LOG_ERROR, "failed to get nsqd stats - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.nsqadmin.logf(LOG_WARN, "%s", err)
		messages = append(messages, pe.Error())
	}

	for _, channelStats := range channelStats {
		for _, hostChannelStats := range channelStats.NodeStats {
			key := fmt.Sprintf("%s:%s:%s", channelStats.TopicName, channelStats.ChannelName, hostChannelStats.Node)
			s, ok := stats[key]
			if !ok {
				s = &counterStats{
					Node:        hostChannelStats.Node,
					TopicName:   channelStats.TopicName,
					ChannelName: channelStats.ChannelName,
				}
				stats[key] = s
			}
			s.MessageCount += hostChannelStats.MessageCount
		}
	}

	return struct {
		Stats   map[string]*counterStats `json:"stats"`
		Message string                   `json:"message"`
	}{stats, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) graphiteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	metric, err := reqParams.Get("metric")
	if err != nil || metric != "rate" {
		return nil, http_api.Err{400, "INVALID_ARG_METRIC"}
	}

	target, err := reqParams.Get("target")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TARGET"}
	}

	params := url.Values{}
	params.Set("from", fmt.Sprintf("-%dsec", s.nsqadmin.getOpts().StatsdInterval*2/time.Second))
	params.Set("until", fmt.Sprintf("-%dsec", s.nsqadmin.getOpts().StatsdInterval/time.Second))
	params.Set("format", "json")
	params.Set("target", target)
	query := fmt.Sprintf("/render?%s", params.Encode())
	url := s.nsqadmin.getOpts().GraphiteURL + query

	s.nsqadmin.logf(LOG_INFO, "GRAPHITE: %s", url)

	var response []struct {
		Target     string       `json:"target"`
		DataPoints [][]*float64 `json:"datapoints"`
	}
	err = s.client.GETV1(url, &response)
	if err != nil {
		s.nsqadmin.logf(LOG_ERROR, "graphite request failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	var rateStr string
	rate := *response[0].DataPoints[0][0]
	if rate < 0 {
		rateStr = "N/A"
	} else {
		rateDivisor := s.nsqadmin.getOpts().StatsdInterval / time.Second
		rateStr = fmt.Sprintf("%.2f", rate/float64(rateDivisor))
	}
	return struct {
		Rate string `json:"rate"`
	}{rateStr}, nil
}

func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	opt := ps.ByName("opt")

	allowConfigFromCIDR := s.nsqadmin.getOpts().AllowConfigFromCIDR
	if allowConfigFromCIDR != "" {
		_, ipnet, _ := net.ParseCIDR(allowConfigFromCIDR)
		addr, _, err := net.SplitHostPort(req.RemoteAddr)
		if err != nil {
			s.nsqadmin.logf(LOG_ERROR, "failed to parse RemoteAddr %s", req.RemoteAddr)
			return nil, http_api.Err{400, "INVALID_REMOTE_ADDR"}
		}
		ip := net.ParseIP(addr)
		if ip == nil {
			s.nsqadmin.logf(LOG_ERROR, "failed to parse RemoteAddr %s", req.RemoteAddr)
			return nil, http_api.Err{400, "INVALID_REMOTE_ADDR"}
		}
		if !ipnet.Contains(ip) {
			return nil, http_api.Err{403, "FORBIDDEN"}
		}
	}

	if req.Method == "PUT" {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := int64(1024*1024 + 1)
		body, err := io.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{413, "INVALID_VALUE"}
		}

		opts := *s.nsqadmin.getOpts()
		switch opt {
		case "nsqlookupd_http_addresses":
			err := json.Unmarshal(body, &opts.NSQLookupdHTTPAddresses)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "log_level":
			logLevelStr := string(body)
			logLevel, err := lg.ParseLogLevel(logLevelStr)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			opts.LogLevel = logLevel
		default:
			return nil, http_api.Err{400, "INVALID_OPTION"}
		}
		s.nsqadmin.swapOpts(&opts)
	}

	v, ok := getOptByCfgName(s.nsqadmin.getOpts(), opt)
	if !ok {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}

	return v, nil
}

func (s *httpServer) isAuthorizedAdminRequest(req *http.Request) bool {
	adminUsers := s.nsqadmin.getOpts().AdminUsers
	if len(adminUsers) == 0 {
		return true
	}
	aclHTTPHeader := s.nsqadmin.getOpts().ACLHTTPHeader
	user := req.Header.Get(aclHTTPHeader)
	for _, v := range adminUsers {
		if v == user {
			return true
		}
	}
	return false
}

func getOptByCfgName(opts interface{}, name string) (interface{}, bool) {
	val := reflect.ValueOf(opts).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		flagName := field.Tag.Get("flag")
		cfgName := field.Tag.Get("cfg")
		if flagName == "" {
			continue
		}
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}
		if name != cfgName {
			continue
		}
		return val.FieldByName(field.Name).Interface(), true
	}
	return nil, false
}
