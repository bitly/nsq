package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/nsqd"
)

type tlsRequiredOption int

func (t *tlsRequiredOption) Set(s string) error {
	s = strings.ToLower(s)
	if s == "tcp-https" {
		*t = nsqd.TLSRequiredExceptHTTP
		return nil
	}
	required, err := strconv.ParseBool(s)
	if required {
		*t = nsqd.TLSRequired
	} else {
		*t = nsqd.TLSNotRequired
	}
	return err
}

func (t *tlsRequiredOption) Get() interface{} { return int(*t) }

func (t *tlsRequiredOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

func (t *tlsRequiredOption) IsBoolFlag() bool { return true }

type tlsMinVersionOption uint16

var tlsVersionTable = []struct {
	val uint16
	str string
}{
	{tls.VersionTLS10, "tls1.0"},
	{tls.VersionTLS11, "tls1.1"},
	{tls.VersionTLS12, "tls1.2"},
	{tls.VersionTLS13, "tls1.3"},
}

func (t *tlsMinVersionOption) Set(s string) error {
	s = strings.ToLower(s)
	if s == "" {
		return nil
	}
	for _, v := range tlsVersionTable {
		if s == v.str {
			*t = tlsMinVersionOption(v.val)
			return nil
		}
	}
	return fmt.Errorf("unknown tlsVersionOption %q", s)
}

func (t *tlsMinVersionOption) Get() interface{} { return uint16(*t) }

func (t *tlsMinVersionOption) String() string {
	for _, v := range tlsVersionTable {
		if uint16(*t) == v.val {
			return v.str
		}
	}
	return strconv.FormatInt(int64(*t), 10)
}

type config map[string]interface{}

// Validate settings in the config file, and fatal on errors
func (cfg config) Validate() {
	// special validation/translation
	if v, exists := cfg["tls_required"]; exists {
		var t tlsRequiredOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["tls_required"] = t.String()
		} else {
			logFatal("failed parsing tls_required %+v", v)
		}
	}
	if v, exists := cfg["tls_min_version"]; exists {
		var t tlsMinVersionOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			newVal := fmt.Sprintf("%v", t.Get())
			if newVal != "0" {
				cfg["tls_min_version"] = newVal
			} else {
				delete(cfg, "tls_min_version")
			}
		} else {
			logFatal("failed parsing tls_min_version %+v", v)
		}
	}
	if v, exists := cfg["log_level"]; exists {
		var t lg.LogLevel
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["log_level"] = t
		} else {
			logFatal("failed parsing log_level %+v", v)
		}
	}
}

func nsqdFlagSet(opts *nsqd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqd", flag.ExitOnError)

	// basic options
	flagSet.Bool("version", false, "print version string")
	flagSet.String("config", "", "path to config file")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqd] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.Int64("node-id", opts.ID, "unique part for message IDs, (int) in range [0,1024) (default is hash of hostname)")
	flagSet.Bool("worker-id", false, "[deprecated] use --node-id")

	flagSet.String("https-address", opts.HTTPSAddress, "<addr>:<port> to listen on for HTTPS clients")
	flagSet.String("http-address", opts.HTTPAddress, "address to listen on for HTTP clients (<addr>:<port> for TCP/IP or <path> for unix socket)")
	flagSet.String("tcp-address", opts.TCPAddress, "address to listen on for TCP clients (<addr>:<port> for TCP/IP or <path> for unix socket)")

	authHTTPAddresses := app.StringArray{}
	flagSet.Var(&authHTTPAddresses, "auth-http-address", "<addr>:<port> or a full url to query auth server (may be given multiple times)")
	flagSet.String("auth-http-request-method", opts.AuthHTTPRequestMethod, "HTTP method to use for auth server requests")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address that will be registered with lookupd (defaults to the OS hostname)")
	flagSet.Int("broadcast-tcp-port", opts.BroadcastTCPPort, "TCP port that will be registered with lookupd (defaults to the TCP port that this nsqd is listening on)")
	flagSet.Int("broadcast-http-port", opts.BroadcastHTTPPort, "HTTP port that will be registered with lookupd (defaults to the HTTP port that this nsqd is listening on)")
	lookupdTCPAddrs := app.StringArray{}
	flagSet.Var(&lookupdTCPAddrs, "lookupd-tcp-address", "lookupd TCP address (may be given multiple times)")
	flagSet.Duration("http-client-connect-timeout", opts.HTTPClientConnectTimeout, "timeout for HTTP connect")
	flagSet.Duration("http-client-request-timeout", opts.HTTPClientRequestTimeout, "timeout for HTTP request")
	flagSet.String("topology-region", opts.TopologyRegion, "A region represents a larger domain, made up of one or more zones for preferring closer consumer")
	flagSet.String("topology-zone", opts.TopologyZone, "A zone represents a logical failure domain for preferring closer consumer")

	// diskqueue options
	flagSet.String("data-path", opts.DataPath, "path to store disk-backed messages")
	flagSet.Int64("mem-queue-size", opts.MemQueueSize, "number of messages to keep in memory (per topic/channel)")
	flagSet.Int64("max-bytes-per-file", opts.MaxBytesPerFile, "number of bytes per diskqueue file before rolling")
	flagSet.Int64("sync-every", opts.SyncEvery, "number of messages per diskqueue fsync")
	flagSet.Duration("sync-timeout", opts.SyncTimeout, "duration of time per diskqueue fsync")

	flagSet.Int("queue-scan-worker-pool-max", opts.QueueScanWorkerPoolMax, "max concurrency for checking in-flight and deferred message timeouts")
	flagSet.Int("queue-scan-selection-count", opts.QueueScanSelectionCount, "number of channels to check per cycle (every 100ms) for in-flight and deferred timeouts")

	// msg and command options
	flagSet.Duration("msg-timeout", opts.MsgTimeout, "default duration to wait before auto-requeing a message")
	flagSet.Duration("max-msg-timeout", opts.MaxMsgTimeout, "maximum duration before a message will timeout")
	flagSet.Int64("max-msg-size", opts.MaxMsgSize, "maximum size of a single message in bytes")
	flagSet.Duration("max-req-timeout", opts.MaxReqTimeout, "maximum requeuing timeout for a message")
	flagSet.Int64("max-body-size", opts.MaxBodySize, "maximum size of a single command body")

	// client overridable configuration options
	flagSet.Duration("max-heartbeat-interval", opts.MaxHeartbeatInterval, "maximum client configurable duration of time between client heartbeats")
	flagSet.Int64("max-rdy-count", opts.MaxRdyCount, "maximum RDY count for a client")
	flagSet.Int64("max-output-buffer-size", opts.MaxOutputBufferSize, "maximum client configurable size (in bytes) for a client output buffer")
	flagSet.Duration("max-output-buffer-timeout", opts.MaxOutputBufferTimeout, "maximum client configurable duration of time between flushing to a client")
	flagSet.Duration("min-output-buffer-timeout", opts.MinOutputBufferTimeout, "minimum client configurable duration of time between flushing to a client")
	flagSet.Duration("output-buffer-timeout", opts.OutputBufferTimeout, "default duration of time between flushing data to clients")
	flagSet.Int("max-channel-consumers", opts.MaxChannelConsumers, "maximum channel consumer connection count per nsqd instance (default 0, i.e., unlimited)")

	// statsd integration options
	flagSet.String("statsd-address", opts.StatsdAddress, "UDP <addr>:<port> of a statsd daemon for pushing stats")
	flagSet.Duration("statsd-interval", opts.StatsdInterval, "duration between pushing to statsd")
	flagSet.Bool("statsd-mem-stats", opts.StatsdMemStats, "toggle sending memory and GC stats to statsd")
	flagSet.String("statsd-prefix", opts.StatsdPrefix, "prefix used for keys sent to statsd (%s for host replacement)")
	flagSet.Int("statsd-udp-packet-size", opts.StatsdUDPPacketSize, "the size in bytes of statsd UDP packets")
	flagSet.Bool("statsd-exclude-ephemeral", opts.StatsdExcludeEphemeral, "Skip ephemeral topics and channels when sending stats to statsd")

	// End to end percentile flags
	e2eProcessingLatencyPercentiles := app.FloatArray{}
	flagSet.Var(&e2eProcessingLatencyPercentiles, "e2e-processing-latency-percentile", "message processing time percentiles (as float (0, 1.0]) to track (can be specified multiple times or comma separated '1.0,0.99,0.95', default none)")
	flagSet.Duration("e2e-processing-latency-window-time", opts.E2EProcessingLatencyWindowTime, "calculate end to end latency quantiles for this duration of time (ie: 60s would only show quantile calculations from the past 60 seconds)")

	// TLS config
	flagSet.String("tls-cert", opts.TLSCert, "path to certificate file")
	flagSet.String("tls-key", opts.TLSKey, "path to key file")
	flagSet.String("tls-client-auth-policy", opts.TLSClientAuthPolicy, "client certificate auth policy ('require' or 'require-verify')")
	flagSet.String("tls-root-ca-file", opts.TLSRootCAFile, "path to certificate authority file")
	tlsRequired := tlsRequiredOption(opts.TLSRequired)
	tlsMinVersion := tlsMinVersionOption(opts.TLSMinVersion)
	flagSet.Var(&tlsRequired, "tls-required", "require TLS for client connections (true, false, tcp-https)")
	flagSet.Var(&tlsMinVersion, "tls-min-version", "minimum SSL/TLS version acceptable ('ssl3.0', 'tls1.0', 'tls1.1', 'tls1.2' or 'tls1.3')")

	// compression
	flagSet.Bool("deflate", opts.DeflateEnabled, "enable deflate feature negotiation (client compression)")
	flagSet.Int("max-deflate-level", opts.MaxDeflateLevel, "max deflate compression level a client can negotiate (> values == > nsqd CPU usage)")
	flagSet.Bool("snappy", opts.SnappyEnabled, "enable snappy feature negotiation (client compression)")

	experiments := app.StringArray{}
	var validExperiments []string
	for _, e := range nsqd.AllExperiments {
		validExperiments = append(validExperiments, fmt.Sprintf("%q", string(e)))
	}
	flagSet.Var(&experiments, "enable-experiment", fmt.Sprintf("enable experimental feature (may be given multiple times) (valid options: %s)", strings.Join(validExperiments, ", ")))

	return flagSet
}
