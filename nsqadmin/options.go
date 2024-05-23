package nsqadmin

import (
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger

	HTTPAddress string `flag:"http-address"`
	BasePath    string `flag:"base-path"`

	DevStaticDir string `flag:"dev-static-dir"`

	GraphiteURL   string `flag:"graphite-url"`
	ProxyGraphite bool   `flag:"proxy-graphite"`

	StatsdPrefix        string `flag:"statsd-prefix"`
	StatsdCounterFormat string `flag:"statsd-counter-format"`
	StatsdGaugeFormat   string `flag:"statsd-gauge-format"`

	StatsdInterval time.Duration `flag:"statsd-interval"`

	NSQLookupdHTTPAddresses []string `flag:"lookupd-http-address" cfg:"nsqlookupd_http_addresses"`
	NSQDHTTPAddresses       []string `flag:"nsqd-http-address" cfg:"nsqd_http_addresses"`
	SkipResolveOnStartup    bool     `flag:"skip-resolve-on-startup" cfg:"skip_resolve_on_startup"`

	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout"`

	HTTPClientTLSInsecureSkipVerify bool   `flag:"http-client-tls-insecure-skip-verify"`
	HTTPClientTLSRootCAFile         string `flag:"http-client-tls-root-ca-file"`
	HTTPClientTLSCert               string `flag:"http-client-tls-cert"`
	HTTPClientTLSKey                string `flag:"http-client-tls-key"`

	AllowConfigFromCIDR string `flag:"allow-config-from-cidr"`

	NotificationHTTPEndpoint string `flag:"notification-http-endpoint"`

	ACLHTTPHeader string   `flag:"acl-http-header"`
	AdminUsers    []string `flag:"admin-user" cfg:"admin_users"`
}

func NewOptions() *Options {
	return &Options{
		LogPrefix:                "[nsqadmin] ",
		LogLevel:                 lg.INFO,
		HTTPAddress:              "0.0.0.0:4171",
		BasePath:                 "/",
		StatsdPrefix:             "nsq.%s",
		StatsdCounterFormat:      "stats.counters.%s.count",
		StatsdGaugeFormat:        "stats.gauges.%s",
		StatsdInterval:           60 * time.Second,
		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,
		AllowConfigFromCIDR:      "127.0.0.1/8",
		ACLHTTPHeader:            "X-Forwarded-User",
		AdminUsers:               []string{},
	}
}
