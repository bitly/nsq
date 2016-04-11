package nsqlookupd

import (
	"github.com/absolute8511/nsq/internal/levellogger"
	"log"
	"os"
	"time"
)

type Options struct {
	Verbose bool `flag:"verbose"`

	TCPAddress       string `flag:"tcp-address"`
	HTTPAddress      string `flag:"http-address"`
	RPCPort          string `flag:"rpc-port"`
	BroadcastAddress string `flag:"broadcast-address"`

	EtcdAddress string `flag:"etcd-address"`
	ClusterID   string `flag:"cluster-id"`

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`

	Logger levellogger.Logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,

		EtcdAddress: "",
		ClusterID:   "nsq-clusterid-test-only",

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,

		Logger: &levellogger.GLogger{},
	}
}
