// This is an NSQ client that publishes incoming messages from
// stdin to the specified topic.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

var (
	topic     = flag.String("topic", "", "NSQ topic to publish to")
	delimiter = flag.String("delimiter", "\n", "character to split input from stdin")
	async     = flag.Bool("async", false, "use async mode (default false)")

	destNsqdTCPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

func main() {
	cfg := nsq.NewConfig()
	flag.Var(&nsq.ConfigFlag{cfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	rate := flag.Int64("rate", 0, "Throttle messages to n/second. 0 to disable")

	flag.Parse()

	if len(*topic) == 0 {
		log.Fatal("--topic required")
	}

	if len(*delimiter) != 1 {
		log.Fatal("--delimiter must be a single byte")
	}

	stopChan := make(chan bool)
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	cfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	// make the producers
	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, cfg)
		if err != nil {
			log.Fatalf("failed to create nsq.Producer - %s", err)
		}
		producers[addr] = producer
	}

	if len(producers) == 0 {
		log.Fatal("--nsqd-tcp-address required")
	}

	throttleEnabled := *rate >= 1
	balance := int64(1)
	// avoid divide by 0 if !throttleEnabled
	var interval time.Duration
	if throttleEnabled {
		interval = time.Second / time.Duration(*rate)
	}
	go func() {
		if !throttleEnabled {
			return
		}
		log.Printf("Throttling messages rate to max:%d/second", *rate)
		// every tick increase the number of messages we can send
		for range time.Tick(interval) {
			n := atomic.AddInt64(&balance, 1)
			// if we build up more than 1s of capacity just bound to that
			if n > int64(*rate) {
				atomic.StoreInt64(&balance, int64(*rate))
			}
		}
	}()

	r := bufio.NewReader(os.Stdin)
	delim := (*delimiter)[0]
	go func() {
		for {
			var err error
			if throttleEnabled {
				currentBalance := atomic.LoadInt64(&balance)
				if currentBalance <= 0 {
					time.Sleep(interval)
				}
				err = readAndPublish(r, delim, producers)
				atomic.AddInt64(&balance, -1)
			} else {
				err = readAndPublish(r, delim, producers)
			}
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				close(stopChan)
				break
			}
		}
	}()

	select {
	case <-termChan:
	case <-stopChan:
	}

	for _, producer := range producers {
		producer.Stop()
	}
}

// readAndPublish reads to the delim from r and publishes the bytes
// to the map of producers.
func readAndPublish(r *bufio.Reader, delim byte, producers map[string]*nsq.Producer) error {
	line, readErr := r.ReadBytes(delim)

	if len(line) > 0 {
		// trim the delimiter
		line = line[:len(line)-1]
	}

	if len(line) == 0 {
		return readErr
	}

	for _, producer := range producers {
		var err error
		if *async {
			err = producer.PublishAsync(*topic, line, nil, nil)
		} else {
			err = producer.Publish(*topic, line)
		}
		if err != nil {
			return err
		}
	}

	return readErr
}
