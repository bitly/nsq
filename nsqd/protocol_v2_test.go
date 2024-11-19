package nsqd

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/test"
)

func mustStartNSQD(opts *Options) (net.Addr, net.Addr, *NSQD) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := os.MkdirTemp("", "nsq-test-")
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}
	nsqd, err := New(opts)
	if err != nil {
		panic(err)
	}
	go func() {
		err := nsqd.Main()
		if err != nil {
			panic(err)
		}
	}()
	return nsqd.RealTCPAddr(), nsqd.RealHTTPAddr(), nsqd
}

func mustConnectNSQD(tcpAddr net.Addr) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}

func identify(t *testing.T, conn io.ReadWriter, extra map[string]interface{}, f int32) []byte {
	ci := make(map[string]interface{})
	ci["client_id"] = "test"
	ci["feature_negotiation"] = true
	for k, v := range extra {
		ci[k] = v
	}
	cmd, _ := nsq.Identify(ci)
	_, err := cmd.WriteTo(conn)
	test.Nil(t, err)
	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Nil(t, err)
	test.Equal(t, frameType, f)
	return data
}

func sub(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Nil(t, err)
	readValidate(t, conn, frameTypeResponse, "OK")
}

func authCmd(t *testing.T, conn io.ReadWriter, authSecret string, expectSuccess string) {
	auth, _ := nsq.Auth(authSecret)
	_, err := auth.WriteTo(conn)
	test.Nil(t, err)
	if expectSuccess != "" {
		readValidate(t, conn, nsq.FrameTypeResponse, expectSuccess)
	}
}

func subFail(t *testing.T, conn io.ReadWriter, topicName string, channelName string) {
	_, err := nsq.Subscribe(topicName, channelName).WriteTo(conn)
	test.Nil(t, err)
	resp, _ := nsq.ReadResponse(conn)
	frameType, _, _ := nsq.UnpackResponse(resp)
	test.Equal(t, frameTypeError, frameType)
}

func readValidate(t *testing.T, conn io.Reader, f int32, d string) []byte {
	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, err := nsq.UnpackResponse(resp)
	test.Nil(t, err)
	test.Equal(t, f, frameType)
	test.Equal(t, d, string(data))
	return data
}

// test channel/topic names
func TestChannelTopicNames(t *testing.T) {
	test.Equal(t, true, protocol.IsValidChannelName("test"))
	test.Equal(t, true, protocol.IsValidChannelName("test-with_period."))
	test.Equal(t, true, protocol.IsValidChannelName("test#ephemeral"))
	test.Equal(t, true, protocol.IsValidTopicName("test"))
	test.Equal(t, true, protocol.IsValidTopicName("test-with_period."))
	test.Equal(t, true, protocol.IsValidTopicName("test#ephemeral"))
	test.Equal(t, false, protocol.IsValidTopicName("test:ephemeral"))
}

// exercise the basic operations of the V2 protocol
func TestBasicV2(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Nil(t, err)

	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, _ := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	test.Equal(t, frameTypeMessage, frameType)
	test.Equal(t, msg.ID, msgOut.ID)
	test.Equal(t, msg.Body, msgOut.Body)
	test.Equal(t, uint16(1), msgOut.Attempts)
}

func TestMultipleConsumerV2(t *testing.T) {
	msgChan := make(chan *Message)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn, err := mustConnectNSQD(tcpAddr)
		test.Nil(t, err)
		defer conn.Close()

		identify(t, conn, nil, frameTypeResponse)
		sub(t, conn, topicName, "ch"+i)

		_, err = nsq.Ready(1).WriteTo(conn)
		test.Nil(t, err)

		go func(c net.Conn) {
			resp, err := nsq.ReadResponse(c)
			test.Nil(t, err)
			_, data, err := nsq.UnpackResponse(resp)
			test.Nil(t, err)
			msg, err := decodeMessage(data)
			test.Nil(t, err)
			msgChan <- msg
		}(conn)
	}

	msgOut := <-msgChan
	test.Equal(t, msg.ID, msgOut.ID)
	test.Equal(t, msg.Body, msgOut.Body)
	test.Equal(t, uint16(1), msgOut.Attempts)
	msgOut = <-msgChan
	test.Equal(t, msg.ID, msgOut.ID)
	test.Equal(t, msg.Body, msgOut.Body)
	test.Equal(t, uint16(1), msgOut.Attempts)
}

// TestSameZoneConsumerV2 tests that a published message goes to same-zone consumer first
// if it's message pump is waiting
func TestSameZoneConsumerV2(t *testing.T) {
	opts := NewOptions()
	opts.Experiments = []string{string(TopologyAwareConsumption)}
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	opts.TopologyRegion = "region"
	opts.TopologyZone = "zone"
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_zone_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	topic.GetChannel("ch")

	var sameZone, diffZone int64
	var exiting int32
	done := make(chan bool, 21)
	for _, zone := range []string{"zone", "zone", "zone2", "zone2"} {
		zone := zone
		conn, err := mustConnectNSQD(tcpAddr)
		test.Nil(t, err)
		defer conn.Close()

		identify(t, conn, map[string]interface{}{"topology_zone": zone}, frameTypeResponse)
		sub(t, conn, topicName, "ch")

		_, err = nsq.Ready(10).WriteTo(conn)
		test.Nil(t, err)

		go func(c net.Conn, zone string) {
			for {
				resp, err := nsq.ReadResponse(c)
				if atomic.LoadInt32(&exiting) == 1 {
					return
				}
				test.Nil(t, err)
				_, data, err := nsq.UnpackResponse(resp)
				test.Nil(t, err)
				_, err = decodeMessage(data)
				test.Nil(t, err)
				if zone == "zone" {
					atomic.AddInt64(&sameZone, 1)
				} else {
					atomic.AddInt64(&diffZone, 1)
				}
				done <- true
			}
		}(conn, zone)
	}

	// first 20 messages go to same zone (each has RDY 10)
	// next message goes to global memoryChan (All consumers)
	for i := 0; i < 21; i++ {
		topic.PutMessage(NewMessage(topic.GenerateID(), make([]byte, 100)))
		if i%2 == 0 {
			// sleep long enough for messagePump to wait again
			time.Sleep(time.Millisecond)
		}
	}
	var doneCount int64
	for range done {
		doneCount += 1
		if doneCount == 21 {
			break
		}
	}
	t.Logf("got same zone %d diffZone %d", sameZone, diffZone)
	atomic.StoreInt32(&exiting, 1)
	test.Equal(t, int64(20), sameZone)
	test.Equal(t, int64(1), diffZone)
}

func TestClientTimeout(t *testing.T) {
	topicName := "test_client_timeout_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 150 * time.Millisecond
	opts.LogLevel = LOG_DEBUG
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	time.Sleep(150 * time.Millisecond)

	// depending on timing there may be 1 or 2 hearbeats sent
	// just read until we get an error
	timer := time.After(100 * time.Millisecond)
	for {
		select {
		case <-timer:
			t.Fatalf("test timed out")
		default:
			_, err := nsq.ReadResponse(conn)
			if err != nil {
				goto done
			}
		}
	}
done:
}

func TestClientHeartbeat(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	test.Equal(t, []byte("_heartbeat_"), data)

	time.Sleep(20 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Nil(t, err)

	// wait long enough that would have timed out (had we not sent the above cmd)
	time.Sleep(100 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Nil(t, err)
}

func TestClientHeartbeatDisableSUB(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	opts.LogLevel = LOG_DEBUG
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)
	subFail(t, conn, topicName, "ch")
}

func TestClientHeartbeatDisable(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 100 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)

	time.Sleep(150 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Nil(t, err)
}

func TestMaxHeartbeatIntervalValid(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval / time.Millisecond)
	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeResponse)
}

func TestMaxHeartbeatIntervalInvalid(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval/time.Millisecond + 1)
	data := identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeError)
	test.Equal(t, "E_BAD_BODY IDENTIFY heartbeat interval (300001) is invalid", string(data))
}

func TestPausing(t *testing.T) {
	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Nil(t, err)

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	channel := topic.GetChannel("ch")
	topic.PutMessage(msg)

	// receive the first message via the client, finish it, and send new RDY
	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msg, _ = decodeMessage(data)
	test.Equal(t, []byte("test body"), msg.Body)

	_, err = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn)
	test.Nil(t, err)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Nil(t, err)

	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	// pause the channel... the client shouldn't receive any more messages
	channel.Pause()

	// sleep to allow the paused state to take effect
	time.Sleep(50 * time.Millisecond)

	msg = NewMessage(topic.GenerateID(), []byte("test body2"))
	topic.PutMessage(msg)

	// allow the client to possibly get a message, the test would hang indefinitely
	// if pausing was not working
	time.Sleep(50 * time.Millisecond)
	msg = <-channel.memoryMsgChan
	test.Equal(t, []byte("test body2"), msg.Body)

	// unpause the channel... the client should now be pushed a message
	channel.UnPause()

	msg = NewMessage(topic.GenerateID(), []byte("test body3"))
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(conn)
	_, data, _ = nsq.UnpackResponse(resp)
	msg, _ = decodeMessage(data)
	test.Equal(t, []byte("test body3"), msg.Body)
}

func TestEmptyCommand(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("\n\n"))
	test.Nil(t, err)

	// if we didn't panic here we're good, see issue #120
}

func TestSizeLimits(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	topicName := "test_limits_v2" + strconv.Itoa(int(time.Now().Unix()))

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	// PUB that's valid
	nsq.Publish(topicName, make([]byte, 95)).WriteTo(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)

	// PUB that's invalid (too big)
	nsq.Publish(topicName, make([]byte, 105)).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_BAD_MESSAGE PUB message too big 105 > 100", string(data))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	// PUB thats empty
	nsq.Publish(topicName, []byte{}).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_BAD_MESSAGE PUB invalid message body size 0", string(data))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	// MPUB body that's valid
	mpub := make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)

	// MPUB body that's invalid (body too big)
	mpub = make([][]byte, 11)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_BAD_BODY MPUB body too big 1148 > 1000", string(data))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	// MPUB that's invalid (one message empty)
	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	mpub = append(mpub, []byte{})
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_BAD_MESSAGE MPUB invalid message(5) body size 0", string(data))

	// need to reconnect
	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	// MPUB body that's invalid (one of the messages is too big)
	mpub = make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 101)
	}
	cmd, _ = nsq.MultiPublish(topicName, mpub)
	cmd.WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_BAD_MESSAGE MPUB message too big 101 > 100", string(data))
}

func TestDPUB(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	topicName := "test_dpub_v2" + strconv.Itoa(int(time.Now().Unix()))

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	// valid
	nsq.DeferredPublish(topicName, time.Second, make([]byte, 100)).WriteTo(conn)
	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)

	time.Sleep(25 * time.Millisecond)

	ch := nsqd.GetTopic(topicName).GetChannel("ch")
	ch.deferredMutex.Lock()
	numDef := len(ch.deferredMessages)
	ch.deferredMutex.Unlock()
	test.Equal(t, 1, numDef)
	test.Equal(t, 1, int(atomic.LoadUint64(&ch.messageCount)))

	// duration out of range
	nsq.DeferredPublish(topicName, opts.MaxReqTimeout+100*time.Millisecond, make([]byte, 100)).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_INVALID DPUB timeout 3600100 out of range 0-3600000", string(data))
}

func TestTouch(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MsgTimeout = 150 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_touch" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.PutMessage(msg)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Nil(t, err)

	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, _ := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	test.Equal(t, frameTypeMessage, frameType)
	test.Equal(t, msg.ID, msgOut.ID)

	time.Sleep(75 * time.Millisecond)

	_, err = nsq.Touch(nsq.MessageID(msg.ID)).WriteTo(conn)
	test.Nil(t, err)

	time.Sleep(75 * time.Millisecond)

	_, err = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn)
	test.Nil(t, err)

	test.Equal(t, uint64(0), channel.timeoutCount)
}

func TestMaxRdyCount(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxRdyCount = 50
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_max_rdy_count" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.PutMessage(msg)

	data := identify(t, conn, nil, frameTypeResponse)
	r := struct {
		MaxRdyCount int64 `json:"max_rdy_count"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, int64(50), r.MaxRdyCount)
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(int(opts.MaxRdyCount)).WriteTo(conn)
	test.Nil(t, err)

	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, _ := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	test.Equal(t, frameTypeMessage, frameType)
	test.Equal(t, msg.ID, msgOut.ID)

	_, err = nsq.Ready(int(opts.MaxRdyCount) + 1).WriteTo(conn)
	test.Nil(t, err)

	resp, err = nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, _ = nsq.UnpackResponse(resp)
	test.Equal(t, int32(1), frameType)
	test.Equal(t, "E_INVALID RDY count 51 out of range 0-50", string(data))
}

func TestFatalError(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("ASDF\n"))
	test.Nil(t, err)

	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, _ := nsq.UnpackResponse(resp)
	test.Equal(t, int32(1), frameType)
	test.Equal(t, "E_INVALID invalid command ASDF", string(data))

	_, err = nsq.ReadResponse(conn)
	test.NotNil(t, err)
}

func TestOutputBuffering(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_output_buffering" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	outputBufferSize := 256 * 1024
	outputBufferTimeout := 500

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), make([]byte, outputBufferSize-1024))
	topic.PutMessage(msg)

	start := time.Now()
	data := identify(t, conn, map[string]interface{}{
		"output_buffer_size":    outputBufferSize,
		"output_buffer_timeout": outputBufferTimeout,
	}, frameTypeResponse)
	var decoded map[string]interface{}
	json.Unmarshal(data, &decoded)
	v, ok := decoded["output_buffer_size"]
	test.Equal(t, true, ok)
	test.Equal(t, outputBufferSize, int(v.(float64)))
	v = decoded["output_buffer_timeout"]
	test.Equal(t, outputBufferTimeout, int(v.(float64)))
	sub(t, conn, topicName, "ch")

	_, err = nsq.Ready(10).WriteTo(conn)
	test.Nil(t, err)

	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	end := time.Now()

	test.Equal(t, true, int(end.Sub(start)/time.Millisecond) >= outputBufferTimeout)

	frameType, data, _ := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	test.Equal(t, frameTypeMessage, frameType)
	test.Equal(t, msg.ID, msgOut.ID)
}

func TestOutputBufferingValidity(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    512 * 1024,
		"output_buffer_timeout": 1000,
	}, frameTypeResponse)
	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    -1,
		"output_buffer_timeout": -1,
	}, frameTypeResponse)
	identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 0,
	}, frameTypeResponse)
	data := identify(t, conn, map[string]interface{}{
		"output_buffer_size":    512*1024 + 1,
		"output_buffer_timeout": 0,
	}, frameTypeError)
	test.Equal(t, fmt.Sprintf("E_BAD_BODY IDENTIFY output buffer size (%d) is invalid", 512*1024+1), string(data))

	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 1001,
	}, frameTypeError)
	test.Equal(t, "E_BAD_BODY IDENTIFY output buffer timeout (1001) is invalid", string(data))
}

func TestTLS(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)
}

func TestTLSRequired(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRequired = TLSRequiredExceptHTTP

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_tls_required" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	subFail(t, conn, topicName, "ch")

	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)
}

func TestTLSAuthRequire(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// No Certs
	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	_, err = nsq.ReadResponse(tlsConn)
	test.NotNil(t, err)

	// With Unsigned Cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)

	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Nil(t, err)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)

}

func TestTLSAuthRequireVerify(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// with no cert
	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)
	_, err = nsq.ReadResponse(tlsConn)
	test.NotNil(t, err)

	// with invalid cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Nil(t, err)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	_, err = nsq.ReadResponse(tlsConn)
	test.NotNil(t, err)

	// with valid cert
	conn, err = mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"tls_v1": true,
	}, frameTypeResponse)
	r = struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)
	cert, err = tls.LoadX509KeyPair("./test/certs/client.pem", "./test/certs/client.key")
	test.Nil(t, err)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConn = tls.Client(conn, tlsConfig)
	err = tlsConn.Handshake()
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)
}

func TestDeflate(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.DeflateEnabled = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"deflate": true,
	}, frameTypeResponse)
	r := struct {
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.Deflate)

	compressConn := flate.NewReader(conn)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)
}

type readWriter struct {
	io.Reader
	io.Writer
}

func TestSnappy(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.SnappyEnabled = true
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"snappy": true,
	}, frameTypeResponse)
	r := struct {
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.Snappy)

	compressConn := snappy.NewReader(conn)
	resp, _ := nsq.ReadResponse(compressConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)

	msgBody := make([]byte, 128000)
	//lint:ignore SA1019 NewWriter is deprecated by NewBufferedWriter, but we don't want to buffer
	w := snappy.NewWriter(conn)

	rw := readWriter{compressConn, w}

	topicName := "test_snappy" + strconv.Itoa(int(time.Now().Unix()))
	sub(t, rw, topicName, "ch")

	_, err = nsq.Ready(1).WriteTo(rw)
	test.Nil(t, err)

	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), msgBody)
	topic.PutMessage(msg)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	test.Equal(t, frameTypeMessage, frameType)
	test.Equal(t, msg.ID, msgOut.ID)
	test.Equal(t, msg.Body, msgOut.Body)
}

func TestTLSDeflate(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.DeflateEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1":  true,
		"deflate": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1   bool `json:"tls_v1"`
		Deflate bool `json:"deflate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)
	test.Equal(t, true, r.Deflate)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)

	compressConn := flate.NewReader(tlsConn)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)
}

func TestSampling(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())

	num := 10000
	sampleRate := 42
	slack := 5

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxRdyCount = int64(num)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"sample_rate": int32(sampleRate),
	}, frameTypeResponse)
	r := struct {
		SampleRate int32 `json:"sample_rate"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, int32(sampleRate), r.SampleRate)

	topicName := "test_sampling" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < num; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test body"))
		topic.PutMessage(msg)
	}
	channel := topic.GetChannel("ch")

	// let the topic drain into the channel
	time.Sleep(50 * time.Millisecond)

	sub(t, conn, topicName, "ch")
	_, err = nsq.Ready(num).WriteTo(conn)
	test.Nil(t, err)

	go func() {
		for {
			_, err := nsq.ReadResponse(conn)
			if err != nil {
				return
			}
		}
	}()

	doneChan := make(chan int)
	go func() {
		for {
			if channel.Depth() == 0 {
				close(doneChan)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	<-doneChan

	channel.inFlightMutex.Lock()
	numInFlight := len(channel.inFlightMessages)
	channel.inFlightMutex.Unlock()

	test.Equal(t, true, numInFlight <= int(float64(num)*float64(sampleRate+slack)/100.0))
	test.Equal(t, true, numInFlight >= int(float64(num)*float64(sampleRate-slack)/100.0))
}

func TestTLSSnappy(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.SnappyEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": true,
		"snappy": true,
	}, frameTypeResponse)
	r := struct {
		TLSv1  bool `json:"tls_v1"`
		Snappy bool `json:"snappy"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, true, r.TLSv1)
	test.Equal(t, true, r.Snappy)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(tlsConn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)

	compressConn := snappy.NewReader(tlsConn)

	resp, _ = nsq.ReadResponse(compressConn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeResponse, frameType)
	test.Equal(t, []byte("OK"), data)
}

func TestClientMsgTimeout(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_cmsg_timeout" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	ch := topic.GetChannel("ch")
	msg := NewMessage(topic.GenerateID(), make([]byte, 100))
	topic.PutMessage(msg)

	// without this the race detector thinks there's a write
	// to msg.Attempts that races with the read in the protocol's messagePump...
	// it does not reflect a realistically possible condition
	topic.PutMessage(NewMessage(topic.GenerateID(), make([]byte, 100)))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"msg_timeout": 1000,
	}, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	test.Equal(t, 0, int(atomic.LoadUint64(&ch.timeoutCount)))
	test.Equal(t, 0, int(atomic.LoadUint64(&ch.requeueCount)))

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(conn)
	_, data, _ := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	test.Equal(t, msg.ID, msgOut.ID)
	test.Equal(t, msg.Body, msgOut.Body)

	_, err = nsq.Ready(0).WriteTo(conn)
	test.Nil(t, err)

	time.Sleep(1150 * time.Millisecond)

	test.Equal(t, 1, int(atomic.LoadUint64(&ch.timeoutCount)))
	test.Equal(t, 0, int(atomic.LoadUint64(&ch.requeueCount)))

	_, err = nsq.Finish(nsq.MessageID(msgOut.ID)).WriteTo(conn)
	test.Nil(t, err)

	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, fmt.Sprintf("E_FIN_FAILED FIN %s failed ID not in flight", msgOut.ID),
		string(data))
}

func TestBadFin(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{}, frameTypeResponse)
	sub(t, conn, "test_fin", "ch")

	fin := nsq.Finish(nsq.MessageID{})
	fin.Params[0] = []byte("")
	_, err = fin.WriteTo(conn)
	test.Nil(t, err)

	resp, _ := nsq.ReadResponse(conn)
	frameType, data, _ := nsq.UnpackResponse(resp)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_INVALID invalid message ID", string(data))
}

func TestReqTimeoutRange(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxReqTimeout = 1 * time.Minute
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_req" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.PutMessage(msg)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Nil(t, err)

	resp, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, _ := nsq.UnpackResponse(resp)
	msgOut, _ := decodeMessage(data)
	test.Equal(t, frameTypeMessage, frameType)
	test.Equal(t, msg.ID, msgOut.ID)

	_, err = nsq.Requeue(nsq.MessageID(msg.ID), -1).WriteTo(conn)
	test.Nil(t, err)

	// It should be immediately available for another attempt
	resp, err = nsq.ReadResponse(conn)
	test.Nil(t, err)
	frameType, data, _ = nsq.UnpackResponse(resp)
	msgOut, _ = decodeMessage(data)
	test.Equal(t, frameTypeMessage, frameType)
	test.Equal(t, msg.ID, msgOut.ID)

	// The priority (processing time) should be >= this
	minTs := time.Now().Add(opts.MaxReqTimeout).UnixNano()

	_, err = nsq.Requeue(nsq.MessageID(msg.ID), opts.MaxReqTimeout*2).WriteTo(conn)
	test.Nil(t, err)

	time.Sleep(100 * time.Millisecond)

	channel.deferredMutex.Lock()
	pqItem := channel.deferredMessages[msg.ID]
	channel.deferredMutex.Unlock()

	test.NotNil(t, pqItem)
	test.Equal(t, true, pqItem.Priority >= minTs)
}

func TestClientAuth(t *testing.T) {
	authResponse := `{"ttl":1, "authorizations":[]}`
	authSecret := "testsecret"
	authError := "E_UNAUTHORIZED AUTH no authorizations found"
	authSuccess := ""
	tlsEnabled := false
	commonName := ""
	httpAuthRequestMethod := "get"
	runAuthTest(t, authResponse, authSecret, authError, authSuccess, tlsEnabled, commonName, httpAuthRequestMethod)

	// now one that will succeed
	authResponse = `{"ttl":10, "authorizations":
		[{"topic":"test", "channels":[".*"], "permissions":["subscribe","publish"]}]
	}`
	authError = ""
	authSuccess = `{"identity":"","identity_url":"","permission_count":1}`
	runAuthTest(t, authResponse, authSecret, authError, authSuccess, tlsEnabled, commonName, httpAuthRequestMethod)

	// one with TLS enabled
	tlsEnabled = true
	commonName = "test.local"
	runAuthTest(t, authResponse, authSecret, authError, authSuccess, tlsEnabled, commonName, httpAuthRequestMethod)

	// test POST based authentication
	httpAuthRequestMethod = "post"
	runAuthTest(t, authResponse, authSecret, authError, authSuccess, tlsEnabled, commonName, httpAuthRequestMethod)

}

func runAuthTest(t *testing.T, authResponse string, authSecret string, authError string,
	authSuccess string, tlsEnabled bool, commonName string, httpAuthRequestMethod string) {
	var err error
	var expectedRemoteIP string
	expectedTLS := "false"
	if tlsEnabled {
		expectedTLS = "true"
	}

	authd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("in test auth handler %s", r.RequestURI)
		test.Equal(t, httpAuthRequestMethod, strings.ToLower(r.Method))

		var values url.Values

		if r.Method == "POST" {
			err = json.NewDecoder(r.Body).Decode(&values)
			if err != nil {
				t.Error(err)
			}
		} else {
			r.ParseForm()
			values = r.Form
		}
		test.Equal(t, expectedRemoteIP, values.Get("remote_ip"))
		test.Equal(t, expectedTLS, values.Get("tls"))
		test.Equal(t, commonName, values.Get("common_name"))
		test.Equal(t, authSecret, values.Get("secret"))
		fmt.Fprint(w, authResponse)
	}))
	defer authd.Close()

	addr, err := url.Parse(authd.URL)
	test.Nil(t, err)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.AuthHTTPAddresses = []string{addr.Host}
	opts.AuthHTTPRequestMethod = httpAuthRequestMethod
	if tlsEnabled {
		opts.TLSCert = "./test/certs/server.pem"
		opts.TLSKey = "./test/certs/server.key"
		opts.TLSClientAuthPolicy = "require"
	}
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	data := identify(t, conn, map[string]interface{}{
		"tls_v1": tlsEnabled,
	}, frameTypeResponse)
	r := struct {
		TLSv1 bool `json:"tls_v1"`
	}{}
	err = json.Unmarshal(data, &r)
	test.Nil(t, err)
	test.Equal(t, tlsEnabled, r.TLSv1)

	var c io.ReadWriter
	var tlsConn *tls.Conn
	c = conn
	if tlsEnabled {
		cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
		test.Nil(t, err)
		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}
		tlsConn = tls.Client(conn, tlsConfig)
		err = tlsConn.Handshake()
		test.Nil(t, err)
		c = tlsConn

		resp, _ := nsq.ReadResponse(tlsConn)
		frameType, data, _ := nsq.UnpackResponse(resp)
		t.Logf("frameType: %d, data: %s", frameType, data)
		test.Equal(t, frameTypeResponse, frameType)
		test.Equal(t, []byte("OK"), data)
	}

	expectedRemoteIP, _, _ = net.SplitHostPort(conn.LocalAddr().String())

	authCmd(t, c, authSecret, authSuccess)
	if authError != "" {
		readValidate(t, c, frameTypeError, authError)
	} else {
		sub(t, c, "test", "ch")
	}
}

func TestIOLoopReturnsClientErrWhenSendFails(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return 0, errors.New("write error")
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

func TestIOLoopReturnsClientErrWhenSendSucceeds(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return len(b), nil
	}

	testIOLoopReturnsClientErr(t, fakeConn)
}

func testIOLoopReturnsClientErr(t *testing.T, fakeConn test.FakeNetConn) {
	fakeConn.ReadFunc = func(b []byte) (int, error) {
		return copy(b, []byte("INVALID_COMMAND\n")), nil
	}

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG

	nsqd, err := New(opts)
	test.Nil(t, err)
	prot := &protocolV2{nsqd: nsqd}
	defer prot.nsqd.Exit()

	client := prot.NewClient(fakeConn)
	err = prot.IOLoop(client)
	test.NotNil(t, err)
	test.Equal(t, "E_INVALID invalid command INVALID_COMMAND", err.Error())
	test.NotNil(t, err.(*protocol.FatalClientErr))
}

func BenchmarkProtocolV2Exec(b *testing.B) {
	b.StopTimer()
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	nsqd, _ := New(opts)
	p := &protocolV2{nsqd}
	c := newClientV2(0, nil, nsqd)
	params := [][]byte{[]byte("NOP")}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		p.Exec(c, params)
	}
}

func benchmarkProtocolV2PubMultiTopic(b *testing.B, numTopics int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewOptions()
	size := 200
	batchSize := int(opts.MaxBodySize) / (size + 4)
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = msg
	}
	b.SetBytes(int64(len(msg)))
	b.StartTimer()

	for j := 0; j < numTopics; j++ {
		topicName := fmt.Sprintf("bench_v2_pub_multi_topic_%d_%d", j, time.Now().Unix())
		wg.Add(1)
		go func() {
			conn, err := mustConnectNSQD(tcpAddr)
			if err != nil {
				panic(err.Error())
			}
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			num := b.N / numTopics / batchSize
			wg.Add(1)
			go func() {
				for i := 0; i < num; i++ {
					cmd, _ := nsq.MultiPublish(topicName, batch)
					_, err := cmd.WriteTo(rw)
					if err != nil {
						panic(err.Error())
					}
					err = rw.Flush()
					if err != nil {
						panic(err.Error())
					}
				}
				wg.Done()
			}()
			wg.Add(1)
			go func() {
				for i := 0; i < num; i++ {
					resp, err := nsq.ReadResponse(rw)
					if err != nil {
						panic(err.Error())
					}
					_, data, _ := nsq.UnpackResponse(resp)
					if !bytes.Equal(data, []byte("OK")) {
						panic("invalid response")
					}
				}
				wg.Done()
			}()
			wg.Done()
		}()
	}

	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func BenchmarkProtocolV2PubMultiTopic1(b *testing.B)  { benchmarkProtocolV2PubMultiTopic(b, 1) }
func BenchmarkProtocolV2PubMultiTopic2(b *testing.B)  { benchmarkProtocolV2PubMultiTopic(b, 2) }
func BenchmarkProtocolV2PubMultiTopic4(b *testing.B)  { benchmarkProtocolV2PubMultiTopic(b, 4) }
func BenchmarkProtocolV2PubMultiTopic8(b *testing.B)  { benchmarkProtocolV2PubMultiTopic(b, 8) }
func BenchmarkProtocolV2PubMultiTopic16(b *testing.B) { benchmarkProtocolV2PubMultiTopic(b, 16) }
func BenchmarkProtocolV2PubMultiTopic32(b *testing.B) { benchmarkProtocolV2PubMultiTopic(b, 32) }

func benchmarkProtocolV2Pub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewOptions()
	batchSize := int(opts.MaxBodySize) / (size + 4)
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = msg
	}
	topicName := "bench_v2_pub" + strconv.Itoa(int(time.Now().Unix()))
	b.SetBytes(int64(len(msg)))
	b.StartTimer()

	for j := 0; j < runtime.GOMAXPROCS(0); j++ {
		wg.Add(1)
		go func() {
			conn, err := mustConnectNSQD(tcpAddr)
			if err != nil {
				panic(err.Error())
			}
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			num := b.N / runtime.GOMAXPROCS(0) / batchSize
			wg.Add(1)
			go func() {
				for i := 0; i < num; i++ {
					cmd, _ := nsq.MultiPublish(topicName, batch)
					_, err := cmd.WriteTo(rw)
					if err != nil {
						panic(err.Error())
					}
					err = rw.Flush()
					if err != nil {
						panic(err.Error())
					}
				}
				wg.Done()
			}()
			wg.Add(1)
			go func() {
				for i := 0; i < num; i++ {
					resp, err := nsq.ReadResponse(rw)
					if err != nil {
						panic(err.Error())
					}
					_, data, _ := nsq.UnpackResponse(resp)
					if !bytes.Equal(data, []byte("OK")) {
						panic("invalid response")
					}
				}
				wg.Done()
			}()
			wg.Done()
		}()
	}

	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func BenchmarkProtocolV2Pub256(b *testing.B)  { benchmarkProtocolV2Pub(b, 256) }
func BenchmarkProtocolV2Pub512(b *testing.B)  { benchmarkProtocolV2Pub(b, 512) }
func BenchmarkProtocolV2Pub1k(b *testing.B)   { benchmarkProtocolV2Pub(b, 1024) }
func BenchmarkProtocolV2Pub2k(b *testing.B)   { benchmarkProtocolV2Pub(b, 2*1024) }
func BenchmarkProtocolV2Pub4k(b *testing.B)   { benchmarkProtocolV2Pub(b, 4*1024) }
func BenchmarkProtocolV2Pub8k(b *testing.B)   { benchmarkProtocolV2Pub(b, 8*1024) }
func BenchmarkProtocolV2Pub16k(b *testing.B)  { benchmarkProtocolV2Pub(b, 16*1024) }
func BenchmarkProtocolV2Pub32k(b *testing.B)  { benchmarkProtocolV2Pub(b, 32*1024) }
func BenchmarkProtocolV2Pub64k(b *testing.B)  { benchmarkProtocolV2Pub(b, 64*1024) }
func BenchmarkProtocolV2Pub128k(b *testing.B) { benchmarkProtocolV2Pub(b, 128*1024) }
func BenchmarkProtocolV2Pub256k(b *testing.B) { benchmarkProtocolV2Pub(b, 256*1024) }
func BenchmarkProtocolV2Pub512k(b *testing.B) { benchmarkProtocolV2Pub(b, 512*1024) }
func BenchmarkProtocolV2Pub1m(b *testing.B)   { benchmarkProtocolV2Pub(b, 1024*1024) }

func benchmarkProtocolV2Sub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, size)
	topicName := "bench_v2_sub" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < b.N; i++ {
		msg := NewMessage(topic.GenerateID(), msg)
		topic.PutMessage(msg)
	}
	topic.GetChannel("ch")
	b.SetBytes(int64(len(msg)))
	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan)
			wg.Done()
		}()
		<-rdyChan
	}
	b.StartTimer()

	close(goChan)
	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func subWorker(n int, workers int, tcpAddr net.Addr, topicName string, rdyChan chan int, goChan chan int) {
	conn, err := mustConnectNSQD(tcpAddr)
	if err != nil {
		panic(err.Error())
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriterSize(conn, 65536))

	identify(nil, conn, nil, frameTypeResponse)
	sub(nil, conn, topicName, "ch")

	rdyCount := int(math.Min(math.Max(float64(n/workers), 1), 2500))
	rdyChan <- 1
	<-goChan
	nsq.Ready(rdyCount).WriteTo(rw)
	rw.Flush()
	num := n / workers
	for i := 0; i < num; i++ {
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType != frameTypeMessage {
			panic("got something else")
		}
		msg, err := decodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(rw)
		if (i+1)%rdyCount == 0 || i+1 == num {
			if i+1 == num {
				nsq.Ready(0).WriteTo(conn)
			}
			rw.Flush()
		}
	}
}

func BenchmarkProtocolV2Sub256(b *testing.B)  { benchmarkProtocolV2Sub(b, 256) }
func BenchmarkProtocolV2Sub512(b *testing.B)  { benchmarkProtocolV2Sub(b, 512) }
func BenchmarkProtocolV2Sub1k(b *testing.B)   { benchmarkProtocolV2Sub(b, 1024) }
func BenchmarkProtocolV2Sub2k(b *testing.B)   { benchmarkProtocolV2Sub(b, 2*1024) }
func BenchmarkProtocolV2Sub4k(b *testing.B)   { benchmarkProtocolV2Sub(b, 4*1024) }
func BenchmarkProtocolV2Sub8k(b *testing.B)   { benchmarkProtocolV2Sub(b, 8*1024) }
func BenchmarkProtocolV2Sub16k(b *testing.B)  { benchmarkProtocolV2Sub(b, 16*1024) }
func BenchmarkProtocolV2Sub32k(b *testing.B)  { benchmarkProtocolV2Sub(b, 32*1024) }
func BenchmarkProtocolV2Sub64k(b *testing.B)  { benchmarkProtocolV2Sub(b, 64*1024) }
func BenchmarkProtocolV2Sub128k(b *testing.B) { benchmarkProtocolV2Sub(b, 128*1024) }
func BenchmarkProtocolV2Sub256k(b *testing.B) { benchmarkProtocolV2Sub(b, 256*1024) }
func BenchmarkProtocolV2Sub512k(b *testing.B) { benchmarkProtocolV2Sub(b, 512*1024) }
func BenchmarkProtocolV2Sub1m(b *testing.B)   { benchmarkProtocolV2Sub(b, 1024*1024) }

func benchmarkProtocolV2MultiSub(b *testing.B, num int) {
	var wg sync.WaitGroup
	b.StopTimer()

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, 256)
	b.SetBytes(int64(len(msg) * num))

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < num; i++ {
		topicName := "bench_v2" + strconv.Itoa(b.N) + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(int(time.Now().Unix()))
		topic := nsqd.GetTopic(topicName)
		for i := 0; i < b.N; i++ {
			msg := NewMessage(topic.GenerateID(), msg)
			topic.PutMessage(msg)
		}
		topic.GetChannel("ch")

		for j := 0; j < workers; j++ {
			wg.Add(1)
			go func() {
				subWorker(b.N, workers, tcpAddr, topicName, rdyChan, goChan)
				wg.Done()
			}()
			<-rdyChan
		}
	}
	b.StartTimer()

	close(goChan)
	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}

func BenchmarkProtocolV2MultiSub1(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 1) }
func BenchmarkProtocolV2MultiSub2(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 2) }
func BenchmarkProtocolV2MultiSub4(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 4) }
func BenchmarkProtocolV2MultiSub8(b *testing.B)  { benchmarkProtocolV2MultiSub(b, 8) }
func BenchmarkProtocolV2MultiSub16(b *testing.B) { benchmarkProtocolV2MultiSub(b, 16) }
