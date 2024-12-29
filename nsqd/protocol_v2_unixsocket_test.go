package nsqd

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/test"
)

func mustUnixSocketStartNSQD(opts *Options) (net.Addr, net.Addr, *NSQD) {
	tmpDir := os.TempDir()
	opts.TCPAddress = path.Join(tmpDir, fmt.Sprintf("nsqd-%d.sock", rand.Int()))
	opts.HTTPAddress = path.Join(tmpDir, fmt.Sprintf("nsqd-%d.sock", rand.Int()))

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

func mustUnixSocketConnectNSQD(addr net.Addr) (net.Conn, error) {
	conn, err := net.DialTimeout("unix", addr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}

// exercise the basic operations of the V2 protocol
func TestUnixSocketBasicV2(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketMultipleConsumerV2(t *testing.T) {
	msgChan := make(chan *Message)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 60 * time.Second
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_multiple_v2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.GetChannel("ch1")
	topic.GetChannel("ch2")
	topic.PutMessage(msg)

	for _, i := range []string{"1", "2"} {
		conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketClientTimeout(t *testing.T) {
	topicName := "test_client_timeout_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 150 * time.Millisecond
	opts.LogLevel = LOG_DEBUG
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketClientHeartbeat(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketClientHeartbeatDisableSUB(t *testing.T) {
	topicName := "test_hb_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 200 * time.Millisecond
	opts.LogLevel = LOG_DEBUG
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)
	subFail(t, conn, topicName, "ch")
}

func TestUnixSocketClientHeartbeatDisable(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.ClientTimeout = 100 * time.Millisecond
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
	test.Nil(t, err)
	defer conn.Close()

	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": -1,
	}, frameTypeResponse)

	time.Sleep(150 * time.Millisecond)

	_, err = nsq.Nop().WriteTo(conn)
	test.Nil(t, err)
}

func TestUnixSocketMaxHeartbeatIntervalValid(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
	test.Nil(t, err)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval / time.Millisecond)
	identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeResponse)
}

func TestUnixSocketMaxHeartbeatIntervalInvalid(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxHeartbeatInterval = 300 * time.Second
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
	test.Nil(t, err)
	defer conn.Close()

	hbi := int(opts.MaxHeartbeatInterval/time.Millisecond + 1)
	data := identify(t, conn, map[string]interface{}{
		"heartbeat_interval": hbi,
	}, frameTypeError)
	test.Equal(t, "E_BAD_BODY IDENTIFY heartbeat interval (300001) is invalid", string(data))
}

func TestUnixSocketPausing(t *testing.T) {
	topicName := "test_pause_v2" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketEmptyCommand(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
	test.Nil(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("\n\n"))
	test.Nil(t, err)

	// if we didn't panic here we're good, see issue #120
}

func TestUnixSocketSizeLimits(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxMsgSize = 100
	opts.MaxBodySize = 1000
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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
	conn, err = mustUnixSocketConnectNSQD(addr)
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
	conn, err = mustUnixSocketConnectNSQD(addr)
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
	conn, err = mustUnixSocketConnectNSQD(addr)
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
	conn, err = mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketDPUB(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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
	nsq.DeferredPublish(topicName, opts.MaxDeferDelay+100*time.Millisecond, make([]byte, 100)).WriteTo(conn)
	resp, _ = nsq.ReadResponse(conn)
	frameType, data, _ = nsq.UnpackResponse(resp)
	t.Logf("frameType: %d, data: %s", frameType, data)
	test.Equal(t, frameTypeError, frameType)
	test.Equal(t, "E_INVALID DPUB defer delay 3600100 out of range 0-3600000", string(data))
}

func TestUnixSocketTouch(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MsgTimeout = 150 * time.Millisecond
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_touch" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketMaxRdyCount(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxRdyCount = 50
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_max_rdy_count" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketFatalError(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketOutputBuffering(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_output_buffering" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketOutputBufferingValidity(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxOutputBufferSize = 512 * 1024
	opts.MaxOutputBufferTimeout = time.Second
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

	conn, err = mustUnixSocketConnectNSQD(addr)
	test.Nil(t, err)
	defer conn.Close()

	data = identify(t, conn, map[string]interface{}{
		"output_buffer_size":    0,
		"output_buffer_timeout": 1001,
	}, frameTypeError)
	test.Equal(t, "E_BAD_BODY IDENTIFY output buffer timeout (1001) is invalid", string(data))
}

func TestUnixSocketTLS(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketTLSRequired(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRequired = TLSRequiredExceptHTTP

	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_tls_required" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustUnixSocketConnectNSQD(addr)
	test.Nil(t, err)
	defer conn.Close()

	subFail(t, conn, topicName, "ch")

	conn, err = mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketTLSAuthRequire(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"

	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// No Certs
	conn, err := mustUnixSocketConnectNSQD(addr)
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
	conn, err = mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketTLSAuthRequireVerify(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"

	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// with no cert
	conn, err := mustUnixSocketConnectNSQD(addr)
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
	conn, err = mustUnixSocketConnectNSQD(addr)
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
	conn, err = mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketDeflate(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.DeflateEnabled = true
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketSnappy(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.SnappyEnabled = true
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketTLSDeflate(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.DeflateEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketSampling(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())

	num := 10000
	sampleRate := 42
	slack := 5

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxRdyCount = int64(num)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketTLSSnappy(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.SnappyEnabled = true
	opts.TLSCert = "./test/certs/cert.pem"
	opts.TLSKey = "./test/certs/key.pem"
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketClientMsgTimeout(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
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

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketBadFin(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketReqTimeoutRange(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.MaxReqTimeout = 1 * time.Minute
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_req" + strconv.Itoa(int(time.Now().Unix()))

	conn, err := mustUnixSocketConnectNSQD(addr)
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

func TestUnixSocketIOLoopReturnsClientErrWhenSendFails(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return 0, errors.New("write error")
	}

	testUnixSocketIOLoopReturnsClientErr(t, fakeConn)
}

func TestUnixSocketIOLoopReturnsClientErrWhenSendSucceeds(t *testing.T) {
	fakeConn := test.NewFakeNetConn()
	fakeConn.WriteFunc = func(b []byte) (int, error) {
		return len(b), nil
	}

	testUnixSocketIOLoopReturnsClientErr(t, fakeConn)
}

func testUnixSocketIOLoopReturnsClientErr(t *testing.T, fakeConn test.FakeNetConn) {
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

func BenchmarkUnixSocketProtocolV2Exec(b *testing.B) {
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

func benchmarkUnixSocketProtocolV2PubMultiTopic(b *testing.B, numTopics int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewOptions()
	size := 200
	batchSize := int(opts.MaxBodySize) / (size + 4)
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
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
			conn, err := mustUnixSocketConnectNSQD(addr)
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

func BenchmarkUnixSocketProtocolV2PubMultiTopic1(b *testing.B) {
	benchmarkUnixSocketProtocolV2PubMultiTopic(b, 1)
}
func BenchmarkUnixSocketkProtocolV2PubMultiTopic2(b *testing.B) {
	benchmarkUnixSocketProtocolV2PubMultiTopic(b, 2)
}
func BenchmarkUnixSocketProtocolV2PubMultiTopic4(b *testing.B) {
	benchmarkUnixSocketProtocolV2PubMultiTopic(b, 4)
}
func BenchmarkUnixSocketProtocolV2PubMultiTopic8(b *testing.B) {
	benchmarkUnixSocketProtocolV2PubMultiTopic(b, 8)
}
func BenchmarkUnixSocketProtocolV2PubMultiTopic16(b *testing.B) {
	benchmarkUnixSocketProtocolV2PubMultiTopic(b, 16)
}
func BenchmarkUnixSocketProtocolV2PubMultiTopic32(b *testing.B) {
	benchmarkUnixSocketProtocolV2PubMultiTopic(b, 32)
}

func benchmarkUnixSocketProtocolV2Pub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewOptions()
	batchSize := int(opts.MaxBodySize) / (size + 4)
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
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
			conn, err := mustUnixSocketConnectNSQD(addr)
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

func BenchmarkUnixSocketProtocolV2Pub256(b *testing.B) { benchmarkUnixSocketProtocolV2Pub(b, 256) }
func BenchmarkUnixSocketProtocolV2Pub512(b *testing.B) { benchmarkUnixSocketProtocolV2Pub(b, 512) }
func BenchmarkUnixSocketProtocolV2Pub1k(b *testing.B)  { benchmarkUnixSocketProtocolV2Pub(b, 1024) }
func BenchmarkUnixSocketProtocolV2Pub2k(b *testing.B)  { benchmarkUnixSocketProtocolV2Pub(b, 2*1024) }
func BenchmarkUnixSocketProtocolV2Pub4k(b *testing.B)  { benchmarkUnixSocketProtocolV2Pub(b, 4*1024) }
func BenchmarkUnixSocketProtocolV2Pub8k(b *testing.B)  { benchmarkUnixSocketProtocolV2Pub(b, 8*1024) }
func BenchmarkUnixSocketProtocolV2Pub16k(b *testing.B) { benchmarkUnixSocketProtocolV2Pub(b, 16*1024) }
func BenchmarkUnixSocketProtocolV2Pub32k(b *testing.B) { benchmarkUnixSocketProtocolV2Pub(b, 32*1024) }
func BenchmarkUnixSocketProtocolV2Pub64k(b *testing.B) { benchmarkUnixSocketProtocolV2Pub(b, 64*1024) }
func BenchmarkUnixSocketProtocolV2Pub128k(b *testing.B) {
	benchmarkUnixSocketProtocolV2Pub(b, 128*1024)
}
func BenchmarkUnixSocketProtocolV2Pub256k(b *testing.B) {
	benchmarkUnixSocketProtocolV2Pub(b, 256*1024)
}
func BenchmarkUnixSocketProtocolV2Pub512k(b *testing.B) {
	benchmarkUnixSocketProtocolV2Pub(b, 512*1024)
}
func BenchmarkUnixSocketProtocolV2Pub1m(b *testing.B) { benchmarkUnixSocketProtocolV2Pub(b, 1024*1024) }

func benchmarkUnixSocketProtocolV2Sub(b *testing.B, size int) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
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
			subWorker(b.N, workers, addr, topicName, rdyChan, goChan)
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

func subUnixSocketWorker(n int, workers int, addr net.Addr, topicName string, rdyChan chan int, goChan chan int) {
	conn, err := mustUnixSocketConnectNSQD(addr)
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

func BenchmarkUnixSocketProtocolV2Sub256(b *testing.B) { benchmarkUnixSocketProtocolV2Sub(b, 256) }
func BenchmarkUnixSocketProtocolV2Sub512(b *testing.B) { benchmarkUnixSocketProtocolV2Sub(b, 512) }
func BenchmarkUnixSocketProtocolV2Sub1k(b *testing.B)  { benchmarkUnixSocketProtocolV2Sub(b, 1024) }
func BenchmarkUnixSocketProtocolV2Sub2k(b *testing.B)  { benchmarkUnixSocketProtocolV2Sub(b, 2*1024) }
func BenchmarkUnixSocketProtocolV2Sub4k(b *testing.B)  { benchmarkUnixSocketProtocolV2Sub(b, 4*1024) }
func BenchmarkUnixSocketProtocolV2Sub8k(b *testing.B)  { benchmarkUnixSocketProtocolV2Sub(b, 8*1024) }
func BenchmarkUnixSocketProtocolV2Sub16k(b *testing.B) { benchmarkUnixSocketProtocolV2Sub(b, 16*1024) }
func BenchmarkUnixSocketProtocolV2Sub32k(b *testing.B) { benchmarkUnixSocketProtocolV2Sub(b, 32*1024) }
func BenchmarkUnixSocketProtocolV2Sub64k(b *testing.B) { benchmarkUnixSocketProtocolV2Sub(b, 64*1024) }
func BenchmarkUnixSocketProtocolV2Sub128k(b *testing.B) {
	benchmarkUnixSocketProtocolV2Sub(b, 128*1024)
}
func BenchmarkUnixSocketProtocolV2Sub256k(b *testing.B) {
	benchmarkUnixSocketProtocolV2Sub(b, 256*1024)
}
func BenchmarkUnixSocketProtocolV2Sub512k(b *testing.B) {
	benchmarkUnixSocketProtocolV2Sub(b, 512*1024)
}
func BenchmarkUnixSocketProtocolV2Sub1m(b *testing.B) { benchmarkUnixSocketProtocolV2Sub(b, 1024*1024) }

func benchmarkUnixSocketProtocolV2MultiSub(b *testing.B, num int) {
	var wg sync.WaitGroup
	b.StopTimer()

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	addr, _, nsqd := mustUnixSocketStartNSQD(opts)
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
				subUnixSocketWorker(b.N, workers, addr, topicName, rdyChan, goChan)
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

func BenchmarkUnixSocketProtocolV2MultiSub2(b *testing.B) {
	benchmarkUnixSocketProtocolV2MultiSub(b, 2)
}
func BenchmarkUnixSocketProtocolV2MultiSub1(b *testing.B) {
	benchmarkUnixSocketProtocolV2MultiSub(b, 1)
}
func BenchmarkUnixSocketProtocolV2MultiSub4(b *testing.B) {
	benchmarkUnixSocketProtocolV2MultiSub(b, 4)
}
func BenchmarkUnixSocketProtocolV2MultiSub8(b *testing.B) {
	benchmarkUnixSocketProtocolV2MultiSub(b, 8)
}
func BenchmarkUnixSocketProtocolV2MultiSub16(b *testing.B) {
	benchmarkUnixSocketProtocolV2MultiSub(b, 16)
}
