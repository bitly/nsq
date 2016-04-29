package nsqdserver

import (
	"io"
	"net"

	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/nsqd"
)

type tcpServer struct {
	ctx *context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		nsqd.NsqLogger().LogErrorf(" failed to read protocol version - %s", err)
		return
	}
	protocolMagic := string(buf)

	nsqd.NsqLogger().LogDebugf("new CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		prot = &protocolV2{ctx: p.ctx}
	default:
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		nsqd.NsqLogger().LogErrorf("client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("client(%s) error - %s", clientConn.RemoteAddr(), err)
		return
	}
}