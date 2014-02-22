package rpc4go

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/rpc"

	"code.google.com/p/goprotobuf/proto"
	rpc4 "github.com/contester/rpc4/proto"
)

type ServerCodec struct {
	r *bufio.Reader
	w io.WriteCloser

	hasPayload bool
	Shutdown bool
}

type ProtoReader interface {
	io.Reader
	io.ByteReader
}

func readProto(r ProtoReader, pb proto.Message) (err error) {
	var size uint32
	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		return
	}
	buf := make([]byte, size)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	if pb != nil {
		err = proto.Unmarshal(buf, pb)
	}
	return
}

// Unbuffered
func writeData(w io.Writer, data []byte) (err error) {
	if err = binary.Write(w, binary.BigEndian, proto.Uint32(uint32(len(data)))); err != nil {
		return
	}
	_, err = w.Write(data)
	return
}

func writeProto(w io.Writer, pb proto.Message) error {
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	return writeData(w, data)
}

func NewServerCodec(conn net.Conn) *ServerCodec {
	return &ServerCodec{r: bufio.NewReader(conn), w: conn, hasPayload: false}
}

func (s *ServerCodec) ReadRequestHeader(req *rpc.Request) error {
	if s.Shutdown {
		return rpc.ErrShutdown
	}

	var header rpc4.Header
	if err := readProto(s.r, &header); err != nil {
		return err
	}
	if header.GetMethod() == "" {
		return fmt.Errorf("header missing method: %s", header)
	}

	req.ServiceMethod = header.GetMethod()
	req.Seq = header.GetSequence()

	s.hasPayload = header.GetPayloadPresent()

	return nil
}

func (s *ServerCodec) ReadRequestBody(pb interface{}) error {
	if s.hasPayload {
		return readProto(s.r, pb.(proto.Message))
	}
	return nil
}

func (s *ServerCodec) WriteResponse(resp *rpc.Response, pb interface{}) (err error) {
	var header rpc4.Header
	header.Method, header.Sequence, header.MessageType = &resp.ServiceMethod, &resp.Seq, rpc4.Header_RESPONSE.Enum()

	var data []byte

	if resp.Error == "" {
		if data, err = proto.Marshal(pb.(proto.Message)); err != nil {
			resp.Error = err.Error()
		}
	}

	if resp.Error != "" {
		header.MessageType = rpc4.Header_ERROR.Enum()
		data = []byte(resp.Error)
	}

	if len(data) > 0 {
		header.PayloadPresent = proto.Bool(true)
	}

	if err = writeProto(s.w, &header); err == nil {
		if header.GetPayloadPresent() {
			err = writeData(s.w, data)
		}
	}

	return
}

// Close closes the underlying conneciton.
func (s *ServerCodec) Close() error {
	return s.w.Close()
}
