package rpc4go

import (
	"bytes"
	"net/rpc"
	"testing"

	"bufio"
)

type bufc struct {
	bytes.Buffer
}

func (b *bufc) Close() error { return nil }

func TestRW(t *testing.T) {
	var buf bufc

	var c Codec
	c.w = &buf
	var req rpc.Request
	req.Seq = 1
	req.ServiceMethod = "foo"

	c.WriteRequest(&req, nil)
	c.r = bufio.NewReader(&buf)
	c.ReadRequestHeader(&req)

	if req.Seq != 1 || req.ServiceMethod != "foo" {
		t.Errorf("Request decode/encode mismatch")
	}

}
