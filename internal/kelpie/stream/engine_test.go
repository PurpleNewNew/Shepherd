package stream

import (
	"encoding/hex"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

func TestHandleData_ReorderBuffering(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		acks []uint32
	)
	sendFn := func(_ string, payload []byte) error {
		s := string(payload)
		if !strings.HasPrefix(s, "proto:") {
			return nil
		}
		parts := strings.SplitN(s, ":", 3)
		if len(parts) != 3 {
			return nil
		}
		mt, err := strconv.ParseUint(parts[1], 16, 16)
		if err != nil {
			return nil
		}
		raw, err := hex.DecodeString(parts[2])
		if err != nil {
			return nil
		}
		if uint16(mt) != protocol.STREAM_ACK {
			return nil
		}
		msg, err := protocol.DecodePayload(protocol.STREAM_ACK, raw)
		if err != nil {
			return nil
		}
		ack, ok := msg.(*protocol.StreamAck)
		if !ok || ack == nil {
			return nil
		}
		mu.Lock()
		acks = append(acks, ack.Ack)
		mu.Unlock()
		return nil
	}

	e := New(DefaultConfig(), sendFn, nil)
	s := e.Accept(123, Options{Target: "t", Meta: map[string]string{}})
	if s == nil {
		t.Fatal("Accept returned nil stream")
	}
	defer s.Close()

	want := []byte("AB")
	got := make([]byte, len(want))
	readErr := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(s, got)
		readErr <- err
	}()

	// 先发送 seq=2 再发送 seq=1；此时应先缓存，暂不投递。
	e.HandleData(&protocol.StreamData{StreamID: 123, Seq: 2, Payload: []byte("B")})
	e.HandleData(&protocol.StreamData{StreamID: 123, Seq: 1, Payload: []byte("A")})
	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for reordered delivery")
	}
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", string(got), string(want))
	}

	// 发送重复帧不应额外投递更多字节。
	e.HandleData(&protocol.StreamData{StreamID: 123, Seq: 1, Payload: []byte("A")})

	mu.Lock()
	defer mu.Unlock()
	if len(acks) < 2 {
		t.Fatalf("expected at least 2 ACKs, got %d (%v)", len(acks), acks)
	}
	if acks[len(acks)-1] != 2 {
		t.Fatalf("expected last ACK=2, got %d (all=%v)", acks[len(acks)-1], acks)
	}
}

func TestHandleData_RxBufferOverflowAborts(t *testing.T) {
	t.Parallel()

	e := New(DefaultConfig(), func(string, []byte) error { return nil }, nil)
	s := e.Accept(456, Options{Target: "t", Meta: map[string]string{}})
	if s == nil {
		t.Fatal("Accept returned nil stream")
	}
	defer s.Close()

	// 在保持 seq=1 缺失的情况下，用乱序帧填满 rxBuf。
	for i := uint32(2); i < 2+257; i++ {
		e.HandleData(&protocol.StreamData{StreamID: 456, Seq: i, Payload: []byte("x")})
	}

	errCh := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, err := s.Read(buf[:])
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected read error after abort, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for stream abort")
	}

	// 该会话应当被移除。
	if meta := e.SessionMeta(456); meta != nil {
		t.Fatalf("expected session to be removed, got meta=%v", meta)
	}
}
