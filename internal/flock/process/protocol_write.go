package process

import (
	"fmt"
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

func sendPreparedProtocolMessageWithDeadline(conn net.Conn, message protocol.Message, timeout time.Duration) error {
	if conn != nil && timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	}
	return sendPreparedProtocolMessage(message)
}

func sendPreparedProtocolMessage(message protocol.Message) error {
	if message == nil {
		return ErrInvalidDownstreamMessage
	}
	switch m := message.(type) {
	case *protocol.RawMessage:
		return sendPreparedRawMessage(m)
	case *protocol.WSMessage:
		if m == nil {
			return ErrInvalidDownstreamMessage
		}
		return sendPreparedRawMessage(m.RawMessage)
	default:
		message.SendMessage()
		return nil
	}
}

func sendPreparedRawMessage(message *protocol.RawMessage) error {
	if message == nil {
		return ErrInvalidDownstreamMessage
	}
	if message.Conn == nil {
		return ErrNoUpstreamSession
	}
	final := append([]byte(nil), message.HeaderBuffer...)
	final = append(final, message.DataBuffer...)
	message.HeaderBuffer = nil
	message.DataBuffer = nil
	if len(final) == 0 {
		return fmt.Errorf("%w: empty prepared message", ErrInvalidDownstreamMessage)
	}
	if err := utils.WriteFull(message.Conn, final); err != nil {
		_ = message.Conn.Close()
		return err
	}
	return nil
}
