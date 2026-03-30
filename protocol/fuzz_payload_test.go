package protocol

import "testing"

// FuzzDecodePayload exercises the generated payload codecs for robustness.
//
// Goal: decoding arbitrary bytes must never panic, and successful decodes should be re-encodable.
// This is a lightweight fuzz harness (not executed by default unless `go test -fuzz=...` is used).
func FuzzDecodePayload(f *testing.F) {
	f.Add(uint16(HI), []byte{})
	f.Add(uint16(UUID), []byte{0x00})
	f.Add(uint16(DTN_DATA), []byte{0x00, 0x01, 0x02})
	f.Add(uint16(STREAM_DATA), []byte{0x00, 0x00, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, messageType uint16, data []byte) {
		// Keep fuzz inputs reasonably bounded for local runs.
		if len(data) > 1<<16 {
			return
		}
		msg, err := DecodePayload(messageType, data)
		if err != nil || msg == nil {
			return
		}
		enc, err := EncodePayload(msg)
		if err != nil {
			t.Fatalf("re-encode failed for type=0x%04x msg=%T: %v", messageType, msg, err)
		}
		_, _ = DecodePayload(messageType, enc)
	})
}

