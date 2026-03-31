package protocol

import "testing"

// FuzzDecodePayload 用来验证生成出来的 payload codec 在健壮性上的表现。
//
// 目标是：对任意字节做解码都绝不能 panic，而成功解码的结果应当还能再次编码。
// 这是一个轻量级 fuzz harness（默认不会执行，除非使用 `go test -fuzz=...`）。
func FuzzDecodePayload(f *testing.F) {
	f.Add(uint16(HI), []byte{})
	f.Add(uint16(UUID), []byte{0x00})
	f.Add(uint16(DTN_DATA), []byte{0x00, 0x01, 0x02})
	f.Add(uint16(STREAM_DATA), []byte{0x00, 0x00, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, messageType uint16, data []byte) {
		// 为本地运行控制一个相对合理的 fuzz 输入上界。
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
