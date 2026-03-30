package utils

import (
	"io"
)

// WriteFull 将缓冲区完整写入目标 writer，遇到短写会自动重试。
// 返回的第一个非 nil 错误应被视作写入失败，调用方需自行恢复（例如关闭底层连接）。
func WriteFull(w io.Writer, buf []byte) error {
	if w == nil || len(buf) == 0 {
		return nil
	}

	total := 0
	for total < len(buf) {
		n, err := w.Write(buf[total:])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		total += n
	}
	return nil
}
