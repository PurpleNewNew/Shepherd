package crypto

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// GzipCompress 使用 gzip 压缩输入字节并返回结果。
func GzipCompress(src []byte) ([]byte, error) {
	var in bytes.Buffer
	w := gzip.NewWriter(&in)
	if _, err := w.Write(src); err != nil {
		w.Close()
		return nil, fmt.Errorf("gzip: write payload: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip: close writer: %w", err)
	}
	return in.Bytes(), nil
}

// GzipDecompressLimit 解压 gzip 数据，并限制最大输出字节数（max < 0 表示不限制）。
func GzipDecompressLimit(src []byte, max int64) ([]byte, error) {
	br := bytes.NewReader(src)
	gr, err := gzip.NewReader(br)
	if err != nil {
		return nil, fmt.Errorf("gzip: new reader: %w", err)
	}
	defer gr.Close()

	reader := io.Reader(gr)
	if max >= 0 {
		reader = io.LimitReader(gr, max+1)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("gzip: read payload: %w", err)
	}
	if max >= 0 && int64(len(data)) > max {
		return nil, fmt.Errorf("gzip: payload exceeds limit %d bytes", max)
	}
	return data, nil
}
