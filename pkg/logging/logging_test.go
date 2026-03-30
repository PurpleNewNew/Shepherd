package logging

import (
	"bytes"
	"strings"
	"testing"
)

type bufferSink struct {
	buf bytes.Buffer
}

func (s *bufferSink) Log(level Level, subsystem, message string) {
	s.buf.WriteString(string(level))
	s.buf.WriteString("|")
	s.buf.WriteString(subsystem)
	s.buf.WriteString("|")
	s.buf.WriteString(message)
}

func TestLoggerWritesThroughSink(t *testing.T) {
	sink := &bufferSink{}
	setSink(sink)
	defer setSink(nil)

	logger := New("test")
	logger.Infof("hello %s", "world")

	out := sink.buf.String()
	if !strings.Contains(out, "INFO|test|hello world") {
		t.Fatalf("unexpected sink output: %s", out)
	}
}
