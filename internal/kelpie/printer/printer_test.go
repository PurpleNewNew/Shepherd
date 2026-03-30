package printer

import "testing"

func TestInitPrinter(t *testing.T) {
	InitPrinter()
	Warning("test warning %s", "message")
	Success("test success")
	Fail("test fail %d", 1)
}
