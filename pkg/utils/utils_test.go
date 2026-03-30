package utils

import (
	"reflect"
	"testing"
)

func TestCheckIPPort(t *testing.T) {
	addr, reuse, err := CheckIPPort("8080")
	if err != nil || addr != "0.0.0.0:8080" || reuse != "0.0.0.0:8080" {
		t.Fatalf("unexpected result %s %s %v", addr, reuse, err)
	}

	addr, reuse, err = CheckIPPort("127.0.0.1:9000")
	if err != nil || addr != "127.0.0.1:9000" || reuse != "0.0.0.0:9000" {
		t.Fatalf("unexpected ip result %s %s %v", addr, reuse, err)
	}

	if _, _, err = CheckIPPort("bad:format:9000"); err == nil {
		t.Fatalf("expected error for invalid address")
	}
}

func TestCheckRange(t *testing.T) {
	values := []int{5, 1, 3, 2}
	CheckRange(values)
	expected := []int{1, 2, 3, 5}
	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("expected sorted %v, got %v", expected, values)
	}
}

func TestConvertGBKRoundTrip(t *testing.T) {
	src := "测试GBK编码"
	gbk := ConvertStr2GBK(src)
	back := ConvertGBK2Str(gbk)
	if back != src {
		t.Fatalf("expected round-trip conversion, got %s", back)
	}
}
