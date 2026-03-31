package streamopts

import (
	"net/url"
	"sort"
	"strings"
)

// Parse 会把 key=value;... 转成 map，并解码其中经过 URL 转义的值。
func Parse(opt string) map[string]string {
	result := make(map[string]string)
	fields := strings.Split(opt, ";")
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		kv := strings.SplitN(field, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if decoded, err := url.QueryUnescape(val); err == nil {
			val = decoded
		}
		if key != "" {
			result[key] = val
		}
	}
	return result
}

// Encode 会构造一个确定顺序的 key=value 列表，并对值做 URL 转义。
func Encode(meta map[string]string) string {
	if len(meta) == 0 {
		return ""
	}
	keys := make([]string, 0, len(meta))
	for k := range meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+url.QueryEscape(meta[k]))
	}
	return strings.Join(parts, ";")
}
