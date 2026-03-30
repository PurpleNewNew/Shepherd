package streamopts

import (
	"net/url"
	"sort"
	"strings"
)

// Parse converts key=value;... into a map, decoding URL-escaped values.
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

// Encode builds deterministic key=value list with URL-escaped values.
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
