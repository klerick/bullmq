package bullmq

import (
	"fmt"
	"strconv"
)

// toInt64 coerces a value decoded from Redis/msgpack (int64, float64, string, …)
// into an int64. Redis integer replies arrive as int64, Lua numbers may arrive as
// float64, and hash fields as strings.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	case []byte:
		i, _ := strconv.ParseInt(string(n), 10, 64)
		return i
	default:
		return 0
	}
}

// toFloat64 coerces a value into a float64.
func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	default:
		return 0
	}
}

// toStr coerces a Redis/msgpack value into a string.
func toStr(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	case int64:
		return strconv.FormatInt(s, 10)
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// flatArrayToMap turns a flat [k1, v1, k2, v2, …] reply (as HGETALL returns inside
// a script) into a string map. Mirrors python scripts.py::array2obj.
func flatArrayToMap(flat []any) map[string]string {
	m := make(map[string]string, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		m[toStr(flat[i])] = toStr(flat[i+1])
	}
	return m
}
