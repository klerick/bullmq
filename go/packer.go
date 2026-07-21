package bullmq

import (
	"bytes"
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

// optsDecodeMap maps short stored option keys back to their long form.
// Ported from python/bullmq/job.py optsDecodeMap (identical across ports).
var optsDecodeMap = map[string]string{
	"fpof": "failParentOnFailure",
	"cpof": "continueParentOnFailure",
	"idof": "ignoreDependencyOnFailure",
	"rdof": "removeDependencyOnFailure",
	"kl":   "keepLogs",
	"de":   "deduplication",
}

// optsEncodeMap is the inverse of optsDecodeMap (long form -> short storage key).
var optsEncodeMap = func() map[string]string {
	m := make(map[string]string, len(optsDecodeMap))
	for short, long := range optsDecodeMap {
		m[long] = short
	}
	return m
}()

// encodeOpts rewrites long option keys to their short storage form before packing,
// leaving unmapped keys untouched. Mirrors python scripts.py::encodeOpts.
func encodeOpts(opts map[string]any) map[string]any {
	encoded := make(map[string]any, len(opts))
	for key, value := range opts {
		if short, ok := optsEncodeMap[key]; ok {
			encoded[short] = value
		} else {
			encoded[key] = value
		}
	}
	return encoded
}

// marshalJSON encodes v as compact JSON WITHOUT HTML escaping, matching the wire
// format Node (JSON.stringify) and Python (separators=(',',':')) produce. Go's
// json.Marshal always HTML-escapes, so we use an Encoder with SetEscapeHTML(false)
// and trim the trailing newline it appends. See PLAN §5.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// packMsgpack encodes v as msgpack. BullMQ's Lua scripts unpack these via
// cmsgpack, which is format-agnostic (int vs float, fixmap vs map16), so the
// exact bytes need not match Node/Python — only the decoded values must. See PLAN §5.
func packMsgpack(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}
