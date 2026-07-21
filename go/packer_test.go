package bullmq

import (
	"encoding/hex"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// JSON must be compact and must NOT HTML-escape <, >, & — otherwise stored job
// data diverges from Node/Python. See PLAN §5.
func TestMarshalJSON(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{map[string]any{"foo": "bar"}, `{"foo":"bar"}`},
		{map[string]string{"html": "<a>&</a>"}, `{"html":"<a>&</a>"}`},
		{map[string]int{"n": 3}, `{"n":3}`},
	}
	for _, c := range cases {
		got, err := marshalJSON(c.in)
		if err != nil {
			t.Errorf("marshalJSON(%v): %v", c.in, err)
			continue
		}
		if string(got) != c.want {
			t.Errorf("marshalJSON(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Long option keys are rewritten to their short storage form before packing.
// Mapping from python job.py optsDecodeMap (inverted).
func TestEncodeOpts(t *testing.T) {
	in := map[string]any{
		"deduplication":       map[string]any{"id": "x"},
		"attempts":            3,
		"failParentOnFailure": true,
		"keepLogs":            10,
	}
	got := encodeOpts(in)
	want := map[string]any{
		"de":       map[string]any{"id": "x"},
		"attempts": 3,
		"fpof":     true,
		"kl":       10,
	}
	if len(got) != len(want) {
		t.Fatalf("encodeOpts len = %d, want %d (%v)", len(got), len(want), got)
	}
	for k, v := range want {
		gv, ok := got[k]
		if !ok {
			t.Errorf("encodeOpts missing key %q", k)
			continue
		}
		// nested maps compared shallowly by presence; scalars by equality
		if _, isMap := v.(map[string]any); !isMap && gv != v {
			t.Errorf("encodeOpts[%q] = %v, want %v", k, gv, v)
		}
	}
}

// packMsgpack output must round-trip: encode a mixed 9-element args array, decode
// it back, and get the same positions/values/nils (order in an array is fixed).
func TestPackMsgpackRoundTrip(t *testing.T) {
	args := []any{"bull:q:", "job1", "test", int64(1700000000000), nil, nil, nil, nil, nil}
	b, err := packMsgpack(args)
	if err != nil {
		t.Fatalf("packMsgpack: %v", err)
	}
	var out []any
	if err := msgpack.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out) != 9 {
		t.Fatalf("decoded len = %d, want 9", len(out))
	}
	if out[0] != "bull:q:" || out[1] != "job1" || out[2] != "test" {
		t.Errorf("decoded strings wrong: %v", out[:3])
	}
	for i := 4; i < 9; i++ {
		if out[i] != nil {
			t.Errorf("position %d should be nil, got %v", i, out[i])
		}
	}
}

// Interop (Go reads Node's wire format): decode the exact bytes produced by
// bullmq's Packr({useRecords:false, encodeUndefinedAsNil:true}) and assert the
// structure. Golden hex generated from the real msgpackr encoder.
func TestDecodesNodeGoldenArgs(t *testing.T) {
	// pack(["bull:q:", "job1", "test", 1700000000000, null,null,null,null,null])
	const golden = "99a762756c6c3a713aa46a6f6231a474657374cb4278bcfe56800000c0c0c0c0c0"
	raw, err := hex.DecodeString(golden)
	if err != nil {
		t.Fatal(err)
	}
	var out []any
	if err := msgpack.Unmarshal(raw, &out); err != nil {
		t.Fatalf("cannot decode Node golden bytes: %v", err)
	}
	if len(out) != 9 {
		t.Fatalf("decoded len = %d, want 9", len(out))
	}
	if out[0] != "bull:q:" || out[1] != "job1" || out[2] != "test" {
		t.Errorf("strings: %v", out[:3])
	}
	// msgpackr encoded the timestamp as float64; decoding yields float64.
	if f, ok := out[3].(float64); !ok || f != 1700000000000 {
		t.Errorf("timestamp: got %v (%T), want 1700000000000", out[3], out[3])
	}
}
