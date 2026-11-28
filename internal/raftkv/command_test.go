package raftkv

import (
	"bytes"
	"encoding/json"
	"testing"
)

// Entries exactly as v1.0 wrote them: json.Marshal of a map[string]string,
// so keys are sorted and k/v are plain strings.
func TestDecodeV1Commands(t *testing.T) {
	cases := []struct {
		raw     string
		op      string
		key     string
		val     string
		comment string
	}{
		{`{"k":"k0","op":"put","v":"v0"}`, "put", "k0", "v0", "not valid base64"},
		{`{"k":"user","op":"put","v":"abcd"}`, "put", "user", "abcd", "valid base64: must not be decoded as such"},
		{`{"k":"user","op":"del"}`, "del", "user", "", "delete has no v"},
		{`{"k":"e","op":"put","v":""}`, "put", "e", "", "empty value"},
		{`{"k":"a<b","op":"put","v":"x\r\ny"}`, "put", "a<b", "x\r\ny", "escaped characters"},
	}
	for _, c := range cases {
		cmd, err := decodeCommand([]byte(c.raw))
		if err != nil {
			t.Fatalf("%s (%s): %v", c.raw, c.comment, err)
		}
		if cmd.Op != c.op || string(cmd.Key) != c.key || string(cmd.Val) != c.val {
			t.Errorf("%s (%s): got op=%q k=%q v=%q", c.raw, c.comment, cmd.Op, cmd.Key, cmd.Val)
		}
	}

	// Check the fixture really is what v1.0's encoder produces.
	got, _ := json.Marshal(map[string]string{"op": "put", "k": "user", "v": "abcd"})
	if string(got) != cases[1].raw {
		t.Fatalf("v1.0 encoding is %s, fixture says %s", got, cases[1].raw)
	}
}

// The format detection relies on the current encoding starting with "op".
func TestCurrentCommandsRoundTripAndAreNotMistakenForV1(t *testing.T) {
	for _, c := range []command{
		{Op: "put", Key: []byte("user"), Val: []byte("abcd")},
		{Op: "put", Key: []byte{0, 0xff, '\r', '\n'}, Val: []byte{}},
		{Op: "del", Key: []byte("k")},
	} {
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(data, []byte(`{"op":`)) {
			t.Fatalf("current encoding %s no longer starts with op; decodeCommand's v1.0 detection depends on it", data)
		}
		got, err := decodeCommand(data)
		if err != nil {
			t.Fatal(err)
		}
		if got.Op != c.Op || !bytes.Equal(got.Key, c.Key) || !bytes.Equal(got.Val, c.Val) {
			t.Errorf("round trip of %+v gave %+v", c, got)
		}
	}
}
