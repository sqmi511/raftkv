package store

import (
	"bytes"
	"errors"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Op: OpSet, Key: "k", Value: []byte("v"), ExpiresAt: 123456789},
		{Op: OpSet, Key: "no-ttl", Value: []byte("v")},
		{Op: OpSet, Key: "empty-value"},
		{Op: OpSet, Key: "binary", Value: []byte{0, 1, 2, 0xFF, 0}},
		{Op: OpSet, Key: "unicode-ключ-密钥", Value: []byte("значение")},
		{Op: OpSet, Key: string(bytes.Repeat([]byte("k"), 300)), Value: []byte("long key needs a 2-byte varint")},
		{Op: OpDelete, Key: "gone"},
		{Op: OpSet, Key: "", Value: []byte("empty key still round-trips")},
	}
	for _, want := range cases {
		got, err := DecodeCommand(want.Encode())
		if err != nil {
			t.Fatalf("DecodeCommand(%+v): %v", want, err)
		}
		if got.Op != want.Op || got.Key != want.Key || got.ExpiresAt != want.ExpiresAt ||
			!bytes.Equal(got.Value, want.Value) {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
		}
	}
}

func TestDecodeDoesNotAliasInput(t *testing.T) {
	enc := Command{Op: OpSet, Key: "k", Value: []byte("value")}.Encode()
	c, err := DecodeCommand(enc)
	if err != nil {
		t.Fatal(err)
	}
	for i := range enc {
		enc[i] = 0
	}
	if string(c.Value) != "value" || c.Key != "k" {
		t.Fatalf("decoded command changed when input was mutated: %+v", c)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	valid := Command{Op: OpSet, Key: "key", Value: []byte("v")}.Encode()

	badOp := append([]byte(nil), valid...)
	badOp[0] = 99

	zeroOp := append([]byte(nil), valid...)
	zeroOp[0] = 0

	// uvarint that never terminates (all continuation bits set)
	badVarint := append([]byte{byte(OpSet), 0, 0, 0, 0, 0, 0, 0, 0},
		bytes.Repeat([]byte{0xFF}, 11)...)

	// declares a 100-byte key but carries none
	keyTooLong := append([]byte{byte(OpSet), 0, 0, 0, 0, 0, 0, 0, 0}, 100)

	cases := map[string][]byte{
		"nil":            nil,
		"too short":      valid[:5],
		"unknown op":     badOp,
		"zero op":        zeroOp,
		"bad varint":     badVarint,
		"key too long":   keyTooLong,
		"just the op":    {byte(OpSet)},
		"header minus 1": valid[:minCommandSize-1],
	}
	for name, b := range cases {
		if _, err := DecodeCommand(b); !errors.Is(err, ErrBadCommand) {
			t.Errorf("%s: err = %v, want ErrBadCommand", name, err)
		}
	}
}
