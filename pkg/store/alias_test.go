package store

import "testing"

// Checks that EncodeAlias writes a 33-byte body (version 0x01 plus the 32-byte
// delta key) and DecodeAlias returns that same key.
func TestAliasBodyRoundTrip(t *testing.T) {
	var k ContentKey
	for i := range k {
		k[i] = byte(i + 1)
	}
	body := EncodeAlias(k)
	if len(body) != AliasBodyLen || body[0] != 0x01 {
		t.Fatalf("body = %x", body)
	}
	got, err := DecodeAlias(body)
	if err != nil || got != k {
		t.Fatalf("DecodeAlias = %x, %v", got, err)
	}
}

// Checks that DecodeAlias rejects a body that is empty, one byte short, one byte
// long, or carries a version other than 0x01.
func TestAliasBodyRejectsMalformed(t *testing.T) {
	good := EncodeAlias(ContentKey{1})
	badVersion := append([]byte(nil), good...)
	badVersion[0] = 0x02
	for name, body := range map[string][]byte{
		"empty":       nil,
		"short":       good[:len(good)-1],
		"long":        append(append([]byte(nil), good...), 0),
		"bad version": badVersion,
	} {
		if _, err := DecodeAlias(body); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
