package seed

import "testing"

func TestChecksumDeterministic(t *testing.T) {
	build := func() uint32 {
		c := NewChecksum()
		c.Vector([]string{"a:i", "b:i"}, func(k string) uint64 {
			if k == "a:i" {
				return 10
			}
			return 3
		})
		c.Table("device")
		c.Row("r1", map[string]any{"name": "X", "enabled": true})
		c.Row("r2", map[string]any{"name": nil})
		c.Version("r1", "name", 100, 0, "a", 9)
		c.Tombstone("r9", 50, 1, "b", 2)
		return c.Sum()
	}
	if build() != build() {
		t.Fatalf("checksum not deterministic")
	}
}

func TestChecksumDetectsChange(t *testing.T) {
	a := NewChecksum()
	a.Table("device")
	a.Row("r1", map[string]any{"name": "X"})
	b := NewChecksum()
	b.Table("device")
	b.Row("r1", map[string]any{"name": "Y"})
	if a.Sum() == b.Sum() {
		t.Fatalf("checksum missed a value change")
	}
	c := NewChecksum()
	c.Table("device")
	c.Row("r1", map[string]any{"name": "Y", "extra": 1})
	if b.Sum() == c.Sum() {
		t.Fatalf("checksum missed a column change")
	}
}

func TestBinaryRoundTrip(t *testing.T) {
	raw := []byte{0x00, 0xFF, 0x01}
	enc := encodeValue(raw, true)
	m, ok := enc.(BinaryMarker)
	if !ok {
		t.Fatalf("binary not wrapped: %T", enc)
	}
	_ = m
	// Through JSON the marker becomes a generic map.
	dec, err := decodeValue(map[string]any{"$bytes": "AP8B"})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := dec.([]byte)
	if !ok || len(got) != 3 || got[1] != 0xFF {
		t.Fatalf("binary round trip = %v", dec)
	}
	if s := encodeValue([]byte("text"), false); s != "text" {
		t.Fatalf("text bytes not decoded as string: %v", s)
	}
}
