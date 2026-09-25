package changelog

import (
	"testing"

	"github.com/mariamesh/mariamesh/internal/clock"
)

func TestOriginKeyRoundTrip(t *testing.T) {
	o := Origin{NodeID: "n1", IncarnationID: "i1"}
	if o.Key() != "n1:i1" {
		t.Fatalf("key = %q", o.Key())
	}
	back, err := ParseKey(o.Key())
	if err != nil || back != o {
		t.Fatalf("round trip failed: %v %+v", err, back)
	}
	if _, err := ParseKey("bogus"); err == nil {
		t.Fatalf("bad key accepted")
	}
}

func TestMissingRanges(t *testing.T) {
	a := Origin{NodeID: "a", IncarnationID: "i"}
	b := Origin{NodeID: "b", IncarnationID: "i"}
	have := Vector{a.Key(): 5000, b.Key(): 2400}
	want := Vector{a.Key(): 5200, b.Key(): 2400, "c:i": 355}
	missing := Missing(have, want)
	if len(missing) != 2 {
		t.Fatalf("missing = %+v, want 2 ranges", missing)
	}
	// Sorted by origin key: a:i then c:i.
	if missing[0].From != 5001 || missing[0].To != 5200 {
		t.Fatalf("range a = %+v", missing[0])
	}
	if missing[1].From != 1 || missing[1].To != 355 {
		t.Fatalf("range c = %+v", missing[1])
	}
}

func TestMinVectors(t *testing.T) {
	a, b := "a:i", "b:i"
	m := Min(Vector{a: 10, b: 5}, Vector{a: 7, b: 9}, Vector{a: 8})
	if m[a] != 7 || m[b] != 0 {
		t.Fatalf("min = %v, want a=7 b=0 (absent counts as 0)", m)
	}
}

func TestVectorSetForwardOnly(t *testing.T) {
	v := Vector{}
	o := Origin{NodeID: "a", IncarnationID: "i"}
	v.Set(o, 5)
	v.Set(o, 3)
	if v.Get(o) != 5 {
		t.Fatalf("vector moved backward: %v", v)
	}
}

func TestEventValidate(t *testing.T) {
	e := Event{
		Origin: Origin{NodeID: "n", IncarnationID: "i"}, OriginSeq: 1,
		Table: "device", RowID: "r1", Op: OpUpdate,
		HLC: clock.HLC{Physical: 1}, Payload: map[string]any{"name": "x"},
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	bad := e
	bad.Op = "UPSERT"
	if err := bad.Validate(); err == nil {
		t.Fatalf("bad op accepted")
	}
	bad = e
	bad.Op = OpDelete
	if err := bad.Validate(); err == nil {
		t.Fatalf("delete with payload accepted")
	}
	bad = e
	bad.OriginSeq = 0
	if err := bad.Validate(); err == nil {
		t.Fatalf("zero seq accepted")
	}
}

func TestPayloadRoundTrip(t *testing.T) {
	e := Event{Payload: map[string]any{"name": "R1", "enabled": true, "n": nil}}
	s, err := e.PayloadJSON()
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParsePayload(s)
	if err != nil {
		t.Fatal(err)
	}
	if back["name"] != "R1" || back["enabled"] != true {
		t.Fatalf("round trip = %v", back)
	}
	if _, ok := back["n"]; !ok {
		t.Fatalf("null value lost: %v", back)
	}
	if m, err := ParsePayload(""); err != nil || m != nil {
		t.Fatalf("empty payload = %v, %v", m, err)
	}
}
