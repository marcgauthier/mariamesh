package conflict

import (
	"testing"

	"github.com/mariamesh/mariamesh/internal/clock"
)

func TestHLCOrdering(t *testing.T) {
	old := Version{HLC: clock.HLC{Physical: 1}, OriginID: "b"}
	new := Version{HLC: clock.HLC{Physical: 2}, OriginID: "a"}
	if !Wins(new, old) {
		t.Fatalf("newer HLC must win")
	}
	if Wins(old, new) {
		t.Fatalf("older HLC must lose")
	}
}

func TestTieBreakDeterministic(t *testing.T) {
	a := Version{HLC: clock.HLC{Physical: 5, Logical: 1}, OriginID: "a", OriginSeq: 9}
	b := Version{HLC: clock.HLC{Physical: 5, Logical: 1}, OriginID: "b", OriginSeq: 1}
	if !Wins(b, a) || Wins(a, b) {
		t.Fatalf("origin tie-break must prefer higher node ID deterministically")
	}
	c := Version{HLC: clock.HLC{Physical: 5, Logical: 1}, OriginID: "b", OriginSeq: 2}
	if !Wins(c, b) {
		t.Fatalf("higher origin seq must win final tie-break")
	}
}

func TestEqualIsNoOp(t *testing.T) {
	v := Version{HLC: clock.HLC{Physical: 5}, OriginID: "a", OriginSeq: 1}
	if Wins(v, v) {
		t.Fatalf("equal versions must not win (idempotent redelivery)")
	}
}
