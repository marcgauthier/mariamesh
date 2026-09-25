package clock

import "testing"

func TestTickMonotonic(t *testing.T) {
	now := uint64(1000)
	c := NewWithNow(func() uint64 { return now })
	a := c.Tick()
	b := c.Tick() // same wall tick: logical must advance
	if !b.After(a) {
		t.Fatalf("tick not monotonic: %+v then %+v", a, b)
	}
	now = 2000
	d := c.Tick()
	if d.Physical != 2000 || d.Logical != 0 {
		t.Fatalf("wall advance not adopted: %+v", d)
	}
	if !d.After(b) {
		t.Fatalf("wall tick not monotonic")
	}
}

func TestUpdateAdvancesPastRemote(t *testing.T) {
	c := NewWithNow(func() uint64 { return 1000 })
	got := c.Update(HLC{Physical: 5000, Logical: 7})
	if got.Physical != 5000 || got.Logical != 8 {
		t.Fatalf("update = %+v, want {5000 8}", got)
	}
	next := c.Tick()
	if !next.After(got) {
		t.Fatalf("tick after update not monotonic: %+v", next)
	}
}

func TestUpdateWallWins(t *testing.T) {
	c := NewWithNow(func() uint64 { return 9000 })
	got := c.Update(HLC{Physical: 1000})
	if got.Physical != 9000 || got.Logical != 0 {
		t.Fatalf("update = %+v, want {9000 0}", got)
	}
}

func TestRestoreNeverGoesBack(t *testing.T) {
	c := NewWithNow(func() uint64 { return 100 })
	c.Tick()
	c.Restore(HLC{Physical: 1}) // older: ignored
	if got := c.Load(); got.Physical != 100 {
		t.Fatalf("restore went back: %+v", got)
	}
	c.Restore(HLC{Physical: 500, Logical: 3})
	if got := c.Load(); got != (HLC{Physical: 500, Logical: 3}) {
		t.Fatalf("restore ignored: %+v", got)
	}
}

func TestCompare(t *testing.T) {
	if (HLC{1, 2}).Compare(HLC{1, 2}) != 0 {
		t.Fatalf("equal HLCs compare nonzero")
	}
	if (HLC{1, 9}).Compare(HLC{2, 0}) != -1 {
		t.Fatalf("physical must dominate logical")
	}
}
