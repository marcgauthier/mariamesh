package replication

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestIDDeterministic(t *testing.T) {
	ns := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	a := ID(ns, "device", "Router-001")
	b := ID(ns, "device", "Router-001")
	if a != b {
		t.Fatalf("ID not deterministic: %s vs %s", a, b)
	}
	if v := a.Version(); v != 5 {
		t.Fatalf("ID version = %d, want 5", v)
	}
	// Case/space normalization converges.
	c := ID(ns, "  Device ", "ROUTER-001")
	if a != c {
		t.Fatalf("normalization diverged: %s vs %s", a, c)
	}
	// Different table or name diverges.
	if a == ID(ns, "network", "Router-001") {
		t.Fatalf("table not mixed into ID")
	}
	if a == ID(ns, "device", "Router-002") {
		t.Fatalf("name not mixed into ID")
	}
	if got := UUID(ns, "device", "Router-001"); got != a {
		t.Fatalf("UUID alias diverged: %s vs %s", got, a)
	}
}

func TestIDMatchesSpecVector(t *testing.T) {
	ns := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	// Spec: UUIDv5(namespace, "device:router-001").
	want := uuid.NewSHA1(ns, []byte("device:router-001"))
	if got := ID(ns, "device", "Router-001"); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestValidateID(t *testing.T) {
	ns := uuid.New()
	id := ID(ns, "device", "Router-001").String()
	if err := ValidateID(ns, "device", "Router-001", id); err != nil {
		t.Fatalf("valid ID rejected: %v", err)
	}
	if err := ValidateID(ns, "device", "Router-001", uuid.NewString()); err == nil {
		t.Fatalf("fabricated ID accepted")
	}
}

func TestTableValidate(t *testing.T) {
	ok := Table{Name: "device", Columns: []Column{{Name: "location"}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid table rejected: %v", err)
	}
	bad := []Table{
		{Name: ""},
		{Name: "dev`ice"},
		{Name: "device", IDColumn: "id", NameColumn: "id"},
		{Name: "device", Columns: []Column{{Name: ""}}},
	}
	for i, tb := range bad {
		if err := tb.Validate(); err == nil {
			t.Fatalf("bad table %d accepted", i)
		}
	}
}

func TestReplicatedColumns(t *testing.T) {
	tb := Table{Name: "device", Columns: []Column{{Name: "location"}, {Name: "enabled"}, {Name: "id"}}}
	got := tb.ReplicatedColumns()
	want := []string{"name", "enabled", "location"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	// Empty columns track only the name column; id is never tracked.
	if got := (Table{Name: "device"}).ReplicatedColumns(); len(got) != 1 || got[0] != "name" {
		t.Fatalf("default columns = %v", got)
	}
}

func TestRegistryDuplicates(t *testing.T) {
	var r registry
	if err := r.register(Table{Name: "device"}); err != nil {
		t.Fatal(err)
	}
	if err := r.register(Table{Name: "Device"}); err == nil {
		t.Fatalf("case-insensitive duplicate accepted")
	}
	if _, err := r.lookup("DEVICE"); err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if _, err := r.lookup("nope"); err == nil {
		t.Fatalf("unknown table lookup succeeded")
	}
}

func TestConfigValidate(t *testing.T) {
	var c Config
	if err := c.validate(); err == nil {
		t.Fatalf("empty config accepted")
	}
}

func TestTriggerSQLFragments(t *testing.T) {
	sql, err := TriggerSQL(Table{Name: "device", Columns: []Column{{Name: "location"}, {Name: "enabled"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{
		"device_repl_insert", "device_repl_update", "device_repl_delete",
		"@replication_apply", "<=>", "SIGNAL SQLSTATE '45000'",
		"JSON_OBJECT", "JSON_INSERT", "current_seq", "replication_log",
		"hlc_physical", "hlc_logical",
	} {
		if !strings.Contains(sql, frag) {
			t.Fatalf("trigger SQL missing %q", frag)
		}
	}
}

func TestMetadataSchemaSQLTables(t *testing.T) {
	sql := MetadataSchemaSQL()
	for _, tbl := range []string{
		"replication_node", "replication_local_state", "replication_log",
		"replication_field_version", "replication_tombstone", "replication_progress",
		"replication_bootstrap", "replication_bootstrap_vector", "replication_meta",
	} {
		if !strings.Contains(sql, "`"+tbl+"`") {
			t.Fatalf("metadata SQL missing table %s", tbl)
		}
	}
}
