package schema

import (
	"strings"
	"testing"
)

func TestTriggerSQLStructure(t *testing.T) {
	sql, err := TriggerSQL(TableSpec{Name: "device", IDColumn: "id", Columns: []string{"name", "location"}})
	if err != nil {
		t.Fatal(err)
	}
	// Three triggers, each guarded by the apply flag.
	for _, name := range []string{"device_repl_insert", "device_repl_update", "device_repl_delete"} {
		if !strings.Contains(sql, "CREATE TRIGGER `"+name+"`") {
			t.Fatalf("missing trigger %s", name)
		}
	}
	if got := strings.Count(sql, "COALESCE(@replication_apply, 0) != 1"); got != 3 {
		t.Fatalf("apply guard count = %d, want 3", got)
	}
	// Update trigger detects NULL-safe changes per column and rejects ID writes.
	if !strings.Contains(sql, "OLD.`location` <=> NEW.`location`") {
		t.Fatalf("update trigger lacks NULL-safe comparison")
	}
	if !strings.Contains(sql, "SIGNAL SQLSTATE '45000'") {
		t.Fatalf("update trigger lacks immutability guard")
	}
	// Only changed columns land in the payload.
	if !strings.Contains(sql, "JSON_INSERT(@repl_payload, '$.location', NEW.`location`)") {
		t.Fatalf("update trigger lacks incremental payload build")
	}
	if strings.Contains(sql, "JSON_INSERT(@repl_payload, '$.id'") {
		t.Fatalf("id column must never enter the payload")
	}
	// Insert trigger records the full row.
	if !strings.Contains(sql, "JSON_OBJECT('name', NEW.`name`, 'location', NEW.`location`)") {
		t.Fatalf("insert trigger lacks full-row payload")
	}
	// Sequence + HLC allocated inside the row's transaction.
	if got := strings.Count(sql, "UPDATE `replication_local_state`"); got != 3 {
		t.Fatalf("sequence allocation count = %d, want 3", got)
	}
}

func TestTriggerSQLRejectsEmpty(t *testing.T) {
	if _, err := TriggerSQL(TableSpec{}); err == nil {
		t.Fatalf("empty spec accepted")
	}
	if _, err := TriggerSQL(TableSpec{Name: "t", IDColumn: "id"}); err == nil {
		t.Fatalf("column-less spec accepted")
	}
}

func TestDropTriggerSQL(t *testing.T) {
	sql := DropTriggerSQL("device")
	for _, name := range []string{"device_repl_insert", "device_repl_update", "device_repl_delete"} {
		if !strings.Contains(sql, "DROP TRIGGER IF EXISTS `"+name+"`") {
			t.Fatalf("drop SQL missing %s", name)
		}
	}
}

func TestMetadataSchemaIdempotent(t *testing.T) {
	sql := MetadataSchemaSQL()
	if got := strings.Count(sql, "CREATE TABLE IF NOT EXISTS"); got != len(MetadataTables) {
		t.Fatalf("CREATE count = %d, want %d", got, len(MetadataTables))
	}
	for _, tbl := range MetadataTables {
		if !strings.Contains(sql, "`"+tbl+"`") {
			t.Fatalf("missing table %s", tbl)
		}
		if len(RequiredColumns[tbl]) == 0 {
			t.Fatalf("no required columns for %s", tbl)
		}
	}
}
