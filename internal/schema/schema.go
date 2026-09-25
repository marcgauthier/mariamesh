// Package schema generates the replication metadata DDL and per-table
// trigger SQL. The host application installs the output through its own
// migrations; this package never executes DDL itself.
package schema

import (
	"fmt"
	"strings"
)

// TableSpec is the trigger generator's view of a replicated table.
type TableSpec struct {
	// Name is the SQL table name.
	Name string
	// IDColumn is the immutable primary key column.
	IDColumn string
	// Columns are the tracked value columns, in trigger order.
	Columns []string
}

// MetadataSchemaSQL returns CREATE TABLE IF NOT EXISTS statements for every
// replication metadata table. It is idempotent and safe to include in
// application migrations.
func MetadataSchemaSQL() string {
	return strings.Join([]string{
		tableReplicationNode,
		tableReplicationLocalState,
		tableReplicationLog,
		tableReplicationFieldVersion,
		tableReplicationTombstone,
		tableReplicationProgress,
		tableReplicationBootstrap,
		tableReplicationBootstrapVector,
		tableReplicationMeta,
	}, "\n\n") + "\n"
}

const tableReplicationNode = `-- Replication cluster membership. Written by AddNode/RetireNode.
CREATE TABLE IF NOT EXISTS ` + "`replication_node`" + ` (
  ` + "`node_id`" + ` CHAR(36) NOT NULL PRIMARY KEY,
  ` + "`incarnation_id`" + ` CHAR(36) NOT NULL,
  ` + "`name`" + ` VARCHAR(255) NOT NULL DEFAULT '',
  ` + "`status`" + ` VARCHAR(16) NOT NULL DEFAULT 'JOINING',
  ` + "`membership_epoch`" + ` BIGINT NOT NULL DEFAULT 0,
  ` + "`schema_version`" + ` BIGINT NOT NULL DEFAULT 0,
  ` + "`addr`" + ` VARCHAR(255) NOT NULL DEFAULT '',
  ` + "`joined_at`" + ` TIMESTAMP(6) NULL DEFAULT NULL,
  ` + "`retired_at`" + ` TIMESTAMP(6) NULL DEFAULT NULL,
  ` + "`last_seen`" + ` TIMESTAMP(6) NULL DEFAULT NULL,
  ` + "`updated_at`" + ` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  KEY ` + "`idx_replication_node_status`" + ` (` + "`status`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationLocalState = `-- Singleton row for the local node: sequence counter + HLC state.
-- replication_local_state must contain exactly one row, maintained by the
-- replication package (DML only, never DDL).
CREATE TABLE IF NOT EXISTS ` + "`replication_local_state`" + ` (
  ` + "`node_id`" + ` CHAR(36) NOT NULL PRIMARY KEY,
  ` + "`incarnation_id`" + ` CHAR(36) NOT NULL,
  ` + "`current_seq`" + ` BIGINT NOT NULL DEFAULT 0,
  ` + "`hlc_physical`" + ` BIGINT NOT NULL DEFAULT 0,
  ` + "`hlc_logical`" + ` BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationLog = `-- Change log: local events (written by triggers) plus received events
-- (written by the apply path for store-and-forward). Identity is
-- (origin_node_id, origin_incarnation_id, origin_seq), which makes
-- re-delivery a no-op. HLC units are milliseconds.
CREATE TABLE IF NOT EXISTS ` + "`replication_log`" + ` (
  ` + "`origin_node_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_incarnation_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_seq`" + ` BIGINT NOT NULL,
  ` + "`change_id`" + ` CHAR(36) NOT NULL,
  ` + "`table_name`" + ` VARCHAR(128) NOT NULL,
  ` + "`row_id`" + ` VARCHAR(36) NOT NULL,
  ` + "`operation`" + ` VARCHAR(16) NOT NULL,
  ` + "`hlc_physical`" + ` BIGINT NOT NULL,
  ` + "`hlc_logical`" + ` BIGINT NOT NULL,
  ` + "`payload`" + ` LONGTEXT NULL DEFAULT NULL COMMENT 'JSON object of changed columns',
  ` + "`received_at`" + ` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (` + "`origin_node_id`" + `, ` + "`origin_incarnation_id`" + `, ` + "`origin_seq`" + `),
  KEY ` + "`idx_replication_log_row`" + ` (` + "`table_name`" + `, ` + "`row_id`" + `),
  KEY ` + "`idx_replication_log_hlc`" + ` (` + "`hlc_physical`" + `, ` + "`hlc_logical`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationFieldVersion = `-- Per-column last-write-wins versions.
CREATE TABLE IF NOT EXISTS ` + "`replication_field_version`" + ` (
  ` + "`table_name`" + ` VARCHAR(128) NOT NULL,
  ` + "`row_id`" + ` VARCHAR(36) NOT NULL,
  ` + "`column_name`" + ` VARCHAR(128) NOT NULL,
  ` + "`hlc_physical`" + ` BIGINT NOT NULL,
  ` + "`hlc_logical`" + ` BIGINT NOT NULL,
  ` + "`origin_node_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_seq`" + ` BIGINT NOT NULL,
  PRIMARY KEY (` + "`table_name`" + `, ` + "`row_id`" + `, ` + "`column_name`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationTombstone = `-- Delete markers. A tombstone wins over any older field write, so a
-- long-offline node cannot resurrect deleted rows.
CREATE TABLE IF NOT EXISTS ` + "`replication_tombstone`" + ` (
  ` + "`table_name`" + ` VARCHAR(128) NOT NULL,
  ` + "`row_id`" + ` VARCHAR(36) NOT NULL,
  ` + "`hlc_physical`" + ` BIGINT NOT NULL,
  ` + "`hlc_logical`" + ` BIGINT NOT NULL,
  ` + "`origin_node_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_seq`" + ` BIGINT NOT NULL,
  ` + "`deleted_at`" + ` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (` + "`table_name`" + `, ` + "`row_id`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationProgress = `-- Per-receiver, per-origin contiguous-sequence cursors. The local node's
-- own rows are the advertised replication vector; rows learned from peers
-- drive garbage collection.
CREATE TABLE IF NOT EXISTS ` + "`replication_progress`" + ` (
  ` + "`receiver_node_id`" + ` CHAR(36) NOT NULL,
  ` + "`receiver_incarnation_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_node_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_incarnation_id`" + ` CHAR(36) NOT NULL,
  ` + "`contiguous_seq`" + ` BIGINT NOT NULL DEFAULT 0,
  ` + "`updated_at`" + ` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (` + "`receiver_node_id`" + `, ` + "`receiver_incarnation_id`" + `, ` + "`origin_node_id`" + `, ` + "`origin_incarnation_id`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationBootstrap = `-- Seed sessions for joining nodes.
CREATE TABLE IF NOT EXISTS ` + "`replication_bootstrap`" + ` (
  ` + "`bootstrap_id`" + ` CHAR(36) NOT NULL PRIMARY KEY,
  ` + "`joining_node_id`" + ` CHAR(36) NOT NULL,
  ` + "`joining_incarnation_id`" + ` CHAR(36) NOT NULL,
  ` + "`status`" + ` VARCHAR(16) NOT NULL DEFAULT 'STARTED',
  ` + "`schema_version`" + ` BIGINT NOT NULL DEFAULT 0,
  ` + "`started_at`" + ` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  ` + "`completed_at`" + ` TIMESTAMP(6) NULL DEFAULT NULL,
  KEY ` + "`idx_replication_bootstrap_joining`" + ` (` + "`joining_node_id`" + `, ` + "`joining_incarnation_id`" + `, ` + "`status`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationBootstrapVector = `-- Seed snapshot vectors. Active rows pin GC: the joining node is
-- treated as if it acknowledged exactly these sequences.
CREATE TABLE IF NOT EXISTS ` + "`replication_bootstrap_vector`" + ` (
  ` + "`bootstrap_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_node_id`" + ` CHAR(36) NOT NULL,
  ` + "`origin_incarnation_id`" + ` CHAR(36) NOT NULL,
  ` + "`seed_seq`" + ` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (` + "`bootstrap_id`" + `, ` + "`origin_node_id`" + `, ` + "`origin_incarnation_id`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

const tableReplicationMeta = `-- Singleton key/value metadata (membership epoch, seed state, ...).
CREATE TABLE IF NOT EXISTS ` + "`replication_meta`" + ` (
  ` + "`meta_key`" + ` VARCHAR(128) NOT NULL PRIMARY KEY,
  ` + "`meta_value`" + ` LONGTEXT NOT NULL,
  ` + "`updated_at`" + ` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// MetadataTables lists the replication metadata tables in dependency order.
var MetadataTables = []string{
	"replication_node",
	"replication_local_state",
	"replication_log",
	"replication_field_version",
	"replication_tombstone",
	"replication_progress",
	"replication_bootstrap",
	"replication_bootstrap_vector",
	"replication_meta",
}

// RequiredColumns maps each metadata table to the columns validation expects.
var RequiredColumns = map[string][]string{
	"replication_node":            {"node_id", "incarnation_id", "name", "status", "membership_epoch", "schema_version", "addr", "joined_at", "retired_at", "last_seen"},
	"replication_local_state":     {"node_id", "incarnation_id", "current_seq", "hlc_physical", "hlc_logical"},
	"replication_log":             {"origin_node_id", "origin_incarnation_id", "origin_seq", "change_id", "table_name", "row_id", "operation", "hlc_physical", "hlc_logical", "payload"},
	"replication_field_version":   {"table_name", "row_id", "column_name", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"},
	"replication_tombstone":       {"table_name", "row_id", "hlc_physical", "hlc_logical", "origin_node_id", "origin_seq"},
	"replication_progress":        {"receiver_node_id", "receiver_incarnation_id", "origin_node_id", "origin_incarnation_id", "contiguous_seq"},
	"replication_bootstrap":       {"bootstrap_id", "joining_node_id", "joining_incarnation_id", "status", "schema_version", "started_at", "completed_at"},
	"replication_bootstrap_vector": {"bootstrap_id", "origin_node_id", "origin_incarnation_id", "seed_seq"},
	"replication_meta":            {"meta_key", "meta_value"},
}

// TriggerNames returns the three trigger names for a table.
func TriggerNames(table string) (insert, update, delete string) {
	return table + "_repl_insert", table + "_repl_update", table + "_repl_delete"
}

// quote wraps an identifier in backticks, doubling embedded backticks.
func quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// qstr quotes a string literal for SQL.
func qstr(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// TriggerSQL generates the INSERT/UPDATE/DELETE change-tracking triggers for
// one table in MariaDB dialect. The application installs the result in a
// migration; statements are separated for execution one at a time (no
// DELIMITER commands are emitted because database/sql cannot parse them).
//
// Every trigger:
//   - returns immediately when the connection-local @replication_apply = 1
//     (set by the apply path so replicated writes never re-enter the log);
//   - allocates (current_seq, HLC) from replication_local_state inside the
//     same transaction as the row change, so row + log commit atomically;
//   - writes one replication_log row under the local origin.
//
// The UPDATE trigger additionally rejects ID changes (SIGNAL 45000) and
// records only columns whose value actually changed, using the NULL-safe
// <=> comparison so NULL<->value transitions are detected.
func TriggerSQL(spec TableSpec) (string, error) {
	if strings.TrimSpace(spec.Name) == "" {
		return "", fmt.Errorf("schema: table name is required")
	}
	if strings.TrimSpace(spec.IDColumn) == "" {
		return "", fmt.Errorf("schema: table %q: id column is required", spec.Name)
	}
	if len(spec.Columns) == 0 {
		return "", fmt.Errorf("schema: table %q: at least one tracked column is required", spec.Name)
	}
	ins, upd, del := TriggerNames(spec.Name)
	var b strings.Builder
	b.WriteString(insertTrigger(spec, ins))
	b.WriteString("\n\n")
	b.WriteString(updateTrigger(spec, upd))
	b.WriteString("\n\n")
	b.WriteString(deleteTrigger(spec, del))
	b.WriteString("\n")
	return b.String(), nil
}

// DropTriggerSQL returns DROP TRIGGER IF EXISTS statements for a table.
func DropTriggerSQL(table string) string {
	ins, upd, del := TriggerNames(table)
	return fmt.Sprintf("DROP TRIGGER IF EXISTS %s;\nDROP TRIGGER IF EXISTS %s;\nDROP TRIGGER IF EXISTS %s;\n",
		quote(ins), quote(upd), quote(del))
}

// allocSequenceSQL emits the trigger body fragment that allocates the next
// (seq, HLC) into @repl_seq/@repl_hp/@repl_hl and the local origin into
// @repl_node/@repl_inc. HLC physical units are milliseconds.
const allocSequenceSQL = `        SET @repl_now = CAST(UNIX_TIMESTAMP(NOW(6)) * 1000 AS UNSIGNED);
        SELECT %[1]s, %[2]s INTO @repl_op, @repl_ol FROM %[3]s LIMIT 1;
        SET @repl_np = GREATEST(@repl_op, @repl_now);
        SET @repl_nl = IF(@repl_op >= @repl_now, @repl_ol + 1, 0);
        UPDATE %[3]s SET %[4]s = %[4]s + 1, %[1]s = @repl_np, %[2]s = @repl_nl;
        SELECT %[5]s, %[6]s, %[4]s INTO @repl_node, @repl_inc, @repl_seq FROM %[3]s LIMIT 1;`

func allocSQL() string {
	return fmt.Sprintf(allocSequenceSQL,
		quote("hlc_physical"), quote("hlc_logical"), quote("replication_local_state"),
		quote("current_seq"), quote("node_id"), quote("incarnation_id"))
}

func insertTrigger(spec TableSpec, name string) string {
	pairs := make([]string, 0, len(spec.Columns))
	for _, c := range spec.Columns {
		pairs = append(pairs, fmt.Sprintf("%s, NEW.%s", qstr(c), quote(c)))
	}
	return fmt.Sprintf(`CREATE TRIGGER %s AFTER INSERT ON %s FOR EACH ROW
BEGIN
    IF COALESCE(@replication_apply, 0) != 1 THEN
%s
        INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        VALUES (@repl_node, @repl_inc, @repl_seq, UUID(), %s, NEW.%s, 'INSERT',
            @repl_np, @repl_nl,
            JSON_OBJECT(%s));
    END IF;
END;`,
		quote(name), quote(spec.Name),
		allocSQL(),
		quote("replication_log"),
		quote("origin_node_id"), quote("origin_incarnation_id"), quote("origin_seq"),
		quote("change_id"), quote("table_name"), quote("row_id"), quote("operation"),
		quote("hlc_physical"), quote("hlc_logical"), quote("payload"),
		qstr(spec.Name), quote(spec.IDColumn),
		strings.Join(pairs, ", "))
}

func updateTrigger(spec TableSpec, name string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`CREATE TRIGGER %s AFTER UPDATE ON %s FOR EACH ROW
BEGIN
    IF COALESCE(@replication_apply, 0) != 1 THEN
        IF NOT (OLD.%s <=> NEW.%s) THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'replication: id column is immutable';
        END IF;
        SET @repl_payload = JSON_OBJECT();
        SET @repl_changed = 0;
`,
		quote(name), quote(spec.Name), quote(spec.IDColumn), quote(spec.IDColumn)))
	for _, c := range spec.Columns {
		fmt.Fprintf(&b, `        IF NOT (OLD.%[1]s <=> NEW.%[1]s) THEN
            SET @repl_payload = JSON_INSERT(@repl_payload, %[2]s, NEW.%[1]s);
            SET @repl_changed = 1;
        END IF;
`, quote(c), qstr("$."+c))
	}
	fmt.Fprintf(&b, `        IF @repl_changed = 1 THEN
%s
            INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            VALUES (@repl_node, @repl_inc, @repl_seq, UUID(), %s, NEW.%s, 'UPDATE',
                @repl_np, @repl_nl, @repl_payload);
        END IF;
    END IF;
END;`,
		indent(allocSQL(), "    "),
		quote("replication_log"),
		quote("origin_node_id"), quote("origin_incarnation_id"), quote("origin_seq"),
		quote("change_id"), quote("table_name"), quote("row_id"), quote("operation"),
		quote("hlc_physical"), quote("hlc_logical"), quote("payload"),
		qstr(spec.Name), quote(spec.IDColumn))
	return b.String()
}

func deleteTrigger(spec TableSpec, name string) string {
	return fmt.Sprintf(`CREATE TRIGGER %s AFTER DELETE ON %s FOR EACH ROW
BEGIN
    IF COALESCE(@replication_apply, 0) != 1 THEN
%s
        INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        VALUES (@repl_node, @repl_inc, @repl_seq, UUID(), %s, OLD.%s, 'DELETE',
            @repl_np, @repl_nl, NULL);
    END IF;
END;`,
		quote(name), quote(spec.Name),
		allocSQL(),
		quote("replication_log"),
		quote("origin_node_id"), quote("origin_incarnation_id"), quote("origin_seq"),
		quote("change_id"), quote("table_name"), quote("row_id"), quote("operation"),
		quote("hlc_physical"), quote("hlc_logical"), quote("payload"),
		qstr(spec.Name), quote(spec.IDColumn))
}

func indent(s, prefix string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n")
}
