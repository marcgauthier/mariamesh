package replication

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/mariamesh/mariamesh/internal/schema"
)

// MetadataSchemaSQL returns idempotent CREATE TABLE IF NOT EXISTS statements
// for the replication metadata tables. The application includes the output
// in its normal migrations; the package itself never executes DDL.
func MetadataSchemaSQL() string {
	return schema.MetadataSchemaSQL()
}

// TriggerSQL generates the change-tracking triggers (insert/update/delete)
// for a registered-style table declaration. The application installs the
// result through its migrations, executing one CREATE TRIGGER statement at
// a time.
func TriggerSQL(t Table) (string, error) {
	if err := t.Validate(); err != nil {
		return "", err
	}
	return schema.TriggerSQL(schema.TableSpec{
		Name:     t.Name,
		IDColumn: t.idColumn(),
		Columns:  t.ReplicatedColumns(),
	})
}

// DropTriggerSQL returns DROP TRIGGER IF EXISTS statements for a table,
// for use in application down-migrations.
func DropTriggerSQL(tableName string) string {
	return schema.DropTriggerSQL(tableName)
}

// ValidateSchema performs read-only validation that the database contains
// the replication metadata tables with the expected columns, exactly one
// replication_local_state row, and — for each given table — the business
// table with its id/name columns plus all three generated triggers.
// It never creates or alters anything.
func ValidateSchema(ctx context.Context, db *sql.DB, tables []Table) error {
	var problems []string
	fail := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	var dbName string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err != nil {
		return fmt.Errorf("%w: cannot determine current database: %v", ErrValidation, err)
	}

	for _, t := range schema.MetadataTables {
		var n int
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
			dbName, t).Scan(&n)
		if err != nil {
			fail("metadata table %q: query failed: %v", t, err)
			continue
		}
		if n == 0 {
			fail("metadata table %q is missing (include MetadataSchemaSQL() in migrations)", t)
			continue
		}
		rows, err := db.QueryContext(ctx,
			`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
			dbName, t)
		if err != nil {
			fail("metadata table %q: column query failed: %v", t, err)
			continue
		}
		have := map[string]bool{}
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err == nil {
				have[strings.ToLower(c)] = true
			}
		}
		rows.Close()
		for _, c := range schema.RequiredColumns[t] {
			if !have[strings.ToLower(c)] {
				fail("metadata table %q is missing column %q", t, c)
			}
		}
	}

	var stateRows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `replication_local_state`").Scan(&stateRows); err != nil {
		fail("replication_local_state unreadable: %v", err)
	} else if stateRows != 1 {
		fail("replication_local_state must contain exactly one row, found %d", stateRows)
	}

	for _, t := range tables {
		if err := t.Validate(); err != nil {
			fail("table %q invalid: %v", t.Name, err)
			continue
		}
		var n int
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
			dbName, t.Name).Scan(&n)
		if err != nil || n == 0 {
			fail("application table %q is missing", t.Name)
			continue
		}
		for _, col := range []string{t.idColumn(), t.nameColumn()} {
			err := db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
				dbName, t.Name, col).Scan(&n)
			if err != nil || n == 0 {
				fail("application table %q is missing column %q", t.Name, col)
			}
		}
		ins, upd, del := schema.TriggerNames(t.Name)
		for _, trg := range []string{ins, upd, del} {
			err := db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME = ?`,
				dbName, trg).Scan(&n)
			if err != nil || n == 0 {
				fail("trigger %q is missing (install TriggerSQL output)", trg)
			}
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w:\n  - %s", ErrValidation, strings.Join(problems, "\n  - "))
	}
	return nil
}
