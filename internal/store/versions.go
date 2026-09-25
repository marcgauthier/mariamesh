package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/mariamesh/mariamesh/internal/clock"
)

// FieldVersion is the winning write recorded for one column (or tombstone).
type FieldVersion struct {
	HLC       clock.HLC
	OriginID  string
	OriginSeq uint64
}

// GetFieldVersions returns stored column versions for one row.
func GetFieldVersions(ctx context.Context, q qtx, table, rowID string) (map[string]FieldVersion, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT `column_name`, `hlc_physical`, `hlc_logical`, `origin_node_id`, `origin_seq` FROM `replication_field_version` WHERE `table_name` = ? AND `row_id` = ?",
		table, rowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]FieldVersion{}
	for rows.Next() {
		var col string
		var v FieldVersion
		if err := rows.Scan(&col, &v.HLC.Physical, &v.HLC.Logical, &v.OriginID, &v.OriginSeq); err != nil {
			return nil, err
		}
		out[col] = v
	}
	return out, rows.Err()
}

// SetFieldVersion records the winning version of one column (upsert).
func SetFieldVersion(ctx context.Context, q qtx, table, rowID, column string, h clock.HLC, originID string, originSeq uint64) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO `+"`replication_field_version`"+` (`+"`table_name`"+`, `+"`row_id`"+`, `+"`column_name`"+`, `+"`hlc_physical`"+`, `+"`hlc_logical`"+`, `+"`origin_node_id`"+`, `+"`origin_seq`"+`)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE `+"`hlc_physical`"+` = VALUES(`+"`hlc_physical`"+`), `+"`hlc_logical`"+` = VALUES(`+"`hlc_logical`"+`),
			`+"`origin_node_id`"+` = VALUES(`+"`origin_node_id`"+`), `+"`origin_seq`"+` = VALUES(`+"`origin_seq`"+`)`,
		table, rowID, column, h.Physical, h.Logical, originID, originSeq)
	return err
}

// GetTombstone returns the delete marker for a row, if any.
func GetTombstone(ctx context.Context, q qtx, table, rowID string) (FieldVersion, bool, error) {
	var v FieldVersion
	err := q.QueryRowContext(ctx,
		"SELECT `hlc_physical`, `hlc_logical`, `origin_node_id`, `origin_seq` FROM `replication_tombstone` WHERE `table_name` = ? AND `row_id` = ?",
		table, rowID).Scan(&v.HLC.Physical, &v.HLC.Logical, &v.OriginID, &v.OriginSeq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return FieldVersion{}, false, nil
		}
		return FieldVersion{}, false, err
	}
	return v, true, nil
}

// SetTombstone records (or replaces) the delete marker for a row.
func SetTombstone(ctx context.Context, q qtx, table, rowID string, h clock.HLC, originID string, originSeq uint64) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO `+"`replication_tombstone`"+` (`+"`table_name`"+`, `+"`row_id`"+`, `+"`hlc_physical`"+`, `+"`hlc_logical`"+`, `+"`origin_node_id`"+`, `+"`origin_seq`"+`)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE `+"`hlc_physical`"+` = VALUES(`+"`hlc_physical`"+`), `+"`hlc_logical`"+` = VALUES(`+"`hlc_logical`"+`),
			`+"`origin_node_id`"+` = VALUES(`+"`origin_node_id`"+`), `+"`origin_seq`"+` = VALUES(`+"`origin_seq`"+`)`,
		table, rowID, h.Physical, h.Logical, originID, originSeq)
	return err
}

// ClearTombstone removes the delete marker (a newer write resurrected the row).
func ClearTombstone(ctx context.Context, q qtx, table, rowID string) error {
	_, err := q.ExecContext(ctx, "DELETE FROM `replication_tombstone` WHERE `table_name` = ? AND `row_id` = ?", table, rowID)
	return err
}

// VersionRow is a stored field version with its coordinates (for seeding).
type VersionRow struct {
	Table     string
	RowID     string
	Column    string
	HLC       clock.HLC
	OriginID  string
	OriginSeq uint64
}

// AllFieldVersions streams every field version for one table (seed source).
func AllFieldVersions(ctx context.Context, q qtx, table string) ([]VersionRow, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT `row_id`, `column_name`, `hlc_physical`, `hlc_logical`, `origin_node_id`, `origin_seq` FROM `replication_field_version` WHERE `table_name` = ? ORDER BY `row_id`, `column_name`",
		table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VersionRow
	for rows.Next() {
		var v VersionRow
		v.Table = table
		if err := rows.Scan(&v.RowID, &v.Column, &v.HLC.Physical, &v.HLC.Logical, &v.OriginID, &v.OriginSeq); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// TombstoneRow is a stored tombstone with its coordinates (for seeding).
type TombstoneRow struct {
	Table     string
	RowID     string
	HLC       clock.HLC
	OriginID  string
	OriginSeq uint64
}

// AllTombstones streams every tombstone for one table (seed source).
func AllTombstones(ctx context.Context, q qtx, table string) ([]TombstoneRow, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT `row_id`, `hlc_physical`, `hlc_logical`, `origin_node_id`, `origin_seq` FROM `replication_tombstone` WHERE `table_name` = ? ORDER BY `row_id`",
		table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TombstoneRow
	for rows.Next() {
		var t TombstoneRow
		t.Table = table
		if err := rows.Scan(&t.RowID, &t.HLC.Physical, &t.HLC.Logical, &t.OriginID, &t.OriginSeq); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
