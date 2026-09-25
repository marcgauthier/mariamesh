package replication

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ID derives the immutable deterministic row ID for a replicated row:
//
//	id = UUIDv5(namespace, lower(table) + ":" + lower(name))
//
// table is the replicated table name and name is the row's initial human
// name. The result feeds the row's PRIMARY KEY id column exactly once at
// insert time; renames never regenerate it.
//
// Normalization is TrimSpace + ToLower on both inputs so applications that
// disagree on case still converge to one ID.
func ID(namespace uuid.UUID, table, name string) uuid.UUID {
	return uuid.NewSHA1(namespace, []byte(normalizeTable(table)+":"+normalizeName(name)))
}

// UUID is an alias of ID kept for API ergonomics.
func UUID(namespace uuid.UUID, table, name string) uuid.UUID {
	return ID(namespace, table, name)
}

// normalizeTable canonicalizes a table name for identity derivation.
func normalizeTable(table string) string {
	return strings.ToLower(strings.TrimSpace(table))
}

// normalizeName canonicalizes the initial row name for identity derivation.
func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// ValidateID reports whether rowID equals the expected deterministic ID for
// (namespace, table, initialName). Callers that track the initial name can
// use this to detect ID fabrication or algorithm drift.
func ValidateID(namespace uuid.UUID, table, initialName, rowID string) error {
	want := ID(namespace, table, initialName).String()
	if !strings.EqualFold(strings.TrimSpace(rowID), want) {
		return fmt.Errorf("%w: table %q name %q: got %q want %q",
			ErrInvalidID, table, initialName, rowID, want)
	}
	return nil
}

// ParseNodeID parses a node/incarnation/namespace UUID, wrapping parse errors.
func ParseNodeID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(strings.TrimSpace(s))
	if err != nil {
		return uuid.Nil, fmt.Errorf("replication: invalid uuid %q: %w", s, err)
	}
	return id, nil
}
