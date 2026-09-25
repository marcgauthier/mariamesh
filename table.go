package replication

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Column describes one replicated column of a table. Type is informational
// (used for diagnostics and seed ordering); the triggers treat values
// opaquely through JSON.
type Column struct {
	// Name is the SQL column name.
	Name string
	// Type is the SQL type as declared, e.g. "VARCHAR(255)". Optional.
	Type string
}

// Table declares a replicated application table.
type Table struct {
	// Name is the SQL table name.
	Name string
	// IDColumn is the immutable primary key column (default "id").
	IDColumn string
	// NameColumn is the human-name column used for identity derivation (default "name").
	NameColumn string
	// Columns lists replicated non-key columns for trigger generation. When
	// empty, only NameColumn is tracked.
	Columns []Column
}

// idColumn returns the effective ID column name.
func (t Table) idColumn() string {
	if t.IDColumn == "" {
		return "id"
	}
	return t.IDColumn
}

// nameColumn returns the effective name column name.
func (t Table) nameColumn() string {
	if t.NameColumn == "" {
		return "name"
	}
	return t.NameColumn
}

// ReplicatedColumns returns the ordered, de-duplicated list of value columns
// covered by change tracking: NameColumn first, then every entry of Columns
// except the ID column, sorted. When Columns is empty only NameColumn is
// tracked.
func (t Table) ReplicatedColumns() []string {
	seen := map[string]bool{strings.ToLower(t.nameColumn()): true}
	rest := []string{}
	for _, c := range t.Columns {
		name := strings.TrimSpace(c.Name)
		if name == "" || strings.EqualFold(name, t.idColumn()) {
			continue
		}
		if seen[strings.ToLower(name)] {
			continue
		}
		seen[strings.ToLower(name)] = true
		rest = append(rest, name)
	}
	sort.Strings(rest)
	return append([]string{t.nameColumn()}, rest...)
}

// Validate checks the table declaration for internal consistency. It does
// not touch the database; see Replicator.Validate for DB-side checks.
func (t Table) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("replication: table name is required")
	}
	if strings.ContainsAny(t.Name, "`\"';") {
		return fmt.Errorf("replication: table name %q contains forbidden characters", t.Name)
	}
	if strings.EqualFold(t.idColumn(), t.nameColumn()) {
		return fmt.Errorf("replication: table %q: id column and name column must differ", t.Name)
	}
	for _, c := range t.Columns {
		if strings.TrimSpace(c.Name) == "" {
			return fmt.Errorf("replication: table %q: column name is required", t.Name)
		}
		if strings.ContainsAny(c.Name, "`\"';") {
			return fmt.Errorf("replication: table %q: column %q contains forbidden characters", t.Name, c.Name)
		}
	}
	return nil
}

// registry is a concurrency-safe table registry.
type registry struct {
	mu     sync.RWMutex
	tables map[string]Table // keyed by lower(table name)
}

// register adds a table; it fails on duplicates or invalid declarations.
func (r *registry) register(t Table) error {
	if err := t.Validate(); err != nil {
		return err
	}
	key := normalizeTable(t.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tables == nil {
		r.tables = map[string]Table{}
	}
	if _, ok := r.tables[key]; ok {
		return fmt.Errorf("%w: %q", ErrTableExists, t.Name)
	}
	r.tables[key] = t
	return nil
}

// lookup returns the registered table or ErrUnknownTable.
func (r *registry) lookup(name string) (Table, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tables[normalizeTable(name)]
	if !ok {
		return Table{}, fmt.Errorf("%w: %q", ErrUnknownTable, name)
	}
	return t, nil
}

// all returns registered tables sorted by name.
func (r *registry) all() []Table {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Table, 0, len(r.tables))
	for _, t := range r.tables {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
