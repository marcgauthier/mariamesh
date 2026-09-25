package replication

import "errors"

// Sentinel errors returned by this package. Use errors.Is / errors.As to
// match them; wrapped context is added with %w.
var (
	// ErrNotStarted is returned when an operation requires a started Replicator.
	ErrNotStarted = errors.New("replication: not started")
	// ErrAlreadyStarted is returned when Start is called twice.
	ErrAlreadyStarted = errors.New("replication: already started")
	// ErrClosed is returned when operating on a closed Replicator.
	ErrClosed = errors.New("replication: closed")
	// ErrUnknownTable is returned when referencing a table that was not registered.
	ErrUnknownTable = errors.New("replication: unknown table")
	// ErrTableExists is returned when registering a duplicate table.
	ErrTableExists = errors.New("replication: table already registered")
	// ErrSchemaMismatch is returned when a peer's schema version differs.
	ErrSchemaMismatch = errors.New("replication: schema version mismatch")
	// ErrProtocolMismatch is returned on wire-protocol incompatibility.
	ErrProtocolMismatch = errors.New("replication: protocol version mismatch")
	// ErrNamespaceMismatch is returned when a message carries the wrong namespace.
	ErrNamespaceMismatch = errors.New("replication: namespace mismatch")
	// ErrRetired is returned when a retired incarnation attempts to rejoin.
	ErrRetired = errors.New("replication: incarnation is retired")
	// ErrInvalidID is returned when a row ID violates the identity contract.
	ErrInvalidID = errors.New("replication: invalid row id")
	// ErrImmutableID is returned when an update attempts to change a row ID.
	ErrImmutableID = errors.New("replication: row id is immutable")
	// ErrNotSeeded is returned when an operation needs a completed seed.
	ErrNotSeeded = errors.New("replication: node has not finished seeding")
	// ErrSeedInProgress is returned when a second seed is attempted concurrently.
	ErrSeedInProgress = errors.New("replication: seed already in progress")
	// ErrNoPeer is returned when no suitable peer exists for an operation.
	ErrNoPeer = errors.New("replication: no suitable peer")
	// ErrValidation is returned by Validate when the schema is incompatible.
	ErrValidation = errors.New("replication: schema validation failed")
)
