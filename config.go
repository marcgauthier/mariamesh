package replication

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ProtocolVersion is the wire protocol version spoken by this package.
// It is exchanged during HELLO; mismatches refuse replication.
const ProtocolVersion = 1

// ALPN is the QUIC ALPN identifier for replication connections.
const ALPN = "mariamesh-repl/1"

// ForwardMode selects how received events propagate.
type ForwardMode int

const (
	// StoreAndForward persists every received event under its original
	// origin and re-serves it to other peers. This is the normal mode.
	StoreAndForward ForwardMode = iota
	// RelayOnly forwards without long-term retention beyond GC needs.
	// Reserved for future use; currently behaves as StoreAndForward.
	RelayOnly
)

// Logger is the structured logging interface used by the package. The host
// application adapts its own logger; the default discards.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Config configures a Replicator. DB, NodeID, IncarnationID, Namespace and
// TLSConfig are required; everything else has a default (see setDefaults).
type Config struct {
	// DB is the MariaDB handle. The schema (app tables, replication
	// metadata tables, triggers) must already exist; the package never
	// runs DDL.
	DB *sql.DB
	// NodeID is the stable identity of this node (survives reinstalls).
	NodeID uuid.UUID
	// IncarnationID identifies this specific build/data directory. A
	// retired incarnation may never rejoin; rebuilds use a new one.
	IncarnationID uuid.UUID
	// Namespace scopes identity derivation and the wire cluster check.
	Namespace uuid.UUID
	// NodeName is a human label advertised to peers. Optional.
	NodeName string
	// SchemaVersion is the exact application schema version. Peers with a
	// different version pause replication with ErrSchemaMismatch.
	SchemaVersion uint64
	// ListenAddr is the QUIC listen address, e.g. ":7443".
	ListenAddr string
	// TLSConfig provides mTLS credentials. NextProtos is set to ALPN
	// automatically when empty. Required.
	TLSConfig *tls.Config
	// ForwardMode selects the forwarding behavior.
	ForwardMode ForwardMode
	// Logger receives structured logs. Optional.
	Logger Logger

	// SyncBatchMaxEvents caps one sync batch. Default 500.
	SyncBatchMaxEvents int
	// SyncBatchMaxBytes caps one sync batch in bytes. Default 2MB.
	SyncBatchMaxBytes int
	// SyncInterval is the periodic anti-entropy tick. Default 5s.
	SyncInterval time.Duration
	// HeartbeatInterval is the membership heartbeat tick. Default 2s.
	HeartbeatInterval time.Duration
	// GCInterval is the garbage-collection tick. Zero disables periodic GC.
	GCInterval time.Duration
	// SeedChunkRows bounds rows per seed message. Default 500.
	SeedChunkRows int
	// DialTimeout bounds one dial attempt. Default 5s.
	DialTimeout time.Duration
	// IdleTimeout is the QUIC max idle timeout. Default 30s.
	IdleTimeout time.Duration
}

func (c *Config) setDefaults() {
	if c.SyncBatchMaxEvents <= 0 {
		c.SyncBatchMaxEvents = 500
	}
	if c.SyncBatchMaxBytes <= 0 {
		c.SyncBatchMaxBytes = 2 << 20
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = 5 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 2 * time.Second
	}
	if c.SeedChunkRows <= 0 {
		c.SeedChunkRows = 500
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = discardLogger{}
	}
}

// validate checks required fields.
func (c Config) validate() error {
	if c.DB == nil {
		return fmt.Errorf("replication: Config.DB is required")
	}
	if c.NodeID == uuid.Nil {
		return fmt.Errorf("replication: Config.NodeID is required")
	}
	if c.IncarnationID == uuid.Nil {
		return fmt.Errorf("replication: Config.IncarnationID is required")
	}
	if c.Namespace == uuid.Nil {
		return fmt.Errorf("replication: Config.Namespace is required")
	}
	if c.TLSConfig == nil {
		return fmt.Errorf("replication: Config.TLSConfig is required")
	}
	if c.ListenAddr == "" {
		return fmt.Errorf("replication: Config.ListenAddr is required")
	}
	return nil
}

// cloneTLS returns a copy of the TLS config with ALPN ensured.
func (c Config) cloneTLS() *tls.Config {
	t := c.TLSConfig.Clone()
	if len(t.NextProtos) == 0 {
		t.NextProtos = []string{ALPN}
	}
	return t
}

type discardLogger struct{}

func (discardLogger) Debug(string, ...any) {}
func (discardLogger) Info(string, ...any)  {}
func (discardLogger) Warn(string, ...any)  {}
func (discardLogger) Error(string, ...any) {}
