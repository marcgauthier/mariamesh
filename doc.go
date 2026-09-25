// Package replication provides embeddable masterless (multi-primary)
// replication for MariaDB-backed Go applications.
//
// The host application owns the database schema: it runs migrations,
// creates application tables, installs the replication metadata tables and
// generated triggers (see MetadataSchemaSQL, TriggerSQL), and decides which
// tables participate. This package never executes DDL. It owns change
// tracking, QUIC transport, synchronization, per-field conflict resolution,
// membership, seeding, and garbage collection.
//
// # Node-to-node communication
//
// The transport layer ports the architecture of Superfly's Corrosion
// (Apache-2.0, https://github.com/superfly/corrosion) to Go:
//
//   - one cached long-lived QUIC connection per peer address, shared by all
//     traffic, with reconnect-on-failure and a single retry;
//   - unreliable QUIC datagrams for membership (SWIM-style) traffic;
//   - unidirectional streams for fire-and-forget change broadcasts;
//   - bidirectional streams for version-exchange + delta sync sessions;
//   - length-delimited, versioned message envelopes with a namespace
//     (cluster) check on receipt;
//   - periodic RTT sampling from live connections to inform peer health;
//   - graceful listener shutdown: refuse new handshakes, drain, then close.
//
// The replication semantics on top (transactional per-origin sequences,
// hybrid-logical-clock per-field last-write-wins, tombstones, replication
// vectors with commit-before-ACK, per-origin GC watermarks, and
// consistent-snapshot seeding with retention pins) follow this package's own
// design for MariaDB.
//
// Basic integration:
//
//	db := openMariaDB()
//	runMigrations(db) // app tables + replication.MetadataSchemaSQL() + triggers
//
//	r, err := replication.New(replication.Config{
//	    DB: db, NodeID: nodeID, IncarnationID: incarnationID,
//	    Namespace: namespace, SchemaVersion: 1,
//	    ListenAddr: ":7443", TLSConfig: tlsConfig,
//	})
//	if err != nil { ... }
//	if err := r.RegisterTable(replication.Table{Name: "device", IDColumn: "id", NameColumn: "name"}); err != nil { ... }
//	if err := r.Validate(ctx); err != nil { ... } // read-only, never DDL
//	if err := r.Start(ctx); err != nil { ... }
//	defer r.Close()
package replication
