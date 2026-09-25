# MARIAMESH — masterless replication for MariaDB, as a Go package

MARIAMESH is an embeddable Go package that gives MariaDB-backed applications
multi-primary replication: every node accepts reads and writes locally while
disconnected, and nodes converge when they reconnect. The host application
stays in charge of its process, its schema, and its migrations; the package
owns change tracking, transport, sync, conflict resolution, membership,
seeding, and garbage collection.

## Rule 1: the application owns the schema

The package **never executes DDL**. It generates SQL; the application installs
it through normal migrations:

```go
replication.MetadataSchemaSQL() // replication_* tables (idempotent)
replication.TriggerSQL(table)   // <table>_repl_{insert,update,delete}
replication.DropTriggerSQL("device")
replication.ValidateSchema(ctx, db, tables) // read-only preflight
```

Startup contract:

```go
db := openMariaDB()
runMigrations(db) // app tables + metadata SQL + triggers

rep, err := replication.New(replication.Config{
    DB: db, NodeID: nodeID, IncarnationID: incarnationID,
    Namespace: namespace, SchemaVersion: 1,
    ListenAddr: ":7443", AdvertiseAddr: "10.0.0.5:7443",
    TLSConfig: tlsConfig, // mTLS; node ID bound via cert SAN
})
rep.RegisterTable(replication.Table{
    Name: "device", IDColumn: "id", NameColumn: "name",
    Columns: []replication.Column{{Name: "location"}, {Name: "enabled"}},
})
rep.Validate(ctx) // read-only; never creates anything
rep.Start(ctx)
defer rep.Close()
```

Every replicated table needs an immutable `id PRIMARY KEY` and a `name`
column. IDs derive deterministically so all nodes agree without coordination:

```go
id := replication.ID(namespace, "device", "Router-001")
// UUIDv5(namespace, "device:router-001"); renames never regenerate it
```

## How it works

**Change capture (MariaDB triggers).** Generated `AFTER INSERT/UPDATE/DELETE`
triggers allocate `(sequence, HLC)` from `replication_local_state` inside the
same transaction as the row change, so row + log commit atomically. Updates
record only changed columns using NULL-safe `<=>` comparison; ID changes are
rejected (`SIGNAL 45000`). Replicated writes set connection-local
`@replication_apply = 1` so triggers skip them (the flag is always reset
before the connection returns to the pool).

**Conflict resolution.** Per-field last-write-wins over Hybrid Logical Clock
versions, ordered by `(HLC, origin node, origin seq)` — deterministic on every
node regardless of delivery order. Deletes write tombstones that beat older
field writes, so offline nodes cannot resurrect rows.

**Sync.** Nodes exchange replication vectors (per-origin contiguous
sequences) and stream only missing ranges in bounded batches. ACKs are sent
only after the MariaDB transaction commits. Received events keep their
original `(origin, seq)` identity, so store-and-forward across partial meshes
is idempotent (`INSERT IGNORE`).

**Seeding.** New incarnations stream a consistent InnoDB snapshot (rows +
field versions + tombstones + vector) with a CRC32 integrity check, then
catch up via normal deltas. The seed vector pins GC, so cleanup continues
during seeding without endangering the joiner.

**Membership & GC.** Administrative `AddNode`/`RetireNode` bump a membership
epoch gossiped over datagram heartbeats; retirement is irreversible per
incarnation. GC deletes per-origin history below the minimum ACK across
`ACTIVE` nodes (+ seed pins) and pauses under a stale epoch.

## Node-to-node communication (ported from Corrosion)

The transport layer ports the architecture of Superfly's Corrosion
(Apache-2.0) to Go with `quic-go`:

| Corrosion (Rust/Quinn) | This package (Go/quic-go) |
|---|---|
| Cached conn per peer + reconnect-once retry | `internal/quic.Transport` |
| Server/client endpoint + TLS builders | `internal/quic` endpoint + `tls.Config` |
| 32 bidi / 256 uni streams, keepalives, datagrams | `DefaultQUICConfig` |
| Per-conn datagram/uni/bi task tree | `Server.Serve` accept + dispatch loops |
| `UniPayload` broadcasts, length-delimited | `KindBroadcast` over uni streams |
| `BiPayload SyncStart` → `serve_sync` | `HELLO` → symmetric Need/Batch/ACK session |
| FOCA over datagrams, RTT sampling, backoff announcer | Heartbeat gossip, `SampleRTTs`, periodic beats |
| Versioned `speedy` envelopes + cluster check | Versioned JSON envelopes + namespace check |

Encoding starts as length-prefixed JSON for debuggability; the version field
reserves room for a future binary codec without changing the architecture.

## Repository layout

```text
.                       package replication (public API)
  replication.go        Replicator: lifecycle, membership, peers, seed, status
  config.go table.go identity.go schema.go status.go errors.go
internal/
  clock/      Hybrid Logical Clock
  conflict/   per-field LWW comparison
  changelog/  events, origins, replication vectors
  schema/     metadata DDL + MariaDB trigger generation
  store/      DML persistence (log, versions, progress, nodes, bootstrap)
  apply/      transactional apply engine (@replication_apply discipline)
  protocol/   framing + versioned envelopes + message types
  quic/       Corrosion-style transport, listener, test certs
  sync/       symmetric delta sessions + gossip broadcaster
  membership/ heartbeat gossip + epoch administration
  gc/         watermark computation + bounded collection
  seed/       snapshot source + target loader + checksums
examples/basic/  minimal host integration (prints SQL without MARIAMESH_DSN)
```

Only the root package is public API; `internal/` carries no compatibility
promise.

## Building and testing

```sh
go build ./...
go vet ./...
go test ./...          # unit + sqlmock + live QUIC loopback tests
go test -race ./...    # race detector
```

DB-backed paths are verified with `go-sqlmock` (store, apply, sync session,
seed stream, membership, GC) plus a real `quic-go` client/server exchange
over loopback UDP. No MariaDB server is required to run the suite; run
`examples/basic` with `MARIAMESH_DSN` against a real MariaDB 10.6+/11.x for a
live smoke test.

## Production notes and limitations (v1)

- Exact `SchemaVersion` match required; mismatched peers refuse sessions.
- Single `id` + `name` table contract; `id` columns must be character types
  (`BINARY(16)` IDs are not supported yet).
- Heartbeat member lists cap at 32 entries; larger clusters converge more
  slowly. SWIM active probing is not implemented (heartbeats + `last_seen`
  suspicion only).
- Tombstones are retained indefinitely; add a retention purge once delete
  propagation windows are characterized for your workload.
- Trigger-local HLC uses millisecond wall time; sub-millisecond same-node
  writes disambiguate via the origin-sequence tie-break.
