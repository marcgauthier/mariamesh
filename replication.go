package replication

import (
	"context"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	quicgo "github.com/quic-go/quic-go"

	"github.com/mariamesh/mariamesh/internal/apply"
	"github.com/mariamesh/mariamesh/internal/changelog"
	"github.com/mariamesh/mariamesh/internal/clock"
	"github.com/mariamesh/mariamesh/internal/gc"
	"github.com/mariamesh/mariamesh/internal/membership"
	"github.com/mariamesh/mariamesh/internal/protocol"
	"github.com/mariamesh/mariamesh/internal/quic"
	"github.com/mariamesh/mariamesh/internal/seed"
	"github.com/mariamesh/mariamesh/internal/store"
	internsync "github.com/mariamesh/mariamesh/internal/sync"
)

// Replicator embeds masterless replication into a host application. The zero
// value is unusable; construct with New, register tables, Validate, Start.
type Replicator struct {
	cfg  Config
	db   *sql.DB
	st   *store.Store
	clk  *clock.Clock
	self changelog.Origin

	tables registry

	mu        sync.Mutex
	started   bool
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startedAt time.Time

	transport   *quic.Transport
	server      *quic.Server
	members     *membership.Manager
	syncEnv     *internsync.Env
	broadcaster *internsync.Broadcaster
	seedTables  []seed.Table

	peers      map[string]*peerEntry // nodeID -> entry
	byAddr     map[string]string     // addr -> nodeID
	needsSeed  map[string]bool
	rtts       map[string]time.Duration
	watermarks changelog.Vector

	syncSem  chan struct{}
	syncKick chan struct{}
	seedMu   sync.Mutex // serializes seed snapshot+pin with GC
}

type peerEntry struct {
	nodeID string
	addr   string
}

// New validates cfg and returns a stopped Replicator.
func New(cfg Config) (*Replicator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.setDefaults()
	cfg.TLSConfig = cfg.cloneTLS()
	r := &Replicator{
		cfg:       cfg,
		db:        cfg.DB,
		st:        store.New(cfg.DB),
		clk:       clock.New(),
		self:      changelog.Origin{NodeID: cfg.NodeID.String(), IncarnationID: cfg.IncarnationID.String()},
		peers:     map[string]*peerEntry{},
		byAddr:    map[string]string{},
		needsSeed: map[string]bool{},
		rtts:      map[string]time.Duration{},
		syncKick:  make(chan struct{}, 1),
	}
	r.members = membership.New(cfg.DB, cfg.Namespace.String(), r.self.NodeID, r.self.IncarnationID,
		cfg.NodeName, cfg.SchemaVersion, advertiseAddr(cfg), cfg.Logger)
	return r, nil
}

// advertiseAddr returns the dialable address gossiped to peers.
func advertiseAddr(cfg Config) string {
	if cfg.AdvertiseAddr != "" {
		return cfg.AdvertiseAddr
	}
	return cfg.ListenAddr
}

// RegisterTable declares a replicated table. It must be called before Start.
func (r *Replicator) RegisterTable(t Table) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return fmt.Errorf("replication: RegisterTable after Start is not supported")
	}
	return r.tables.register(t)
}

// Validate performs read-only checks: config sanity plus ValidateSchema over
// the registered tables. It never writes.
func (r *Replicator) Validate(ctx context.Context) error {
	if err := r.cfg.validate(); err != nil {
		return err
	}
	return ValidateSchema(ctx, r.db, r.tables.all())
}

// Start initializes local state, binds the QUIC listener, and launches the
// sync, broadcast, membership, and GC loops. ctx is the parent lifetime;
// Close terminates everything regardless.
func (r *Replicator) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return ErrAlreadyStarted
	}
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.started = true
	r.startedAt = time.Now()
	r.mu.Unlock()

	liveCtx := r.ctx
	st, err := r.st.EnsureLocalState(liveCtx, r.self.NodeID, r.self.IncarnationID)
	if err != nil {
		r.shutdown()
		return err
	}
	r.clk.Restore(st.HLC)
	if err := r.members.LoadEpoch(liveCtx); err != nil {
		r.shutdown()
		return err
	}
	if err := r.members.EnsureSelf(liveCtx, store.StatusJoining); err != nil {
		r.shutdown()
		return err
	}

	// Snapshot the table registry into apply/seed specs.
	applyTables := map[string]apply.TableApply{}
	r.seedTables = nil
	for _, t := range r.tables.all() {
		cols := map[string]bool{}
		for _, c := range t.ReplicatedColumns() {
			cols[c] = true
		}
		applyTables[normalizeTable(t.Name)] = apply.TableApply{Name: t.Name, IDColumn: t.idColumn(), Columns: cols}
		r.seedTables = append(r.seedTables, seed.Table{Name: t.Name, IDColumn: t.idColumn(), Columns: t.ReplicatedColumns()})
	}
	applier := apply.New(r.db, r.clk, r.self, applyTables)

	r.syncEnv = &internsync.Env{
		Store: r.st, Applier: applier, Clock: r.clk, Self: r.self,
		Namespace: r.cfg.Namespace.String(), AdvertiseAddr: advertiseAddr(r.cfg),
		ProtocolVersion: ProtocolVersion,
		SchemaVersion:   r.cfg.SchemaVersion, Epoch: r.members.Epoch,
		BatchMaxEvents: r.cfg.SyncBatchMaxEvents, BatchMaxBytes: r.cfg.SyncBatchMaxBytes,
		RecordPeerVector: func(ctx context.Context, peer changelog.Origin, v changelog.Vector) error {
			return store.SetProgressMany(ctx, r.db, peer, v)
		},
		CheckPeer: func(ctx context.Context, h protocol.Hello) error {
			return r.members.CheckPeer(ctx, h)
		},
		Log: r.cfg.Logger,
	}
	r.broadcaster = internsync.NewBroadcaster(r.db, r.self, r.cfg.Namespace.String())

	quicConf := quic.DefaultQUICConfig(r.cfg.IdleTimeout)
	r.transport = quic.NewTransport(r.cfg.TLSConfig, ALPN, quicConf, r.cfg.DialTimeout, r.cfg.Logger)
	srv, err := quic.Listen(r.cfg.ListenAddr, r.cfg.TLSConfig, ALPN, quicConf, r.cfg.Logger)
	if err != nil {
		r.shutdown()
		return err
	}
	r.server = srv

	r.syncSem = make(chan struct{}, 4)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = srv.Serve(liveCtx, quic.Handlers{
			OnDatagram: r.onDatagram,
			OnUni:      r.onUni,
			OnBi:       r.onBi,
		})
	}()
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.syncLoop(liveCtx) }()
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.broadcastLoop(liveCtx) }()
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.heartbeatLoop(liveCtx) }()
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.rttLoop(liveCtx) }()
	if r.cfg.GCInterval > 0 {
		r.wg.Add(1)
		go func() { defer r.wg.Done(); r.gcLoop(liveCtx) }()
	}

	// First-node promotion: fresh database, no peers anywhere -> ACTIVE.
	if r.peerCount() == 0 && r.logEmpty(liveCtx) {
		if status, err := r.members.SelfStatus(liveCtx); err == nil && status == store.StatusJoining {
			if err := r.members.Activate(liveCtx, r.self.NodeID); err != nil {
				r.cfg.Logger.Warn("replication: first-node activation failed", "err", err)
			} else {
				r.cfg.Logger.Info("replication: first node activated")
			}
		}
	}
	r.cfg.Logger.Info("replication: started", "node", r.self.NodeID, "addr", srv.Addr().String())
	return nil
}

// Close stops all loops and the listener. It never closes cfg.DB.
func (r *Replicator) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	r.shutdown()
	return nil
}

func (r *Replicator) shutdown() {
	r.mu.Lock()
	cancel := r.cancel
	srv := r.server
	tr := r.transport
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if srv != nil {
		_ = srv.Close()
	}
	r.wg.Wait()
	if tr != nil {
		_ = tr.Close()
	}
}

func (r *Replicator) requireStarted() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if !r.started {
		return ErrNotStarted
	}
	return nil
}

// AddPeer records a peer's dial address. Usable before Start for bootstrap.
func (r *Replicator) AddPeer(nodeID, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers[nodeID] = &peerEntry{nodeID: nodeID, addr: addr}
	r.byAddr[addr] = nodeID
}

// RemovePeer forgets a peer and drops its cached connection.
func (r *Replicator) RemovePeer(nodeID string) {
	r.mu.Lock()
	e, ok := r.peers[nodeID]
	if ok {
		delete(r.peers, nodeID)
		delete(r.byAddr, e.addr)
		delete(r.needsSeed, nodeID)
	}
	tr := r.transport
	r.mu.Unlock()
	if ok && tr != nil {
		tr.Drop(e.addr)
	}
}

// Connect runs one immediate sync session against addr.
func (r *Replicator) Connect(ctx context.Context, addr string) error {
	if err := r.requireStarted(); err != nil {
		return err
	}
	return r.syncWith(ctx, addr, r.nodeIDForAddr(addr))
}

// Peers returns the in-memory address book with liveness.
func (r *Replicator) Peers() []PeerInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PeerInfo, 0, len(r.peers))
	for _, e := range r.peers {
		_, connected := r.transport.PeerConn(e.addr)
		out = append(out, PeerInfo{NodeID: e.nodeID, Addr: e.addr, RTT: r.rtts[e.addr], Connected: connected, NeedsSeed: r.needsSeed[e.nodeID]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// AddNode registers a cluster member (administrative, epoch-bumped).
func (r *Replicator) AddNode(ctx context.Context, nodeID, incarnationID, name, addr string, schemaVersion uint64) error {
	if err := r.requireStarted(); err != nil {
		return err
	}
	if err := r.members.AddNode(ctx, nodeID, incarnationID, name, addr, schemaVersion); err != nil {
		return err
	}
	if addr != "" {
		r.AddPeer(nodeID, addr)
	}
	r.kickSync()
	return nil
}

// RetireNode irreversibly retires a node's current incarnation.
func (r *Replicator) RetireNode(ctx context.Context, nodeID string) error {
	if err := r.requireStarted(); err != nil {
		return err
	}
	return r.members.RetireNode(ctx, nodeID)
}

// Nodes lists known members with derived health.
func (r *Replicator) Nodes(ctx context.Context) ([]NodeInfo, error) {
	rows, err := store.ListNodes(ctx, r.db)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := make([]NodeInfo, 0, len(rows))
	for _, n := range rows {
		out = append(out, NodeInfo{
			NodeID: n.NodeID, IncarnationID: n.IncarnationID, Name: n.Name,
			Status: n.Status, Health: membership.Health(n, r.cfg.HeartbeatInterval, now),
			MembershipEpoch: n.MembershipEpoch, SchemaVersion: n.SchemaVersion,
			Addr: n.Addr, LastSeen: n.LastSeen,
		})
	}
	return out, nil
}

// Progress returns the local replication vector.
func (r *Replicator) Progress(ctx context.Context) (changelog.Vector, error) {
	return r.st.LocalVector(ctx, r.self)
}

// GCWatermarks recomputes the collection watermarks (read-only).
func (r *Replicator) GCWatermarks(ctx context.Context) (changelog.Vector, error) {
	return gc.Watermarks(ctx, r.db)
}

// CollectGarbage runs one bounded GC cycle now.
func (r *Replicator) CollectGarbage(ctx context.Context) (GCResult, error) {
	var out GCResult
	if err := r.requireStarted(); err != nil {
		return out, err
	}
	r.seedMu.Lock()
	defer r.seedMu.Unlock()
	res, err := gc.Run(ctx, r.db, 1000)
	if err != nil {
		return out, err
	}
	r.mu.Lock()
	r.watermarks = res.Watermarks
	r.mu.Unlock()
	out = GCResult{Watermarks: res.Watermarks, Deleted: res.Deleted, Origins: res.Origins}
	return out, nil
}

// GCResult summarizes a collection cycle.
type GCResult struct {
	Watermarks changelog.Vector
	Deleted    int64
	Origins    int
}

// PendingChanges reports per-origin backlog (max stored vs contiguous).
func (r *Replicator) PendingChanges(ctx context.Context) ([]PendingOrigin, error) {
	vec, err := r.st.LocalVector(ctx, r.self)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx,
		"SELECT `origin_node_id`, `origin_incarnation_id`, MAX(`origin_seq`) FROM `replication_log` GROUP BY 1, 2")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	maxima := map[string]uint64{}
	for rows.Next() {
		var node, inc string
		var m uint64
		if err := rows.Scan(&node, &inc, &m); err != nil {
			return nil, err
		}
		maxima[node+":"+inc] = m
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	keys := map[string]bool{}
	for k := range vec {
		keys[k] = true
	}
	for k := range maxima {
		keys[k] = true
	}
	out := []PendingOrigin{}
	for k := range keys {
		c, m := vec[k], maxima[k]
		var p uint64
		if m > c {
			p = m - c
		}
		out = append(out, PendingOrigin{Origin: k, Contiguous: c, MaxStored: m, Pending: p})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Origin < out[j].Origin })
	return out, nil
}

// Status snapshots replicator state.
func (r *Replicator) Status(ctx context.Context) (Status, error) {
	var s Status
	s.NodeID, s.IncarnationID = r.self.NodeID, r.self.IncarnationID
	s.SchemaVersion = r.cfg.SchemaVersion
	s.Epoch = r.members.Epoch()
	s.StartedAt = r.startedAt
	state, err := r.members.SelfStatus(ctx)
	if err != nil {
		return s, err
	}
	s.State = state
	vec, err := r.st.LocalVector(ctx, r.self)
	if err != nil {
		return s, err
	}
	s.Vector = vec
	s.Peers = r.Peers()
	nodes, err := r.Nodes(ctx)
	if err != nil {
		return s, err
	}
	s.Nodes = nodes
	r.mu.Lock()
	s.GCWatermarks = r.watermarks
	r.mu.Unlock()
	pending, err := r.PendingChanges(ctx)
	if err != nil {
		return s, err
	}
	s.Pending = pending
	return s, nil
}

// BeginSeed streams a full snapshot from sourceAddr into this (empty,
// JOINING) node. It does not activate the node; see Join.
func (r *Replicator) BeginSeed(ctx context.Context, sourceAddr string) error {
	if err := r.requireStarted(); err != nil {
		return err
	}
	status, err := r.members.SelfStatus(ctx)
	if err != nil {
		return err
	}
	if status == store.StatusActive {
		return fmt.Errorf("replication: node is already ACTIVE; seeding requires a fresh JOINING node")
	}
	tables := map[string]seed.Table{}
	for _, t := range r.seedTables {
		tables[normalizeTable(t.Name)] = t
	}
	target := &seed.Target{
		DB: r.db, Clock: r.clk, Self: r.self,
		Namespace: r.cfg.Namespace.String(), SchemaVersion: r.cfg.SchemaVersion, Tables: tables,
	}
	nodeID := r.nodeIDForAddr(sourceAddr)
	str, err := r.transport.OpenBI(ctx, sourceAddr, nodeID)
	if err != nil {
		return err
	}
	defer str.Close()
	req := protocol.SeedRequest{
		BootstrapID: uuid.NewString(), JoiningNodeID: r.self.NodeID,
		JoiningIncarnationID: r.self.IncarnationID, SchemaVersion: r.cfg.SchemaVersion,
	}
	vec, err := target.Fetch(ctx, str, req)
	if err != nil {
		return err
	}
	r.cfg.Logger.Info("replication: seed complete", "source", sourceAddr, "origins", len(vec))
	r.kickSync()
	return nil
}

// Join seeds from sourceAddr and then activates this node. Deltas after the
// seed vector arrive through normal sync.
func (r *Replicator) Join(ctx context.Context, sourceAddr string) error {
	if err := r.BeginSeed(ctx, sourceAddr); err != nil {
		return err
	}
	if err := r.members.Activate(ctx, r.self.NodeID); err != nil {
		return err
	}
	r.cfg.Logger.Info("replication: node ACTIVE", "node", r.self.NodeID)
	return nil
}

// --- background loops ---

func (r *Replicator) kickSync() {
	select {
	case r.syncKick <- struct{}{}:
	default:
	}
}

func (r *Replicator) syncLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.SyncInterval)
	defer t.Stop()
	r.syncAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.syncKick:
			r.syncAll(ctx)
		case <-t.C:
			r.syncAll(ctx)
		}
	}
}

func (r *Replicator) syncTargets() []peerEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]peerEntry, 0, len(r.peers))
	for _, e := range r.peers {
		if e.addr != "" {
			out = append(out, *e)
		}
	}
	return out
}

func (r *Replicator) syncAll(ctx context.Context) {
	targets := r.syncTargets()
	if len(targets) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, pe := range targets {
		select {
		case r.syncSem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		wg.Add(1)
		go func(pe peerEntry) {
			defer wg.Done()
			defer func() { <-r.syncSem }()
			if err := r.syncWith(ctx, pe.addr, pe.nodeID); err != nil {
				r.cfg.Logger.Debug("replication: sync failed", "peer", pe.nodeID, "addr", pe.addr, "err", err)
			}
		}(pe)
	}
	wg.Wait()
}

func (r *Replicator) syncWith(ctx context.Context, addr, nodeID string) error {
	sctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	str, err := r.transport.OpenBI(sctx, addr, nodeID)
	if err != nil {
		return err
	}
	defer str.Close()
	if err := internsync.Session(sctx, str, r.syncEnv, true); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.needsSeed, nodeID)
	r.mu.Unlock()
	return nil
}

func (r *Replicator) broadcastLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.broadcastOnce(ctx)
		}
	}
}

func (r *Replicator) broadcastOnce(ctx context.Context) {
	events, err := r.broadcaster.Collect(ctx, 100)
	if err != nil || len(events) == 0 {
		return
	}
	frame, err := internsync.BroadcastFrame(r.cfg.Namespace.String(), events)
	if err != nil {
		return
	}
	for _, pe := range r.randomPeers(3) {
		if err := r.transport.SendUni(ctx, pe.addr, pe.nodeID, frame); err != nil {
			r.cfg.Logger.Debug("replication: broadcast failed", "peer", pe.nodeID, "err", err)
		}
	}
}

func (r *Replicator) randomPeers(n int) []peerEntry {
	all := r.syncTargets()
	if len(all) <= n {
		return all
	}
	rand.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	return all[:n]
}

func (r *Replicator) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	r.beat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.beat(ctx)
		}
	}
}

func (r *Replicator) beat(ctx context.Context) {
	frame, err := r.members.Heartbeat(ctx)
	if err != nil {
		return
	}
	_ = store.TouchNode(ctx, r.db, r.self.NodeID)
	for _, pe := range r.syncTargets() {
		if err := r.transport.SendDatagram(ctx, pe.addr, pe.nodeID, frame); err != nil {
			r.cfg.Logger.Debug("replication: heartbeat failed", "peer", pe.nodeID, "err", err)
		}
	}
}

func (r *Replicator) rttLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rtts := r.transport.SampleRTTs()
			r.mu.Lock()
			r.rtts = rtts
			r.mu.Unlock()
		}
	}
}

func (r *Replicator) gcLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.seedMu.Lock()
			res, err := gc.Run(ctx, r.db, 1000)
			r.seedMu.Unlock()
			if err != nil {
				r.cfg.Logger.Debug("replication: gc skipped", "err", err)
				continue
			}
			if res.Deleted > 0 {
				r.cfg.Logger.Info("replication: gc collected", "deleted", res.Deleted, "origins", res.Origins)
			}
			r.mu.Lock()
			r.watermarks = res.Watermarks
			r.mu.Unlock()
		}
	}
}

// --- inbound handlers ---

func certNodeID(conn *quicgo.Conn) string {
	certs := conn.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		return ""
	}
	return certSAN(certs[0])
}

func certSAN(cert *x509.Certificate) string {
	if cert == nil || len(cert.DNSNames) == 0 {
		return ""
	}
	return cert.DNSNames[0]
}

func (r *Replicator) onDatagram(ctx context.Context, conn *quicgo.Conn, data []byte) {
	// Bind the claimed sender to its certificate when mTLS presents one.
	if cn := certNodeID(conn); cn != "" {
		if e, err := protocol.Decode(data, r.cfg.Namespace.String()); err == nil && e.Kind == protocol.KindHeartbeat {
			var hb protocol.Heartbeat
			if protocol.DecodeBody(e, &hb) == nil && hb.NodeID != "" && hb.NodeID != cn {
				r.cfg.Logger.Warn("replication: heartbeat identity mismatch", "claimed", hb.NodeID, "cert", cn)
				return
			}
			if hb.Addr != "" {
				r.learnPeer(hb.NodeID, hb.Addr)
			}
		}
	} else if e, err := protocol.Decode(data, r.cfg.Namespace.String()); err == nil && e.Kind == protocol.KindHeartbeat {
		var hb protocol.Heartbeat
		if protocol.DecodeBody(e, &hb) == nil && hb.Addr != "" {
			r.learnPeer(hb.NodeID, hb.Addr)
		}
	}
	if err := r.members.HandleHeartbeat(ctx, data); err != nil {
		r.cfg.Logger.Debug("replication: heartbeat rejected", "err", err)
	}
}

func (r *Replicator) learnPeer(nodeID, addr string) {
	if nodeID == "" || addr == "" || nodeID == r.self.NodeID {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.peers[nodeID]; !ok {
		r.peers[nodeID] = &peerEntry{nodeID: nodeID, addr: addr}
		r.byAddr[addr] = nodeID
	}
}

func (r *Replicator) onUni(ctx context.Context, conn *quicgo.Conn, str *quicgo.ReceiveStream) {
	defer str.CancelRead(0)
	if err := internsync.HandleBroadcast(ctx, str, r.syncEnv); err != nil {
		r.cfg.Logger.Debug("replication: broadcast rejected", "err", err)
	}
}

func (r *Replicator) onBi(ctx context.Context, conn *quicgo.Conn, str *quicgo.Stream) {
	defer str.Close()
	frame, err := readFrameTimeout(ctx, str, 15*time.Second)
	if err != nil {
		return
	}
	e, err := protocol.Decode(frame, r.cfg.Namespace.String())
	if err != nil {
		return
	}
	switch e.Kind {
	case protocol.KindHello:
		r.serveSync(ctx, conn, str, frame)
	case protocol.KindSeedRequest:
		var req protocol.SeedRequest
		if err := protocol.DecodeBody(e, &req); err != nil {
			return
		}
		r.serveSeed(ctx, conn, str, req)
	default:
		r.cfg.Logger.Debug("replication: unknown session open", "kind", e.Kind)
	}
}

func (r *Replicator) serveSync(ctx context.Context, conn *quicgo.Conn, str *quicgo.Stream, firstFrame []byte) {
	cn := certNodeID(conn)
	env := *r.syncEnv
	outer := env.CheckPeer
	env.CheckPeer = func(ctx context.Context, h protocol.Hello) error {
		if cn != "" && h.NodeID != cn {
			return fmt.Errorf("replication: hello claims %q, certificate binds %q", h.NodeID, cn)
		}
		r.learnPeer(h.NodeID, h.Addr)
		if outer != nil {
			return outer(ctx, h)
		}
		return nil
	}
	rw := &replayStream{first: framedBytes(firstFrame), Stream: str}
	if err := internsync.Session(ctx, rw, &env, false); err != nil {
		r.cfg.Logger.Debug("replication: inbound sync failed", "remote", conn.RemoteAddr().String(), "err", err)
	}
}

func (r *Replicator) serveSeed(ctx context.Context, conn *quicgo.Conn, str *quicgo.Stream, req protocol.SeedRequest) {
	src := &seed.Source{
		DB: r.db, Namespace: r.cfg.Namespace.String(),
		SchemaVersion: r.cfg.SchemaVersion, Tables: r.seedTables, ChunkRows: r.cfg.SeedChunkRows,
	}
	// Serialize snapshot+pin with GC; streaming then runs concurrently.
	r.seedMu.Lock()
	snap, err := src.Prepare(ctx, str, certNodeID(conn), req)
	r.seedMu.Unlock()
	if err != nil {
		r.cfg.Logger.Warn("replication: seed prepare failed", "joining", req.JoiningNodeID, "err", err)
		return
	}
	if err := src.Stream(ctx, str, snap); err != nil {
		r.cfg.Logger.Warn("replication: seed stream failed", "joining", req.JoiningNodeID, "err", err)
	}
}

// --- helpers ---

func (r *Replicator) peerCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.peers)
}

func (r *Replicator) nodeIDForAddr(addr string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byAddr[addr]
}

func (r *Replicator) logEmpty(ctx context.Context) bool {
	var n int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `replication_log`").Scan(&n); err != nil {
		return false
	}
	return n == 0
}

func readFrameTimeout(ctx context.Context, r io.Reader, d time.Duration) ([]byte, error) {
	type result struct {
		b   []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		b, err := protocol.ReadFrame(r)
		ch <- result{b, err}
	}()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
		return nil, fmt.Errorf("replication: session open timed out")
	case res := <-ch:
		return res.b, res.err
	}
}

// replayStream re-serves one already-consumed frame before the live stream.
type replayStream struct {
	first []byte
	off   int
	*quicgo.Stream
}

func (s *replayStream) Read(p []byte) (int, error) {
	if s.off < len(s.first) {
		n := copy(p, s.first[s.off:])
		s.off += n
		return n, nil
	}
	return s.Stream.Read(p)
}

func framedBytes(payload []byte) []byte {
	out := make([]byte, 4+len(payload))
	n := len(payload)
	out[0] = byte(n >> 24)
	out[1] = byte(n >> 16)
	out[2] = byte(n >> 8)
	out[3] = byte(n)
	copy(out[4:], payload)
	return out
}
