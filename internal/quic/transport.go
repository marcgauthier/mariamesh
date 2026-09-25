package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

// TrafficClass labels outbound traffic for logs. Connections are shared
// across classes; the label only aids debugging.
type TrafficClass int

const (
	// TrafficSync is version exchange + delta sync (bidi streams).
	TrafficSync TrafficClass = iota
	// TrafficBroadcast is fire-and-forget change batches (uni streams).
	TrafficBroadcast
	// TrafficMembership is SWIM-style heartbeats (datagrams).
	TrafficMembership
)

func (t TrafficClass) String() string {
	switch t {
	case TrafficSync:
		return "sync"
	case TrafficBroadcast:
		return "broadcast"
	case TrafficMembership:
		return "membership"
	default:
		return "unknown"
	}
}

// Logger is a minimal structured logger to avoid depending on the root package.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Transport manages outbound QUIC connectivity the way Corrosion's
// Transport does: one cached long-lived connection per peer address,
// reused across traffic classes, with health checks and a single
// reconnect-and-retry around every send or stream open.
type Transport struct {
	mu          sync.Mutex
	conns       map[string]*peerConn
	tls         *tls.Config
	alpn        string
	quicConf    *quicgo.Config
	dialTimeout time.Duration
	log         Logger
}

type peerConn struct {
	mu   sync.Mutex
	conn *quicgo.Conn
}

// NewTransport returns an outbound transport. baseTLS supplies mTLS
// credentials; ServerName is pinned per peer at dial time.
func NewTransport(baseTLS *tls.Config, alpn string, quicConf *quicgo.Config, dialTimeout time.Duration, log Logger) *Transport {
	if log == nil {
		log = noopLogger{}
	}
	return &Transport{
		conns:       map[string]*peerConn{},
		tls:         baseTLS,
		alpn:        alpn,
		quicConf:    quicConf,
		dialTimeout: dialTimeout,
		log:         log,
	}
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// entry returns the shared connection slot for addr.
func (t *Transport) entry(addr string) *peerConn {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.conns[addr]
	if !ok {
		e = &peerConn{}
		t.conns[addr] = e
	}
	return e
}

// healthy reports whether the cached connection is still usable.
func healthy(c *quicgo.Conn) bool {
	return c != nil && c.Context().Err() == nil
}

// connect returns the cached connection for addr, dialing when absent or
// dead. peerNodeID pins TLS ServerName; empty leaves baseTLS untouched.
// Callers must hold no locks; dialing is serialized per peer.
func (t *Transport) connect(ctx context.Context, addr, peerNodeID string, class TrafficClass) (*quicgo.Conn, error) {
	e := t.entry(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	if healthy(e.conn) {
		return e.conn, nil
	}
	e.conn = nil
	dialCtx, cancel := context.WithTimeout(ctx, t.dialTimeout)
	defer cancel()
	tlsConf := ClientTLSForPeer(t.tls, t.alpn, peerNodeID)
	start := time.Now()
	conn, err := quicgo.DialAddr(dialCtx, addr, tlsConf, t.quicConf)
	if err != nil {
		return nil, fmt.Errorf("quic: dial %s (%s): %w", addr, class, err)
	}
	t.log.Debug("quic dial connected", "addr", addr, "class", class.String(), "rtt", conn.ConnectionStats().SmoothedRTT.String(), "took", time.Since(start).String())
	e.conn = conn
	return conn, nil
}

// reconnect drops the cached connection and dials afresh.
func (t *Transport) reconnect(ctx context.Context, addr, peerNodeID string, class TrafficClass) (*quicgo.Conn, error) {
	e := t.entry(addr)
	e.mu.Lock()
	if e.conn != nil {
		_ = e.conn.CloseWithError(0, "reconnecting")
		e.conn = nil
	}
	e.mu.Unlock()
	return t.connect(ctx, addr, peerNodeID, class)
}

// SendDatagram sends unreliable membership traffic, reconnecting once on
// failure. data should stay well under the path MTU (~1KB hearts beat best).
func (t *Transport) SendDatagram(ctx context.Context, addr, peerNodeID string, data []byte) error {
	conn, err := t.connect(ctx, addr, peerNodeID, TrafficMembership)
	if err != nil {
		return err
	}
	if err := conn.SendDatagram(data); err != nil {
		t.log.Debug("quic datagram failed, reconnecting", "addr", addr, "err", err)
		conn, err = t.reconnect(ctx, addr, peerNodeID, TrafficMembership)
		if err != nil {
			return err
		}
		return conn.SendDatagram(data)
	}
	return nil
}

// SendUni sends one length-delimited payload on a fresh unidirectional
// stream and closes it (fire-and-forget broadcast). It reconnects once on
// failure.
func (t *Transport) SendUni(ctx context.Context, addr, peerNodeID string, frame []byte) error {
	conn, err := t.connect(ctx, addr, peerNodeID, TrafficBroadcast)
	if err != nil {
		return err
	}
	str, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.log.Debug("quic open uni failed, reconnecting", "addr", addr, "err", err)
		conn, err = t.reconnect(ctx, addr, peerNodeID, TrafficBroadcast)
		if err != nil {
			return err
		}
		str, err = conn.OpenUniStreamSync(ctx)
		if err != nil {
			return fmt.Errorf("quic: open uni stream to %s: %w", addr, err)
		}
	}
	if _, err := str.Write(frame); err != nil {
		_ = str.Close()
		return fmt.Errorf("quic: write uni stream to %s: %w", addr, err)
	}
	return str.Close()
}

// OpenBI opens a bidirectional stream for one sync/seed session,
// reconnecting once on failure. The caller owns the stream lifetime.
func (t *Transport) OpenBI(ctx context.Context, addr, peerNodeID string) (*quicgo.Stream, error) {
	conn, err := t.connect(ctx, addr, peerNodeID, TrafficSync)
	if err != nil {
		return nil, err
	}
	str, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.log.Debug("quic open bidi failed, reconnecting", "addr", addr, "err", err)
		conn, err = t.reconnect(ctx, addr, peerNodeID, TrafficSync)
		if err != nil {
			return nil, err
		}
		str, err = conn.OpenStreamSync(ctx)
		if err != nil {
			return nil, fmt.Errorf("quic: open bidi stream to %s: %w", addr, err)
		}
	}
	return str, nil
}

// PeerConn returns the cached healthy connection for addr, if any.
func (t *Transport) PeerConn(addr string) (*quicgo.Conn, bool) {
	t.mu.Lock()
	e, ok := t.conns[addr]
	t.mu.Unlock()
	if !ok {
		return nil, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !healthy(e.conn) {
		return nil, false
	}
	return e.conn, true
}

// SampleRTTs returns the smoothed RTT of every live cached connection.
// Like Corrosion's sampler it feeds peer-health decisions in membership.
func (t *Transport) SampleRTTs() map[string]time.Duration {
	t.mu.Lock()
	addrs := make([]string, 0, len(t.conns))
	entries := make([]*peerConn, 0, len(t.conns))
	for a, e := range t.conns {
		addrs = append(addrs, a)
		entries = append(entries, e)
	}
	t.mu.Unlock()
	out := map[string]time.Duration{}
	for i, e := range entries {
		e.mu.Lock()
		if healthy(e.conn) {
			if rtt := e.conn.ConnectionStats().SmoothedRTT; rtt > 0 {
				out[addrs[i]] = rtt
			}
		}
		e.mu.Unlock()
	}
	return out
}

// PeerAddrs returns known peer addresses (healthy or not).
func (t *Transport) PeerAddrs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.conns))
	for a := range t.conns {
		out = append(out, a)
	}
	return out
}

// Drop closes and forgets the cached connection for addr.
func (t *Transport) Drop(addr string) {
	t.mu.Lock()
	e, ok := t.conns[addr]
	if ok {
		delete(t.conns, addr)
	}
	t.mu.Unlock()
	if ok {
		e.mu.Lock()
		if e.conn != nil {
			_ = e.conn.CloseWithError(0, "dropped")
			e.conn = nil
		}
		e.mu.Unlock()
	}
}

// Close shuts down every cached connection.
func (t *Transport) Close() error {
	t.mu.Lock()
	entries := make([]*peerConn, 0, len(t.conns))
	for _, e := range t.conns {
		entries = append(entries, e)
	}
	t.conns = map[string]*peerConn{}
	t.mu.Unlock()
	for _, e := range entries {
		e.mu.Lock()
		if e.conn != nil {
			_ = e.conn.CloseWithError(0, "shutdown")
			e.conn = nil
		}
		e.mu.Unlock()
	}
	return nil
}
