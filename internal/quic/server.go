package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

// Handlers dispatches the three traffic classes of one accepted connection.
// Each callback runs in its own goroutine and must honor ctx cancellation.
// This mirrors Corrosion's per-connection task tree: a datagram (FOCA)
// handler, a uni-stream (broadcast) acceptor, and a bidi-stream (sync)
// acceptor.
type Handlers struct {
	// Authenticate vets an accepted connection before handlers start
	// (certificate-to-node binding lives here). Nil accepts all.
	Authenticate func(conn *quicgo.Conn) error
	// OnDatagram handles one membership datagram.
	OnDatagram func(ctx context.Context, conn *quicgo.Conn, data []byte)
	// OnUni handles one inbound unidirectional stream (broadcast).
	// The handler owns reading; the stream is already accepted.
	OnUni func(ctx context.Context, conn *quicgo.Conn, str *quicgo.ReceiveStream)
	// OnBi handles one inbound bidirectional stream (sync/seed session).
	OnBi func(ctx context.Context, conn *quicgo.Conn, str *quicgo.Stream)
}

// Server is a QUIC listener with Corrosion-style connection handling.
type Server struct {
	listener *quicgo.Listener
	log      Logger

	mu    sync.Mutex
	conns map[*quicgo.Conn]struct{}
	wg    sync.WaitGroup
}

// Listen starts a QUIC listener on addr with the given TLS credentials.
func Listen(addr string, baseTLS *tls.Config, alpn string, quicConf *quicgo.Config, log Logger) (*Server, error) {
	if log == nil {
		log = noopLogger{}
	}
	l, err := quicgo.ListenAddr(addr, EnsureALPN(baseTLS, alpn), quicConf)
	if err != nil {
		return nil, fmt.Errorf("quic: listen %s: %w", addr, err)
	}
	return &Server{listener: l, log: log, conns: map[*quicgo.Conn]struct{}{}}, nil
}

// Addr returns the bound address.
func (s *Server) Addr() net.Addr { return s.listener.Addr() }

// Serve accepts connections until ctx ends, then shuts down gracefully:
// refuse new handshakes, let handlers drain briefly, then close.
func (s *Server) Serve(ctx context.Context, h Handlers) error {
	s.log.Info("quic listener serving", "addr", s.listener.Addr().String())
	go func() {
		<-ctx.Done()
		_ = s.listener.Close()
	}()
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			s.log.Debug("quic accept failed", "err", err)
			continue
		}
		if h.Authenticate != nil {
			if err := h.Authenticate(conn); err != nil {
				s.log.Warn("quic connection refused", "remote", conn.RemoteAddr().String(), "err", err)
				_ = conn.CloseWithError(1, "unauthorized")
				continue
			}
		}
		s.track(conn)
		s.spawnConnHandlers(ctx, conn, h)
	}
	s.drain()
	return nil
}

func (s *Server) track(conn *quicgo.Conn) {
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrack(conn *quicgo.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

// spawnConnHandlers starts the three per-connection loops.
func (s *Server) spawnConnHandlers(ctx context.Context, conn *quicgo.Conn, h Handlers) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.untrack(conn)
		remote := conn.RemoteAddr().String()
		s.log.Debug("quic connection accepted", "remote", remote)
		var inner sync.WaitGroup
		inner.Add(3)
		go func() { defer inner.Done(); s.datagramLoop(ctx, conn, h) }()
		go func() { defer inner.Done(); s.uniLoop(ctx, conn, h) }()
		go func() { defer inner.Done(); s.biLoop(ctx, conn, h) }()
		inner.Wait()
		s.log.Debug("quic connection handlers done", "remote", remote)
	}()
}

// datagramLoop forwards membership datagrams (Corrosion: spawn_foca_handler).
func (s *Server) datagramLoop(ctx context.Context, conn *quicgo.Conn, h Handlers) {
	for {
		data, err := conn.ReceiveDatagram(ctx)
		if err != nil {
			return // connection closed or shutdown
		}
		if h.OnDatagram != nil {
			h.OnDatagram(ctx, conn, data)
		}
	}
}

// uniLoop accepts broadcast streams (Corrosion: spawn_unipayload_handler).
func (s *Server) uniLoop(ctx context.Context, conn *quicgo.Conn, h Handlers) {
	for {
		str, err := conn.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		if h.OnUni == nil {
			str.CancelRead(0)
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			h.OnUni(ctx, conn, str)
		}()
	}
}

// biLoop accepts sync/seed sessions (Corrosion: spawn_bipayload_handler).
func (s *Server) biLoop(ctx context.Context, conn *quicgo.Conn, h Handlers) {
	for {
		str, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		if h.OnBi == nil {
			_ = str.Close()
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			h.OnBi(ctx, conn, str)
		}()
	}
}

// drain waits briefly for handlers, then closes remaining connections.
func (s *Server) drain() {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	s.mu.Lock()
	conns := make([]*quicgo.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.CloseWithError(0, "shutting down")
	}
}

// Close stops the listener immediately.
func (s *Server) Close() error { return s.listener.Close() }
