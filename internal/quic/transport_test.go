package quic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

func leaf(t *testing.T, c tls.Certificate) *x509.Certificate {
	t.Helper()
	cert, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func bothPools(t *testing.T, a, b tls.Certificate) *x509.CertPool {
	t.Helper()
	p := x509.NewCertPool()
	p.AddCert(leaf(t, a))
	p.AddCert(leaf(t, b))
	return p
}

func TestNodeCertBinding(t *testing.T) {
	cert, pool, err := GenerateNodeCert("node-a")
	if err != nil {
		t.Fatal(err)
	}
	if pool == nil {
		t.Fatalf("nil pool")
	}
	id, err := NodeIDFromPeerCerts([]*x509.Certificate{leaf(t, cert)})
	if err != nil || id != "node-a" {
		t.Fatalf("binding = %q, %v", id, err)
	}
	if _, err := NodeIDFromPeerCerts(nil); err == nil {
		t.Fatalf("empty chain accepted")
	}
}

func TestTransportThreeChannels(t *testing.T) {
	certA, _ := mustCert(t, "node-a")
	certB, _ := mustCert(t, "node-b")
	pool := bothPools(t, certA, certB)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{certB},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{certA},
		RootCAs:      pool,
	}
	const alpn = "mariamesh-test/1"
	qconf := DefaultQUICConfig(10 * time.Second)

	srv, err := Listen("127.0.0.1:0", serverTLS, alpn, qconf, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr := srv.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	gotDatagram := make(chan []byte, 4)
	gotUni := make(chan []byte, 4)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ctx, Handlers{
			OnDatagram: func(_ context.Context, _ *quicgo.Conn, data []byte) {
				gotDatagram <- append([]byte(nil), data...)
			},
			OnUni: func(_ context.Context, _ *quicgo.Conn, str *quicgo.ReceiveStream) {
				b, err := io.ReadAll(str)
				if err != nil {
					return
				}
				gotUni <- b
			},
			OnBi: func(_ context.Context, _ *quicgo.Conn, str *quicgo.Stream) {
				defer str.Close()
				req := make([]byte, 4)
				if _, err := io.ReadFull(str, req); err != nil {
					return
				}
				_, _ = str.Write(append([]byte("pong:"), req...))
			},
		})
	}()

	tr := NewTransport(clientTLS, alpn, DefaultQUICConfig(10*time.Second), 10*time.Second, nil)
	defer tr.Close()

	// 1. Datagram (membership channel).
	if err := tr.SendDatagram(ctx, addr, "node-b", []byte("heartbeat")); err != nil {
		t.Fatalf("datagram: %v", err)
	}
	select {
	case b := <-gotDatagram:
		if string(b) != "heartbeat" {
			t.Fatalf("datagram = %q", b)
		}
	case <-ctx.Done():
		t.Fatalf("datagram not received")
	}

	// 2. Uni stream (broadcast channel).
	if err := tr.SendUni(ctx, addr, "node-b", []byte("frame-bytes")); err != nil {
		t.Fatalf("uni: %v", err)
	}
	select {
	case b := <-gotUni:
		if string(b) != "frame-bytes" {
			t.Fatalf("uni = %q", b)
		}
	case <-ctx.Done():
		t.Fatalf("uni not received")
	}

	// 3. Bidi stream (sync channel) with a response.
	str, err := tr.OpenBI(ctx, addr, "node-b")
	if err != nil {
		t.Fatalf("open bidi: %v", err)
	}
	if _, err := str.Write([]byte("ping")); err != nil {
		t.Fatalf("bidi write: %v", err)
	}
	resp := make([]byte, 9)
	if _, err := io.ReadFull(str, resp); err != nil {
		t.Fatalf("bidi read: %v", err)
	}
	_ = str.Close()
	if string(resp) != "pong:ping" {
		t.Fatalf("bidi response = %q", resp)
	}

	// 4. Connection reuse across channels + RTT sampling.
	c1, ok := tr.PeerConn(addr)
	if !ok {
		t.Fatalf("no cached connection")
	}
	str2, err := tr.OpenBI(ctx, addr, "node-b")
	if err != nil {
		t.Fatal(err)
	}
	_ = str2.Close()
	c2, ok := tr.PeerConn(addr)
	if !ok || c1 != c2 {
		t.Fatalf("connection not shared across opens")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if rtts := tr.SampleRTTs(); len(rtts) == 1 {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("no RTT sample: %v", rtts)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 5. Wrong ServerName must fail the handshake (cert binding).
	trBad := NewTransport(clientTLS, alpn, DefaultQUICConfig(10*time.Second), 3*time.Second, nil)
	defer trBad.Close()
	if err := trBad.SendDatagram(ctx, addr, "node-impostor", []byte("x")); err == nil {
		t.Fatalf("dial with wrong server name succeeded")
	}

	cancel()
	select {
	case err := <-serveErr:
		if err != nil && ctx.Err() == nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("server did not shut down")
	}
}

func mustCert(t *testing.T, nodeID string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	cert, pool, err := GenerateNodeCert(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return cert, pool
}
