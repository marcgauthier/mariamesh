package protocol

import (
	"bytes"
	"strings"
	"testing"

	"github.com/mariamesh/mariamesh/internal/changelog"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payloads := [][]byte{[]byte("hello"), {}, bytes.Repeat([]byte("x"), 1<<16)}
	for _, p := range payloads {
		if err := WriteFrame(&buf, p); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range payloads {
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame mismatch len %d vs %d", len(got), len(want))
		}
	}
}

func TestFrameTooLarge(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, make([]byte, MaxFrameBytes+1)); err == nil {
		t.Fatalf("oversize write accepted")
	}
	// Hand-crafted oversize header must be rejected on read.
	buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	if _, err := ReadFrame(&buf); err == nil {
		t.Fatalf("oversize read accepted")
	}
}

func TestEnvelopeVersionAndNamespace(t *testing.T) {
	raw, err := Encode("ns1", KindHello, Hello{NodeID: "n"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Decode(raw, "ns1")
	if err != nil {
		t.Fatal(err)
	}
	var h Hello
	if err := DecodeBody(e, &h); err != nil || h.NodeID != "n" {
		t.Fatalf("body decode = %+v, %v", h, err)
	}
	if _, err := Decode(raw, "other"); err == nil {
		t.Fatalf("wrong namespace accepted")
	}
	raw2, _ := Encode("ns1", KindAck, Ack{Vector: changelog.Vector{"a:i": 3}})
	e2, err := Decode(raw2, "ns1")
	if err != nil {
		t.Fatal(err)
	}
	if e2.Kind != KindAck {
		t.Fatalf("kind = %q", e2.Kind)
	}
	if _, err := Decode([]byte("{bogus"), "ns1"); err == nil {
		t.Fatalf("garbage frame accepted")
	}
}

func TestHelloVectorJSON(t *testing.T) {
	raw, err := Encode("ns", KindHello, Hello{Vector: changelog.Vector{"a:i": 12882}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "12882") {
		t.Fatalf("vector missing from hello: %s", raw)
	}
}
