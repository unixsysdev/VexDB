package segment

import (
	"strings"
	"testing"
)

func TestIDToU64Determinism(t *testing.T) {
	id := "cluster-42"
	first := idToU64(id)
	second := idToU64(id)
	if first != second {
		t.Fatalf("expected deterministic hash, got %d and %d", first, second)
	}

	other := idToU64("cluster-99")
	if first == other {
		t.Fatalf("expected different hashes for different IDs: %d == %d", first, other)
	}
}

func TestEncodeMetadata(t *testing.T) {
	if b := encodeMetadata(nil); b != nil {
		t.Fatalf("expected nil for nil map, got %q", string(b))
	}

	m := map[string]interface{}{"foo": "bar", "num": 1}
	b := encodeMetadata(m)
	if b == nil {
		t.Fatalf("expected bytes for valid map, got nil")
	}
	s := string(b)
	if !strings.Contains(s, "\"foo\":\"bar\"") || !strings.Contains(s, "\"num\":1") {
		t.Fatalf("unexpected json output: %s", s)
	}

	invalid := map[string]interface{}{"ch": make(chan int)}
	if b := encodeMetadata(invalid); b != nil {
		t.Fatalf("expected nil for non-serializable map, got %q", string(b))
	}
}
