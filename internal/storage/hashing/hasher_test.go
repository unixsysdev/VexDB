package hashing

import (
	"testing"

	"go.uber.org/zap"
	"vxdb/internal/metrics"
	"vxdb/internal/types"
)

type mapConfig map[string]interface{}

func (m mapConfig) Validate() error                    { return nil }
func (m mapConfig) Get(key string) (interface{}, bool) { v, ok := m[key]; return v, ok }
func (m mapConfig) GetMetricsConfig() interface{}      { return nil }

func newStorageMetrics() *metrics.StorageMetrics {
	m, _ := metrics.NewMetrics(nil)
	sm := metrics.NewStorageMetrics(m, "test")
	sm.Errors = m.NewCounter("errors_total", "", []string{"component", "error_type"})
	sm.HashingOperations = m.NewCounter("hash_ops", "", []string{"component", "operation"})
	sm.CacheHits = m.NewCounter("cache_hits", "", []string{"storage", "cache_type"})
	sm.CacheMisses = m.NewCounter("cache_miss", "", []string{"storage", "cache_type"})
	return sm
}

func newTestHasher(t *testing.T, cfg map[string]interface{}) *Hasher {
	t.Helper()
	logger := zap.NewNop()
	sm := newStorageMetrics()
	hasher, err := NewHasher(mapConfig{"hasher": cfg}, *logger, sm)
	if err != nil {
		t.Fatalf("failed to create hasher: %v", err)
	}
	return hasher
}

func TestNewHasherConfigValidation(t *testing.T) {
	logger := zap.NewNop()
	sm := newStorageMetrics()

	_, err := NewHasher(mapConfig{"hasher": map[string]interface{}{"algorithm": "bogus"}}, *logger, sm)
	if err == nil {
		t.Fatal("expected error for invalid algorithm")
	}

	_, err = NewHasher(mapConfig{"hasher": map[string]interface{}{"strategy": "bogus"}}, *logger, sm)
	if err == nil {
		t.Fatal("expected error for invalid strategy")
	}

	_, err = NewHasher(mapConfig{"hasher": map[string]interface{}{"cluster_count": 0}}, *logger, sm)
	if err == nil {
		t.Fatal("expected error for zero cluster count")
	}
}

func TestHashVectorDeterministic(t *testing.T) {
	h := newTestHasher(t, map[string]interface{}{"cluster_count": 16})
	v, err := types.NewVector("1", []float32{0.1, 0.2}, nil)
	if err != nil {
		t.Fatalf("failed to create vector: %v", err)
	}
	hash1, err := h.HashVector(v)
	if err != nil {
		t.Fatalf("hash failed: %v", err)
	}
	hash2, err := h.HashVector(v)
	if err != nil {
		t.Fatalf("hash failed: %v", err)
	}
	if hash1 != hash2 {
		t.Fatalf("expected deterministic hash, got %d and %d", hash1, hash2)
	}
}

func TestAssignClusterRange(t *testing.T) {
	h := newTestHasher(t, map[string]interface{}{"cluster_count": 8})
	v, err := types.NewVector("1", []float32{1.0, 2.0, 3.0}, nil)
	if err != nil {
		t.Fatalf("failed to create vector: %v", err)
	}
	cluster, err := h.AssignCluster(v)
	if err != nil {
		t.Fatalf("assign cluster failed: %v", err)
	}
	if cluster >= 8 {
		t.Fatalf("cluster %d out of range", cluster)
	}
}
