package search

import (
	"context"
	"testing"

	"vxdb/internal/metrics"
	"vxdb/internal/types"

	"go.uber.org/zap"
)

// helper to create IVFIndex with default Go engine
func newTestIVFIndex(t *testing.T) *IVFIndex {
	t.Helper()
	m, _ := metrics.NewMetrics(nil)
	sm := metrics.NewStorageMetrics(m, "test")
	idx, err := NewIVFIndex(nil, zap.NewNop(), sm, nil)
	if err != nil {
		t.Fatalf("NewIVFIndex error: %v", err)
	}
	return idx
}

func TestIVFBuildAndSearch(t *testing.T) {
	ivf := newTestIVFIndex(t)
	ivf.config.NumLists = 3
	ivf.config.NProbe = 1

	vectors := []*types.Vector{
		{ID: "a1", Data: []float32{0, 0}},
		{ID: "b1", Data: []float32{10, 10}},
		{ID: "c1", Data: []float32{-10, -10}},
		{ID: "a2", Data: []float32{0.1, 0}},
		{ID: "b2", Data: []float32{9.9, 10}},
		{ID: "c2", Data: []float32{-9.9, -10}},
	}

	if err := ivf.Build(vectors); err != nil {
		t.Fatalf("Build error: %v", err)
	}

	q := &SearchQuery{
		QueryVector:    &types.Vector{Data: []float32{0, 0}},
		DistanceMetric: DistanceEuclidean,
		Limit:          2,
		IncludeVector:  true,
	}
	results, err := ivf.Search(context.Background(), q)
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Vector.ID != "a1" && results[0].Vector.ID != "a2" {
		t.Fatalf("unexpected first result: %+v", results[0])
	}
}

// mock engine to verify index modularity

type mockIVFEngine struct {
	built    bool
	searched bool
}

func (m *mockIVFEngine) Build(v []*types.Vector, cfg *IVFConfig) error {
	m.built = true
	return nil
}

func (m *mockIVFEngine) Search(ctx context.Context, q *SearchQuery, cfg *IVFConfig) ([]*SearchResult, error) {
	m.searched = true
	return []*SearchResult{}, nil
}

func (m *mockIVFEngine) Add(v []*types.Vector) error { return nil }
func (m *mockIVFEngine) Delete(ids []string) error   { return nil }
func (m *mockIVFEngine) Clear() error                { return nil }
func (m *mockIVFEngine) Stats() *IndexStats {
	return &IndexStats{Type: IndexTypeIVF, Status: IndexStatusReady}
}

func TestIVFIndexCustomEngine(t *testing.T) {
	eng := &mockIVFEngine{}
	idx, err := NewIVFIndex(nil, zap.NewNop(), nil, eng)
	if err != nil {
		t.Fatalf("NewIVFIndex error: %v", err)
	}
	if err := idx.Build([]*types.Vector{}); err != nil {
		t.Fatalf("Build error: %v", err)
	}
	if !eng.built {
		t.Fatalf("custom engine Build not called")
	}
	q := &SearchQuery{QueryVector: &types.Vector{Data: []float32{0}}, DistanceMetric: DistanceEuclidean}
	if _, err := idx.Search(context.Background(), q); err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if !eng.searched {
		t.Fatalf("custom engine Search not called")
	}
}
