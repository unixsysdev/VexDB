package search

import (
	"context"
	"math"
	"testing"

	"vxdb/internal/metrics"
	"vxdb/internal/types"

	"go.uber.org/zap"
)

// helper to create LinearSearch for tests
func newTestLinearSearch(t *testing.T) *LinearSearch {
	t.Helper()
	m, _ := metrics.NewMetrics(nil)
	sm := metrics.NewStorageMetrics(m, "test")
	ls, err := NewLinearSearch(nil, zap.NewNop(), sm)
	if err != nil {
		t.Fatalf("NewLinearSearch error: %v", err)
	}
	return ls
}

func TestLinearSearchDistanceMetrics(t *testing.T) {
	ls := newTestLinearSearch(t)
	v1 := &types.Vector{ID: "a", Data: []float32{1, 0}}
	v2 := &types.Vector{ID: "b", Data: []float32{0, 1}}

	tests := []struct {
		metric   DistanceMetric
		expected float32
	}{
		{DistanceEuclidean, float32(math.Sqrt2)},
		{DistanceCosine, 1.0},
		{DistanceManhattan, 2.0},
		{DistanceDotProduct, 0.0},
		{DistanceHamming, 2.0},
		{DistanceJaccard, 1.0},
	}

	for _, tt := range tests {
		t.Run(string(tt.metric), func(t *testing.T) {
			d, err := ls.calculateDistance(v1, v2, tt.metric)
			if err != nil {
				t.Fatalf("calculateDistance error: %v", err)
			}
			if math.Abs(float64(d-tt.expected)) > 1e-5 {
				t.Fatalf("distance for %s = %v, want %v", tt.metric, d, tt.expected)
			}
		})
	}
}

func TestLinearSearchSearch(t *testing.T) {
	ls := newTestLinearSearch(t)
	vectors := []*types.Vector{
		{ID: "v1", Data: []float32{0, 0}},
		{ID: "v2", Data: []float32{1, 1}},
		{ID: "v3", Data: []float32{2, 2}},
	}
	if err := ls.Build(vectors); err != nil {
		t.Fatalf("Build error: %v", err)
	}

	q := &SearchQuery{
		QueryVector:    &types.Vector{Data: []float32{0, 0}},
		DistanceMetric: DistanceEuclidean,
		Limit:          2,
		IncludeVector:  true,
	}
	results, err := ls.Search(context.Background(), q)
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Vector.ID != "v1" || results[0].Distance != 0 {
		t.Fatalf("unexpected first result: %+v", results[0])
	}
	if results[1].Vector.ID != "v2" {
		t.Fatalf("unexpected second result: %+v", results[1])
	}
}

func TestLinearSearchThreshold(t *testing.T) {
	ls := newTestLinearSearch(t)
	vectors := []*types.Vector{
		{ID: "v1", Data: []float32{0, 0}},
		{ID: "v2", Data: []float32{1, 1}},
	}
	if err := ls.Build(vectors); err != nil {
		t.Fatalf("Build error: %v", err)
	}

	q := &SearchQuery{
		QueryVector:    &types.Vector{Data: []float32{0, 0}},
		DistanceMetric: DistanceEuclidean,
		Threshold:      0.5,
		IncludeVector:  true,
	}
	results, err := ls.Search(context.Background(), q)
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(results) != 1 || results[0].Vector.ID != "v1" {
		t.Fatalf("unexpected results: %+v", results)
	}
}
