package search

import (
	"context"
	"testing"

	"vxdb/internal/metrics"
	"vxdb/internal/types"

	"go.uber.org/zap"
)

type mapConfig map[string]interface{}

func (m mapConfig) Validate() error                    { return nil }
func (m mapConfig) Get(key string) (interface{}, bool) { v, ok := m[key]; return v, ok }
func (m mapConfig) GetMetricsConfig() interface{}      { return nil }

func TestAddVectorsAutoBuildsIVF(t *testing.T) {
	cfg := mapConfig{
		"search": map[string]interface{}{
			"default_index_type": "ivf",
		},
	}
	m, _ := metrics.NewMetrics(nil)
	sm := metrics.NewStorageMetrics(m, "test")
	eng, err := NewSearchEngine(cfg, zap.NewNop(), sm)
	if err != nil {
		t.Fatalf("NewSearchEngine: %v", err)
	}
	vectors := []*types.Vector{{ID: "v1", Data: []float32{0, 0}}, {ID: "v2", Data: []float32{1, 1}}}
	if err := eng.AddVectors(vectors); err != nil {
		t.Fatalf("AddVectors: %v", err)
	}
	res, err := eng.Search(context.Background(), &SearchQuery{QueryVector: &types.Vector{Data: []float32{0, 0}}, Limit: 1})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res) == 0 {
		t.Fatalf("expected results, got none")
	}
}
