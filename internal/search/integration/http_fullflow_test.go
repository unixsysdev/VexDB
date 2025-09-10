package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"vxdb/internal/search/api"
	"vxdb/internal/search/config"
	"vxdb/internal/search/response"
	"vxdb/internal/types"
)

type inMemorySearchService struct {
	mu      sync.Mutex
	vectors []*types.Vector
}

func (s *inMemorySearchService) Search(ctx context.Context, q *types.Vector, k int, filter map[string]string) ([]*types.SearchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.vectors) == 0 {
		return nil, nil
	}
	best := s.vectors[0]
	bestDist, _ := q.Distance(best, "euclidean")
	for _, v := range s.vectors[1:] {
		d, _ := q.Distance(v, "euclidean")
		if d < bestDist {
			bestDist = d
			best = v
		}
	}
	return []*types.SearchResult{{Vector: best, Distance: bestDist}}, nil
}

func (s *inMemorySearchService) MultiClusterSearch(ctx context.Context, q *types.Vector, k int, clusters []string, filter map[string]string) ([]*types.SearchResult, error) {
	return s.Search(ctx, q, k, filter)
}
func (s *inMemorySearchService) GetClusterInfo(ctx context.Context, id string) (*types.ClusterInfo, error) {
	return nil, nil
}
func (s *inMemorySearchService) GetClusterStatus(ctx context.Context) (*types.ClusterStatus, error) {
	return nil, nil
}
func (s *inMemorySearchService) HealthCheck(ctx context.Context, detailed bool) (*types.HealthStatus, error) {
	return &types.HealthStatus{Healthy: true}, nil
}
func (s *inMemorySearchService) GetMetrics(ctx context.Context, metricType, clusterID, nodeID string) (map[string]float64, error) {
	return nil, nil
}
func (s *inMemorySearchService) UpdateConfig(ctx context.Context, cfg map[string]string) error {
	return nil
}
func (s *inMemorySearchService) GetConfig(ctx context.Context) (map[string]string, error) {
	return nil, nil
}

// TestHTTPSearchFlow verifies the HTTP search handler with a real service implementation.
func TestHTTPSearchFlow(t *testing.T) {
	svc := &inMemorySearchService{vectors: []*types.Vector{{ID: "v1", Data: []float32{0.1, 0.2}}}}
	cfg := config.DefaultSearchServiceConfig()
	logger := zap.NewNop()
	handler := api.NewSearchHandler(cfg, logger, nil, svc)

	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	ts := httptest.NewServer(router)
	defer ts.Close()

	body := map[string]any{"vector": []float64{0.1, 0.2}, "k": 1}
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(body)
	resp, err := http.Post(ts.URL+"/search", "application/json", buf)
	if err != nil {
		t.Fatalf("post search: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var sr response.SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(sr.Results) != 1 || sr.Results[0].ID != "v1" {
		t.Fatalf("unexpected result: %+v", sr.Results)
	}

	simBody := map[string]any{"vector": []float64{0.1, 0.2}, "k": 1, "distance_threshold": 1.0}
	buf2 := new(bytes.Buffer)
	json.NewEncoder(buf2).Encode(simBody)
	resp2, err := http.Post(ts.URL+"/search/similarity", "application/json", buf2)
	if err != nil {
		t.Fatalf("post similarity: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("sim status: %d", resp2.StatusCode)
	}
	var sr2 response.SearchResponse
	if err := json.NewDecoder(resp2.Body).Decode(&sr2); err != nil {
		t.Fatalf("decode sim: %v", err)
	}
	if len(sr2.Results) != 1 || sr2.Results[0].ID != "v1" {
		t.Fatalf("unexpected sim result: %+v", sr2.Results)
	}
}
