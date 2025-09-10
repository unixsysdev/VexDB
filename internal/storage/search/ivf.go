package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"vxdb/internal/config"
	"vxdb/internal/metrics"
	"vxdb/internal/types"

	"go.uber.org/zap"
)

var (
	ErrIVFNotBuilt = errors.New("ivf index not built")
)

// IVFConfig represents configuration for the IVF index
// NumLists controls the number of inverted lists (clusters)
// NProbe specifies how many lists to search during queries
// DistanceMetric defines the metric used for clustering and search
//
// The config is intentionally lightweight so the underlying engine can be
// swapped with CGO/FAISS based implementations without affecting callers.
type IVFConfig struct {
	NumLists       int            `yaml:"num_lists" json:"num_lists"`
	NProbe         int            `yaml:"nprobe" json:"nprobe"`
	DistanceMetric DistanceMetric `yaml:"distance_metric" json:"distance_metric"`
}

// DefaultIVFConfig returns default configuration for IVF index
func DefaultIVFConfig() *IVFConfig {
	return &IVFConfig{
		NumLists:       1,
		NProbe:         1,
		DistanceMetric: DistanceEuclidean,
	}
}

// IVFEngine defines the interface for IVF implementations. This allows the
// search index to remain agnostic about the underlying algorithm and enables
// swapping in CGO/FAISS engines later without changing call sites.
type IVFEngine interface {
	Build(vectors []*types.Vector, cfg *IVFConfig) error
	Search(ctx context.Context, query *SearchQuery, cfg *IVFConfig) ([]*SearchResult, error)
	Add(vectors []*types.Vector) error
	Delete(ids []string) error
	Clear() error
	Stats() *IndexStats
}

// IVFIndex wraps an IVFEngine and exposes the SearchIndex interface expected by
// the search engine. It delegates all heavy lifting to the configured engine.
type IVFIndex struct {
	config  *IVFConfig
	engine  IVFEngine
	status  IndexStatus
	mu      sync.RWMutex
	logger  *zap.Logger
	metrics *metrics.StorageMetrics
}

// NewIVFIndex creates a new IVF index. If engine is nil, a simple in-memory
// Go implementation is used. Callers can inject alternate engines (e.g. CGO
// FAISS) to replace the default.
func NewIVFIndex(cfg config.Config, logger *zap.Logger, metrics *metrics.StorageMetrics, engine IVFEngine) (*IVFIndex, error) {
	ivfCfg := DefaultIVFConfig()
	if cfg != nil {
		if c, ok := cfg.Get("ivf"); ok {
			if m, ok := c.(map[string]interface{}); ok {
				if nl, ok := m["num_lists"].(int); ok {
					ivfCfg.NumLists = nl
				}
				if np, ok := m["nprobe"].(int); ok {
					ivfCfg.NProbe = np
				}
				if dm, ok := m["distance_metric"].(string); ok {
					ivfCfg.DistanceMetric = DistanceMetric(dm)
				}
			}
		}
	}

	if engine == nil {
		engine = newGoIVFEngine(ivfCfg)
	}

	return &IVFIndex{
		config:  ivfCfg,
		engine:  engine,
		status:  IndexStatusNotReady,
		logger:  logger,
		metrics: metrics,
	}, nil
}

func (i *IVFIndex) GetType() IndexType { return IndexTypeIVF }

func (i *IVFIndex) GetStatus() IndexStatus { return i.status }

func (i *IVFIndex) GetConfig() *IndexConfig { return &IndexConfig{Type: IndexTypeIVF} }

func (i *IVFIndex) GetStats() *IndexStats {
	if i.engine != nil {
		return i.engine.Stats()
	}
	return &IndexStats{Type: IndexTypeIVF, Status: i.status}
}

func (i *IVFIndex) Build(vectors []*types.Vector) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.engine.Build(vectors, i.config); err != nil {
		i.status = IndexStatusError
		return err
	}
	i.status = IndexStatusReady
	return nil
}

func (i *IVFIndex) Update(vectors []*types.Vector) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.engine.Add(vectors); err != nil {
		return err
	}
	return nil
}

func (i *IVFIndex) Delete(ids []string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.engine.Delete(ids)
}

func (i *IVFIndex) Clear() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.engine.Clear(); err != nil {
		return err
	}
	i.status = IndexStatusNotReady
	return nil
}

func (i *IVFIndex) Search(ctx context.Context, query *SearchQuery) ([]*SearchResult, error) {
	if query == nil {
		return nil, ErrInvalidSearchQuery
	}
	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSearchQuery, err)
	}
	return i.engine.Search(ctx, query, i.config)
}

func (i *IVFIndex) BatchSearch(ctx context.Context, queries []*SearchQuery) ([][]*SearchResult, error) {
	results := make([][]*SearchResult, len(queries))
	for idx, q := range queries {
		r, err := i.Search(ctx, q)
		if err != nil {
			return nil, err
		}
		results[idx] = r
	}
	return results, nil
}

func (i *IVFIndex) Rebuild() error { return nil }

func (i *IVFIndex) Optimize() error { return nil }

func (i *IVFIndex) Validate() error { return nil }

func (i *IVFIndex) Start() error { return nil }

func (i *IVFIndex) Stop() error { return nil }

func (i *IVFIndex) IsReady() bool { return i.status == IndexStatusReady }

// ----------------- Default Go engine implementation --------------------

type goIVFEngine struct {
	cfg       *IVFConfig
	centroids []*types.Vector
	lists     map[int][]*types.Vector
	mu        sync.RWMutex
}

func newGoIVFEngine(cfg *IVFConfig) *goIVFEngine {
	return &goIVFEngine{
		cfg:   cfg,
		lists: make(map[int][]*types.Vector),
	}
}

func (e *goIVFEngine) Build(vectors []*types.Vector, cfg *IVFConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
	if cfg.NumLists <= 0 {
		cfg.NumLists = 1
	}
	k := cfg.NumLists
	if len(vectors) < k {
		k = len(vectors)
	}
	e.centroids = make([]*types.Vector, k)
	copy(e.centroids, vectors[:k])
	e.lists = make(map[int][]*types.Vector, k)
	for _, v := range vectors {
		idx := e.assign(v)
		e.lists[idx] = append(e.lists[idx], v)
	}
	return nil
}

func (e *goIVFEngine) assign(v *types.Vector) int {
	best := 0
	bestDist := math.MaxFloat64
	for i, c := range e.centroids {
		d, _ := v.Distance(c, string(e.cfg.DistanceMetric))
		if d < bestDist {
			bestDist = d
			best = i
		}
	}
	v.ClusterID = uint32(best)
	return best
}

func (e *goIVFEngine) Search(ctx context.Context, query *SearchQuery, cfg *IVFConfig) ([]*SearchResult, error) {
	e.mu.RLock()
	centroids := e.centroids
	lists := e.lists
	e.mu.RUnlock()

	if len(centroids) == 0 {
		return nil, ErrIVFNotBuilt
	}

	type cd struct {
		idx  int
		dist float64
	}
	cds := make([]cd, len(centroids))
	metric := cfg.DistanceMetric
	if query.DistanceMetric != "" {
		metric = query.DistanceMetric
	}
	for i, c := range centroids {
		d, err := query.QueryVector.Distance(c, string(metric))
		if err != nil {
			return nil, err
		}
		cds[i] = cd{i, d}
	}
	sort.Slice(cds, func(i, j int) bool { return cds[i].dist < cds[j].dist })
	nprobe := cfg.NProbe
	if nprobe <= 0 || nprobe > len(cds) {
		nprobe = len(cds)
	}
	candidates := make([]*types.Vector, 0)
	for i := 0; i < nprobe; i++ {
		candidates = append(candidates, lists[cds[i].idx]...)
	}

	results := make([]*SearchResult, 0, len(candidates))
	for _, v := range candidates {
		d, err := query.QueryVector.Distance(v, string(metric))
		if err != nil {
			return nil, err
		}
		if query.Threshold > 0 && float32(d) > query.Threshold {
			continue
		}
		res := &SearchResult{Vector: v, Distance: d}
		results = append(results, res)
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Distance < results[j].Distance })
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}
	return results, nil
}

func (e *goIVFEngine) Add(vectors []*types.Vector) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, v := range vectors {
		idx := e.assign(v)
		e.lists[idx] = append(e.lists[idx], v)
	}
	return nil
}

func (e *goIVFEngine) Delete(ids []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	for idx, list := range e.lists {
		filtered := list[:0]
		for _, v := range list {
			if _, ok := idSet[v.ID]; !ok {
				filtered = append(filtered, v)
			}
		}
		e.lists[idx] = filtered
	}
	return nil
}

func (e *goIVFEngine) Clear() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.centroids = nil
	e.lists = make(map[int][]*types.Vector)
	return nil
}

func (e *goIVFEngine) Stats() *IndexStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var count int64
	for _, l := range e.lists {
		count += int64(len(l))
	}
	return &IndexStats{Type: IndexTypeIVF, Status: IndexStatusReady, VectorsCount: count}
}
