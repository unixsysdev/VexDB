package types

import (
	"fmt"
	"time"
)

// Timestamp represents a timestamp in the system
type Timestamp int64

// NowTimestamp returns the current timestamp
func NowTimestamp() Timestamp {
	return Timestamp(time.Now().Unix())
}

// Time converts the timestamp to time.Time
func (t Timestamp) Time() time.Time {
	return time.Unix(int64(t), 0)
}

// String returns the string representation of the timestamp
func (t Timestamp) String() string {
	return t.Time().String()
}

// SearchResult represents a single search result
type SearchResult struct {
	Vector   *Vector                `json:"vector" yaml:"vector"`
	Distance float64                `json:"distance" yaml:"distance"`
	Score    float32                `json:"score,omitempty" yaml:"score,omitempty"`
	Rank     int                    `json:"rank,omitempty" yaml:"rank,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	NodeID   string                 `json:"node_id,omitempty" yaml:"node_id,omitempty"`
}

// SearchResults represents a collection of search results
type SearchResults struct {
	Results []*SearchResult `json:"results" yaml:"results"`
	Total   int             `json:"total" yaml:"total"`
	Query   *Vector         `json:"query,omitempty" yaml:"query,omitempty"`
	K       int             `json:"k" yaml:"k"`
}

// SortSearchResults sorts search results by distance (ascending)
func SortSearchResults(results []*SearchResult) {
	// Simple bubble sort for now - in practice, use a more efficient algorithm
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i].Distance > results[j].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

// SearchStatus represents the status of a search operation
type SearchStatus struct {
	TotalSearches  int64     `json:"total_searches" yaml:"total_searches"`
	FailedSearches int64     `json:"failed_searches" yaml:"failed_searches"`
	AvgLatency     float64   `json:"avg_latency" yaml:"avg_latency"` // in milliseconds
	LastSearch     time.Time `json:"last_search" yaml:"last_search"`
}

// LinearSearchStatus represents the status of linear search
type LinearSearchStatus struct {
	Started      bool      `json:"started" yaml:"started"`
	TotalVectors int64     `json:"total_vectors" yaml:"total_vectors"`
	Searches     int64     `json:"searches" yaml:"searches"`
	AvgLatency   float64   `json:"avg_latency" yaml:"avg_latency"` // in milliseconds
	LastSearch   time.Time `json:"last_search" yaml:"last_search"`
}

// ParallelSearchStatus represents the status of parallel search
type ParallelSearchStatus struct {
	Started    bool                `json:"started" yaml:"started"`
	Linear     *LinearSearchStatus `json:"linear" yaml:"linear"`
	Workers    int                 `json:"workers" yaml:"workers"`
	MaxWorkers int                 `json:"max_workers" yaml:"max_workers"`
}

// DualSearchStatus represents the status of dual search
type DualSearchStatus struct {
	Started  bool                  `json:"started" yaml:"started"`
	Linear   *LinearSearchStatus   `json:"linear" yaml:"linear"`
	Parallel *ParallelSearchStatus `json:"parallel" yaml:"parallel"`
}

// EngineStatus represents the status of the search engine
type EngineStatus struct {
	Started       bool              `json:"started" yaml:"started"`
	Dual          *DualSearchStatus `json:"dual" yaml:"dual"`
	TotalVectors  int64             `json:"total_vectors" yaml:"total_vectors"`
	TotalSegments int64             `json:"total_segments" yaml:"total_segments"`
	Searches      int64             `json:"searches" yaml:"searches"`
	AvgLatency    float64           `json:"avg_latency" yaml:"avg_latency"` // in milliseconds
	LastSearch    time.Time         `json:"last_search" yaml:"last_search"`
}

// SegmentStatus represents the status of a segment
type SegmentStatus struct {
	ID          string    `json:"id" yaml:"id"`
	ClusterID   uint32    `json:"cluster_id" yaml:"cluster_id"`
	Status      string    `json:"status" yaml:"status"` // "active", "sealed", "compacting", "deleted"
	VectorCount uint64    `json:"vector_count" yaml:"vector_count"`
	CreatedAt   time.Time `json:"created_at" yaml:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" yaml:"updated_at"`
}

// SegmentManagerStatus represents the status of the segment manager
type SegmentManagerStatus struct {
	TotalSegments int                       `json:"total_segments" yaml:"total_segments"`
	Segments      map[string]*SegmentStatus `json:"segments" yaml:"segments"`
}

// GenerateSegmentID creates a simple segment ID from cluster and timestamp
func GenerateSegmentID(clusterID uint32) string {
	return fmt.Sprintf("%d-%d", clusterID, time.Now().UnixNano())
}

// SegmentInfo represents information about a segment
type SegmentInfo struct {
	ID          string    `json:"id" yaml:"id"`
	ClusterID   uint32    `json:"cluster_id" yaml:"cluster_id"`
	Status      string    `json:"status" yaml:"status"`
	VectorCount uint64    `json:"vector_count" yaml:"vector_count"`
	CreatedAt   time.Time `json:"created_at" yaml:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" yaml:"updated_at"`
}

// ClusterIDString returns a string representation of the cluster ID
func (si *SegmentInfo) ClusterIDString() string {
	return string(si.ClusterID)
}
