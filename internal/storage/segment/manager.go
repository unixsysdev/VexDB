// Package segment provides segment management functionality for VexDB
//go:build ignore

package segment

import (
	"context"
	"sync"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/storage/compression"
	"vexdb/internal/types"

	"go.uber.org/zap"
)

// Manager manages all segments in the storage system
type Manager struct {
	config      *config.Config
	logger      *zap.Logger
	metrics     *metrics.Collector
	compressor  *compression.Compressor
	
	segments    map[string]*Segment
	manifest    *Manifest
	
	mu          sync.RWMutex
	started     bool
}

// NewManager creates a new segment manager
func NewManager(cfg *config.Config, logger *zap.Logger, metrics *metrics.Collector, compressor *compression.Compressor) (*Manager, error) {
	m := &Manager{
		config:     cfg,
		logger:     logger,
		metrics:    metrics,
		compressor: compressor,
		segments:   make(map[string]*Segment),
	}

	// Load or create manifest
	if err := m.initializeManifest(); err != nil {
		return nil, err
	}

	return m, nil
}

// initializeManifest initializes the segment manifest
func (m *Manager) initializeManifest() error {
	m.manifest = NewManifest(m.config, m.logger, m.metrics)
	
	// Load existing segments from manifest
	if err := m.manifest.Load(); err != nil {
		m.logger.Warn("Failed to load manifest, creating new one", zap.Error(err))
		// Create new manifest
		if err := m.manifest.Create(); err != nil {
			return err
		}
	}

	return nil
}

// Start starts the segment manager
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return nil
	}

	m.logger.Info("Starting segment manager")

	// Load all segments from the manifest
	segmentInfos := m.manifest.GetAllSegments()
	for _, info := range segmentInfos {
		segment, err := NewSegment(info, m.config, m.logger, m.metrics, m.compressor)
		if err != nil {
			m.logger.Error("Failed to create segment", zap.String("segment_id", info.ID), zap.Error(err))
			continue
		}
		
		m.segments[info.ID] = segment
	}

	m.started = true
	m.logger.Info("Segment manager started successfully")
	return nil
}

// Stop stops the segment manager
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return nil
	}

	m.logger.Info("Stopping segment manager")

	// Stop all segments
	for id, segment := range m.segments {
		if err := segment.Close(); err != nil {
			m.logger.Error("Failed to close segment", zap.String("segment_id", id), zap.Error(err))
		}
	}

	// Save manifest
	if err := m.manifest.Save(); err != nil {
		m.logger.Error("Failed to save manifest", zap.Error(err))
	}

	m.segments = make(map[string]*Segment)
	m.started = false
	m.logger.Info("Segment manager stopped successfully")
	return nil
}

// CreateSegment creates a new segment
func (m *Manager) CreateSegment(clusterID uint32) (*Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return nil, types.ErrStorageNotStarted
	}

	// Generate segment ID
	segmentID := generateSegmentID(clusterID)
	
	// Create segment info
	info := &SegmentInfo{
		ID:        segmentID,
		ClusterID: clusterID,
		Status:    SegmentStatusActive,
	}

	// Create segment
	segment, err := NewSegment(info, m.config, m.logger, m.metrics, m.compressor)
	if err != nil {
		return nil, err
	}

	// Add to segments map
	m.segments[segmentID] = segment

	// Add to manifest
	if err := m.manifest.AddSegment(info); err != nil {
		// Rollback
		delete(m.segments, segmentID)
		segment.Close()
		return nil, err
	}

	return segment, nil
}

// GetSegment retrieves a segment by ID
func (m *Manager) GetSegment(segmentID string) (*Segment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.started {
		return nil, types.ErrStorageNotStarted
	}

	segment, exists := m.segments[segmentID]
	if !exists {
		return nil, types.ErrSegmentNotFound
	}

	return segment, nil
}

// GetSegmentsByCluster retrieves all segments for a specific cluster
func (m *Manager) GetSegmentsByCluster(clusterID uint32) []*Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var segments []*Segment
	for _, segment := range m.segments {
		if segment.GetClusterID() == clusterID {
			segments = append(segments, segment)
		}
	}

	return segments
}

// GetAllSegments returns all segments
func (m *Manager) GetAllSegments() []*Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()

	segments := make([]*Segment, 0, len(m.segments))
	for _, segment := range m.segments {
		segments = append(segments, segment)
	}

	return segments
}

// RemoveSegment removes a segment
func (m *Manager) RemoveSegment(segmentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return types.ErrStorageNotStarted
	}

	segment, exists := m.segments[segmentID]
	if !exists {
		return types.ErrSegmentNotFound
	}

	// Close segment
	if err := segment.Close(); err != nil {
		return err
	}

	// Remove from segments map
	delete(m.segments, segmentID)

	// Remove from manifest
	if err := m.manifest.RemoveSegment(segmentID); err != nil {
		m.logger.Error("Failed to remove segment from manifest", zap.String("segment_id", segmentID), zap.Error(err))
	}

	return nil
}

// GetStatus returns the current status of the segment manager
func (m *Manager) GetStatus() *types.SegmentManagerStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := &types.SegmentManagerStatus{
		TotalSegments: len(m.segments),
		Segments:      make(map[string]*types.SegmentStatus),
	}

	for id, segment := range m.segments {
		status.Segments[id] = segment.GetStatus()
	}

	return status
}

// generateSegmentID generates a unique segment ID for a cluster
func generateSegmentID(clusterID uint32) string {
	return types.GenerateSegmentID(clusterID)
}
