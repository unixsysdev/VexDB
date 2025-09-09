//go:build ignore

package segment

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrManifestNotFound    = errors.New("manifest not found")
	ErrManifestCorrupted   = errors.New("manifest is corrupted")
	ErrSegmentAlreadyAdded = errors.New("segment already added")
	ErrSegmentNotFound     = errors.New("segment not found")
	ErrInvalidManifest     = errors.New("invalid manifest")
	ErrSaveFailed          = errors.New("failed to save manifest")
	ErrLoadFailed          = errors.New("failed to load manifest")
)

const (
	ManifestMagic    = 0x4D414E46 // "MANF" in hex
	ManifestVersion  = 1
	ManifestFileName = "manifest.vex"
)

// ManifestEntry represents an entry in the segment manifest
type ManifestEntry struct {
	SegmentID     uint64            `json:"segment_id"`
	ClusterID     uint32            `json:"cluster_id"`
	VectorDim     uint32            `json:"vector_dim"`
	VectorCount   uint32            `json:"vector_count"`
	CreatedAt     int64             `json:"created_at"`
	ModifiedAt    int64             `json:"modified_at"`
	Size          int64             `json:"size"`
	Path          string            `json:"path"`
	Status        string            `json:"status"` // active, readonly, compacting, deleted
	Checksum      uint32            `json:"checksum"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// Manifest represents the segment manifest
type Manifest struct {
	magic        uint32
	version      uint32
	createdAt    int64
	modifiedAt   int64
	clusterID    uint32
	vectorDim    uint32
	entries      map[uint64]*ManifestEntry // segment_id -> entry
	path         string
	mu           sync.RWMutex
	dirty        bool
	logger       logging.Logger
	metrics      *metrics.StorageMetrics
}

// NewManifest creates a new manifest
func NewManifest(path string, clusterID uint32, vectorDim uint32, logger logging.Logger, metrics *metrics.StorageMetrics) *Manifest {
	now := time.Now().Unix()
	
	return &Manifest{
		magic:      ManifestMagic,
		version:    ManifestVersion,
		createdAt:  now,
		modifiedAt: now,
		clusterID:  clusterID,
		vectorDim:  vectorDim,
		entries:    make(map[uint64]*ManifestEntry),
		path:       path,
		logger:     logger,
		metrics:    metrics,
		dirty:      true,
	}
}

// LoadManifest loads an existing manifest from file
func LoadManifest(path string, logger logging.Logger, metrics *metrics.StorageMetrics) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLoadFailed, err)
	}
	
	manifest := &Manifest{
		entries: make(map[uint64]*ManifestEntry),
		path:    path,
		logger:  logger,
		metrics: metrics,
	}
	
	if err := manifest.Deserialize(data); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrManifestCorrupted, err)
	}
	
	manifest.dirty = false
	
	return manifest, nil
}

// AddSegment adds a segment to the manifest
func (m *Manifest) AddSegment(entry *ManifestEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.entries[entry.SegmentID]; exists {
		return ErrSegmentAlreadyAdded
	}
	
	// Validate entry
	if entry.ClusterID != m.clusterID {
		return fmt.Errorf("%w: cluster ID mismatch", ErrInvalidManifest)
	}
	if entry.VectorDim != m.vectorDim {
		return fmt.Errorf("%w: vector dimension mismatch", ErrInvalidManifest)
	}
	
	m.entries[entry.SegmentID] = entry
	m.modifiedAt = time.Now().Unix()
	m.dirty = true
	
	m.logger.Info("Added segment to manifest",
		"segment_id", entry.SegmentID,
		"cluster_id", entry.ClusterID,
		"path", entry.Path,
		"vector_count", entry.VectorCount)
	
	return nil
}

// RemoveSegment removes a segment from the manifest
func (m *Manifest) RemoveSegment(segmentID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.entries[segmentID]; !exists {
		return ErrSegmentNotFound
	}
	
	delete(m.entries, segmentID)
	m.modifiedAt = time.Now().Unix()
	m.dirty = true
	
	m.logger.Info("Removed segment from manifest", "segment_id", segmentID)
	
	return nil
}

// GetSegment returns a segment entry from the manifest
func (m *Manifest) GetSegment(segmentID uint64) (*ManifestEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	entry, exists := m.entries[segmentID]
	if !exists {
		return nil, ErrSegmentNotFound
	}
	
	return entry, nil
}

// GetSegments returns all segment entries from the manifest
func (m *Manifest) GetSegments() []*ManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	segments := make([]*ManifestEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		segments = append(segments, entry)
	}
	
	// Sort by created time
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].CreatedAt < segments[j].CreatedAt
	})
	
	return segments
}

// GetActiveSegments returns all active segments
func (m *Manifest) GetActiveSegments() []*ManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	segments := make([]*ManifestEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		if entry.Status == "active" {
			segments = append(segments, entry)
		}
	}
	
	// Sort by created time
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].CreatedAt < segments[j].CreatedAt
	})
	
	return segments
}

// GetReadOnlySegments returns all read-only segments
func (m *Manifest) GetReadOnlySegments() []*ManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	segments := make([]*ManifestEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		if entry.Status == "readonly" {
			segments = append(segments, entry)
		}
	}
	
	// Sort by created time
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].CreatedAt < segments[j].CreatedAt
	})
	
	return segments
}

// UpdateSegmentStatus updates the status of a segment
func (m *Manifest) UpdateSegmentStatus(segmentID uint64, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	entry, exists := m.entries[segmentID]
	if !exists {
		return ErrSegmentNotFound
	}
	
	entry.Status = status
	entry.ModifiedAt = time.Now().Unix()
	m.modifiedAt = time.Now().Unix()
	m.dirty = true
	
	m.logger.Info("Updated segment status",
		"segment_id", segmentID,
		"status", status)
	
	return nil
}

// UpdateSegmentMetadata updates the metadata of a segment
func (m *Manifest) UpdateSegmentMetadata(segmentID uint64, metadata map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	entry, exists := m.entries[segmentID]
	if !exists {
		return ErrSegmentNotFound
	}
	
	entry.Metadata = metadata
	entry.ModifiedAt = time.Now().Unix()
	m.modifiedAt = time.Now().Unix()
	m.dirty = true
	
	return nil
}

// UpdateSegmentVectorCount updates the vector count of a segment
func (m *Manifest) UpdateSegmentVectorCount(segmentID uint64, count uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	entry, exists := m.entries[segmentID]
	if !exists {
		return ErrSegmentNotFound
	}
	
	entry.VectorCount = count
	entry.ModifiedAt = time.Now().Unix()
	m.modifiedAt = time.Now().Unix()
	m.dirty = true
	
	return nil
}

// UpdateSegmentSize updates the size of a segment
func (m *Manifest) UpdateSegmentSize(segmentID uint64, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	entry, exists := m.entries[segmentID]
	if !exists {
		return ErrSegmentNotFound
	}
	
	entry.Size = size
	entry.ModifiedAt = time.Now().Unix()
	m.modifiedAt = time.Now().Unix()
	m.dirty = true
	
	return nil
}

// GetSegmentCount returns the number of segments in the manifest
func (m *Manifest) GetSegmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return len(m.entries)
}

// GetTotalVectorCount returns the total number of vectors across all segments
func (m *Manifest) GetTotalVectorCount() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var total uint64
	for _, entry := range m.entries {
		total += uint64(entry.VectorCount)
	}
	
	return total
}

// GetTotalSize returns the total size of all segments
func (m *Manifest) GetTotalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var total int64
	for _, entry := range m.entries {
		total += entry.Size
	}
	
	return total
}

// GetClusterID returns the cluster ID of the manifest
func (m *Manifest) GetClusterID() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.clusterID
}

// GetVectorDim returns the vector dimension of the manifest
func (m *Manifest) GetVectorDim() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.vectorDim
}

// GetCreatedAt returns the creation time of the manifest
func (m *Manifest) GetCreatedAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return time.Unix(m.createdAt, 0)
}

// GetModifiedAt returns the last modification time of the manifest
func (m *Manifest) GetModifiedAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return time.Unix(m.modifiedAt, 0)
}

// IsDirty returns true if the manifest has unsaved changes
func (m *Manifest) IsDirty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.dirty
}

// Save saves the manifest to file
func (m *Manifest) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.dirty {
		return nil
	}
	
	data, err := m.Serialize()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSaveFailed, err)
	}
	
	// Write to temporary file first
	tempPath := m.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("%w: %v", ErrSaveFailed, err)
	}
	
	// Rename temporary file to final path
	if err := os.Rename(tempPath, m.path); err != nil {
		return fmt.Errorf("%w: %v", ErrSaveFailed, err)
	}
	
	m.dirty = false
	
	m.logger.Info("Saved manifest", "path", m.path, "segments", len(m.entries))
	
	return nil
}

// Serialize serializes the manifest to bytes
func (m *Manifest) Serialize() ([]byte, error) {
	// Create JSON data for entries
	entriesData, err := json.Marshal(m.entries)
	if err != nil {
		return nil, err
	}
	
	// Create binary header
	header := make([]byte, 32)
	binary.LittleEndian.PutUint32(header[0:4], m.magic)
	binary.LittleEndian.PutUint32(header[4:8], m.version)
	binary.LittleEndian.PutUint64(header[8:16], uint64(m.createdAt))
	binary.LittleEndian.PutUint64(header[16:24], uint64(m.modifiedAt))
	binary.LittleEndian.PutUint32(header[24:28], m.clusterID)
	binary.LittleEndian.PutUint32(header[28:32], m.vectorDim)
	
	// Combine header and entries data
	data := append(header, entriesData...)
	
	return data, nil
}

// Deserialize deserializes the manifest from bytes
func (m *Manifest) Deserialize(data []byte) error {
	if len(data) < 32 {
		return ErrManifestCorrupted
	}
	
	// Read header
	m.magic = binary.LittleEndian.Uint32(data[0:4])
	m.version = binary.LittleEndian.Uint32(data[4:8])
	m.createdAt = int64(binary.LittleEndian.Uint64(data[8:16]))
	m.modifiedAt = int64(binary.LittleEndian.Uint64(data[16:24]))
	m.clusterID = binary.LittleEndian.Uint32(data[24:28])
	m.vectorDim = binary.LittleEndian.Uint32(data[28:32])
	
	// Validate magic and version
	if m.magic != ManifestMagic {
		return ErrManifestCorrupted
	}
	if m.version != ManifestVersion {
		return ErrManifestCorrupted
	}
	
	// Read entries
	if len(data) > 32 {
		if err := json.Unmarshal(data[32:], &m.entries); err != nil {
			return ErrManifestCorrupted
		}
	}
	
	return nil
}

// Validate validates the manifest
func (m *Manifest) Validate() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Validate magic and version
	if m.magic != ManifestMagic {
		return ErrManifestCorrupted
	}
	if m.version != ManifestVersion {
		return ErrManifestCorrupted
	}
	
	// Validate entries
	for segmentID, entry := range m.entries {
		if entry.SegmentID != segmentID {
			return ErrManifestCorrupted
		}
		if entry.ClusterID != m.clusterID {
			return ErrManifestCorrupted
		}
		if entry.VectorDim != m.vectorDim {
			return ErrManifestCorrupted
		}
		if entry.Status == "" {
			entry.Status = "active"
		}
	}
	
	return nil
}

// Compact compacts the manifest by removing deleted segments
func (m *Manifest) Compact() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Remove deleted segments
	for segmentID, entry := range m.entries {
		if entry.Status == "deleted" {
			delete(m.entries, segmentID)
		}
	}
	
	m.modifiedAt = time.Now().Unix()
	m.dirty = true
	
	return nil
}

// GetStats returns manifest statistics
func (m *Manifest) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	stats := make(map[string]interface{})
	stats["cluster_id"] = m.clusterID
	stats["vector_dim"] = m.vectorDim
	stats["segment_count"] = len(m.entries)
	stats["total_vector_count"] = m.GetTotalVectorCount()
	stats["total_size"] = m.GetTotalSize()
	stats["created_at"] = m.createdAt
	stats["modified_at"] = m.modifiedAt
	stats["dirty"] = m.dirty
	
	// Count segments by status
	statusCounts := make(map[string]int)
	for _, entry := range m.entries {
		statusCounts[entry.Status]++
	}
	stats["status_counts"] = statusCounts
	
	return stats
}

// Backup creates a backup of the manifest
func (m *Manifest) Backup(backupPath string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	data, err := m.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize manifest: %w", err)
	}
	
	if err := os.WriteFile(backupPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write backup: %w", err)
	}
	
	m.logger.Info("Created manifest backup", "path", backupPath)
	
	return nil
}

// Restore restores the manifest from a backup
func (m *Manifest) Restore(backupPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	data, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup: %w", err)
	}
	
	backupManifest := &Manifest{
		entries: make(map[uint64]*ManifestEntry),
		path:    m.path,
		logger:  m.logger,
		metrics: m.metrics,
	}
	
	if err := backupManifest.Deserialize(data); err != nil {
		return fmt.Errorf("failed to deserialize backup: %w", err)
	}
	
	// Replace current manifest with backup
	m.magic = backupManifest.magic
	m.version = backupManifest.version
	m.createdAt = backupManifest.createdAt
	m.modifiedAt = backupManifest.modifiedAt
	m.clusterID = backupManifest.clusterID
	m.vectorDim = backupManifest.vectorDim
	m.entries = backupManifest.entries
	m.dirty = true
	
	m.logger.Info("Restored manifest from backup", "path", backupPath)
	
	return nil
}

// Manager represents a manifest manager
type Manager struct {
	config      *config.Config
	manifests   map[uint32]*Manifest // cluster_id -> manifest
	mu          sync.RWMutex
	logger      logging.Logger
	metrics     *metrics.StorageMetrics
	dataDir     string
	autoSave    bool
	saveInterval time.Duration
	saveTimer   *time.Timer
}

// NewManager creates a new manifest manager
func NewManager(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*Manager, error) {
	dataDir := "./data"
	if cfg != nil {
		if dataDirCfg, ok := cfg.Get("data_dir"); ok {
			if dir, ok := dataDirCfg.(string); ok {
				dataDir = dir
			}
		}
	}
	
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	manager := &Manager{
		config:    cfg,
		manifests: make(map[uint32]*Manifest),
		logger:    logger,
		metrics:   metrics,
		dataDir:   dataDir,
		autoSave:  true,
		saveInterval: 30 * time.Second,
	}
	
	// Load existing manifests
	if err := manager.loadManifests(); err != nil {
		logger.Error("Failed to load manifests", "error", err)
	}
	
	// Start auto-save timer
	manager.startAutoSave()
	
	return manager, nil
}

// loadManifests loads all manifests from the data directory
func (m *Manager) loadManifests() error {
	entries, err := os.ReadDir(m.dataDir)
	if err != nil {
		return err
	}
	
	for _, entry := range entries {
		if entry.IsDir() {
			manifestPath := filepath.Join(m.dataDir, entry.Name(), ManifestFileName)
			if _, err := os.Stat(manifestPath); err == nil {
				manifest, err := LoadManifest(manifestPath, m.logger, m.metrics)
				if err != nil {
					m.logger.Error("Failed to load manifest", "path", manifestPath, "error", err)
					continue
				}
				
				m.mu.Lock()
				m.manifests[manifest.GetClusterID()] = manifest
				m.mu.Unlock()
				
				m.logger.Info("Loaded manifest", "cluster_id", manifest.GetClusterID(), "path", manifestPath)
			}
		}
	}
	
	return nil
}

// GetManifest returns a manifest for the given cluster ID
func (m *Manager) GetManifest(clusterID uint32) (*Manifest, error) {
	m.mu.RLock()
	manifest, exists := m.manifests[clusterID]
	m.mu.RUnlock()
	
	if !exists {
		// Create new manifest
		manifestPath := filepath.Join(m.dataDir, fmt.Sprintf("cluster_%d", clusterID), ManifestFileName)
		manifest = NewManifest(manifestPath, clusterID, 0, m.logger, m.metrics) // vector_dim will be set when first segment is added
		
		m.mu.Lock()
		m.manifests[clusterID] = manifest
		m.mu.Unlock()
		
		m.logger.Info("Created new manifest", "cluster_id", clusterID, "path", manifestPath)
	}
	
	return manifest, nil
}

// SaveAll saves all manifests
func (m *Manager) SaveAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	for _, manifest := range m.manifests {
		if err := manifest.Save(); err != nil {
			m.logger.Error("Failed to save manifest", "cluster_id", manifest.GetClusterID(), "error", err)
			return err
		}
	}
	
	return nil
}

// startAutoSave starts the auto-save timer
func (m *Manager) startAutoSave() {
	if m.saveTimer != nil {
		m.saveTimer.Stop()
	}
	
	m.saveTimer = time.AfterFunc(m.saveInterval, func() {
		if err := m.SaveAll(); err != nil {
			m.logger.Error("Failed to auto-save manifests", "error", err)
		}
		
		// Restart timer
		m.startAutoSave()
	})
}

// Close closes the manifest manager
func (m *Manager) Close() error {
	if m.saveTimer != nil {
		m.saveTimer.Stop()
	}
	
	// Save all manifests
	if err := m.SaveAll(); err != nil {
		return err
	}
	
	m.logger.Info("Closed manifest manager")
	
	return nil
}

// GetStats returns manager statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	stats := make(map[string]interface{})
	stats["manifest_count"] = len(m.manifests)
	stats["data_dir"] = m.dataDir
	stats["auto_save"] = m.autoSave
	stats["save_interval"] = m.saveInterval.String()
	
	// Aggregate stats from all manifests
	var totalSegments int
	var totalVectors uint64
	var totalSize int64
	
	for _, manifest := range m.manifests {
		manifestStats := manifest.GetStats()
		totalSegments += manifestStats["segment_count"].(int)
		totalVectors += manifestStats["total_vector_count"].(uint64)
		totalSize += manifestStats["total_size"].(int64)
	}
	
	stats["total_segments"] = totalSegments
	stats["total_vectors"] = totalVectors
	stats["total_size"] = totalSize
	
	return stats
}
