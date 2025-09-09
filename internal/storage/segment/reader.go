package segment

import (
    "encoding/binary"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "hash/crc32"
    "math"
    "os"
    "sync"

    "vexdb/internal/config"
    "vexdb/internal/metrics"
    "vexdb/internal/storage/compression"
    "vexdb/internal/types"
    "go.uber.org/zap"
)

var (
    ErrSegmentNotOpen    = errors.New("segment not open")
    ErrReaderClosed      = errors.New("reader is closed")
    ErrInvalidOffset     = errors.New("invalid offset")
    ErrReadFailed        = errors.New("read failed")
    ErrSeekFailed        = errors.New("seek failed")
    ErrVectorNotFound    = errors.New("vector not found")
	ErrCorruptedIndex    = errors.New("corrupted index")
	ErrInvalidVectorSize = errors.New("invalid vector size")
)

// ReaderConfig represents the configuration for the segment reader
type ReaderConfig struct {
	MaxOpenFiles       int           `yaml:"max_open_files" json:"max_open_files"`
	ReadBufferSize     int           `yaml:"read_buffer_size" json:"read_buffer_size"`
	EnableMMap         bool          `yaml:"enable_mmap" json:"enable_mmap"`
	EnablePrefetch     bool          `yaml:"enable_prefetch" json:"enable_prefetch"`
	PrefetchDistance   int           `yaml:"prefetch_distance" json:"prefetch_distance"`
	CacheSize          int           `yaml:"cache_size" json:"cache_size"`
	EnableReadAhead    bool          `yaml:"enable_read_ahead" json:"enable_read_ahead"`
	ReadAheadSize      int           `yaml:"read_ahead_size" json:"read_ahead_size"`
	DataDir            string        `yaml:"data_dir" json:"data_dir"`
	EnableChecksum     bool          `yaml:"enable_checksum" json:"enable_checksum"`
}

// DefaultReaderConfig returns the default reader configuration
func DefaultReaderConfig() *ReaderConfig {
	return &ReaderConfig{
		MaxOpenFiles:     1000,
		ReadBufferSize:   64 * 1024, // 64KB
		EnableMMap:       true,
		EnablePrefetch:   true,
		PrefetchDistance: 10,
		CacheSize:        1000,
		EnableReadAhead:  true,
		ReadAheadSize:    128 * 1024, // 128KB
		DataDir:          "./data",
		EnableChecksum:   true,
	}
}

// Reader represents a segment reader
type Reader struct {
    config      *ReaderConfig
    segment     *FileSegment
	file        *os.File
	path        string
	mmapData    []byte
    index       map[uint64]*FileIndexEntry
	cache       map[uint64]*types.Vector
	mu          sync.RWMutex
	closed      bool
    logger      *zap.Logger
    metrics     *metrics.Collector
	readBuffer  []byte
	openFiles   map[string]*os.File
	fileCount   int
}

// NewReader creates a new segment reader
func NewReader(cfg *config.Config, logger *zap.Logger, metrics *metrics.Collector, _ *compression.Compressor, _ *os.File) (*Reader, error) {
    readerConfig := DefaultReaderConfig()
    // ignore cfg overrides for now
	
	return &Reader{
		config:     readerConfig,
		logger:     logger,
		metrics:    metrics,
		readBuffer: make([]byte, readerConfig.ReadBufferSize),
		cache:      make(map[uint64]*types.Vector),
		openFiles:  make(map[string]*os.File),
	}, nil
}

// Open opens a segment for reading
func (r *Reader) Open(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.closed {
		return ErrReaderClosed
	}
	
	if r.segment != nil {
		return ErrSegmentExists
	}
	
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("segment file not found: %s", path)
	}
	
	// Load segment
    segment, err := LoadFileSegment(path)
	if err != nil {
		return fmt.Errorf("failed to load segment: %w", err)
	}
	
	// Open file
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}
	
	// Read segment data
    data, err := io.ReadAll(file)
    if err != nil {
        _ = file.Close()
        return fmt.Errorf("failed to read segment data: %w", err)
    }
	
	// Deserialize segment
    if err := segment.Deserialize(data); err != nil {
        _ = file.Close()
        return fmt.Errorf("failed to deserialize segment: %w", err)
    }
	
	// Build index
    index := make(map[uint64]*FileIndexEntry)
	for _, entry := range segment.index {
		index[entry.VectorID] = entry
	}
	
	r.segment = segment
	r.path = path
	r.file = file
	r.index = index
	
	// Enable memory mapping if configured
	if r.config.EnableMMap {
            if err := r.enableMMap(); err != nil {
                r.logger.Warn("Failed to enable memory mapping", zap.Error(err))
            }
	}
	
    r.logger.Info("Opened segment for reading",
        zap.String("path", path),
        zap.Uint32("cluster_id", segment.GetClusterID()),
        zap.Uint32("vector_dim", segment.GetVectorDim()),
        zap.Int("vector_count", segment.GetVectorCount()),
        zap.Bool("mmap_enabled", r.mmapData != nil))
	
	return nil
}

// enableMMap enables memory mapping for the segment
func (r *Reader) enableMMap() error {
	// This would implement memory mapping using syscall.Mmap
	// For now, we'll leave it as a placeholder
	// In a real implementation, this would map the file into memory
	// for faster random access
	return nil
}

// ReadVector reads a vector by ID
func (r *Reader) ReadVector(vectorID uint64) (*types.Vector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.closed {
		return nil, ErrReaderClosed
	}
	
	if r.segment == nil {
		return nil, ErrSegmentNotOpen
	}
	
	// Check cache first
    if vector, exists := r.cache[vectorID]; exists {
        if r.metrics != nil { r.metrics.RecordCounter("storage_cache_hits_total", 1, map[string]string{"component":"segment_reader"}) }
        return vector, nil
    }
	
    if r.metrics != nil { r.metrics.RecordCounter("storage_cache_misses_total", 1, map[string]string{"component":"segment_reader"}) }
	
	// Find index entry
	entry, exists := r.index[vectorID]
	if !exists {
		return nil, ErrVectorNotFound
	}
	
	// Read vector from file
	vector, err := r.readVectorFromEntry(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to read vector: %w", err)
	}
	
	// Update cache
	if len(r.cache) < r.config.CacheSize {
		r.cache[vectorID] = vector
	}
	
    // Update metrics
    if r.metrics != nil { r.metrics.RecordCounter("storage_read_operations_total", 1, map[string]string{"component":"segment_reader"}) }
	
	return vector, nil
}

// readVectorFromEntry reads a vector from an index entry
func (r *Reader) readVectorFromEntry(entry *FileIndexEntry) (*types.Vector, error) {
    var data []byte
	
	if r.mmapData != nil {
		// Read from memory-mapped data
		if int(entry.Offset+uint64(entry.Size)) > len(r.mmapData) {
			return nil, ErrInvalidOffset
		}
		data = r.mmapData[entry.Offset : entry.Offset+uint64(entry.Size)]
	} else {
		// Read from file
            if _, err := r.file.Seek(int64(entry.Offset), io.SeekStart); err != nil {
                if r.metrics != nil { r.metrics.RecordCounter("storage_errors_total", 1, map[string]string{"component":"segment_reader","type":"seek_failed"}) }
                return nil, fmt.Errorf("%w: %v", ErrSeekFailed, err)
            }
		
		data = make([]byte, entry.Size)
            if _, err := io.ReadFull(r.file, data); err != nil {
                if r.metrics != nil { r.metrics.RecordCounter("storage_errors_total", 1, map[string]string{"component":"segment_reader","type":"read_failed"}) }
                return nil, fmt.Errorf("%w: %v", ErrReadFailed, err)
            }
	}
	
    // Parse vector data
    return r.parseVectorData(data, entry)
}

// parseVectorData parses vector data from bytes
func (r *Reader) parseVectorData(data []byte, entry *FileIndexEntry) (*types.Vector, error) {
	if len(data) < 24 {
		return nil, ErrCorruptedData
	}
	
	// Read vector header
    vectorHeader := &FileVectorHeader{}
	vectorHeader.VectorID = binary.LittleEndian.Uint64(data[0:8])
	vectorHeader.Timestamp = int64(binary.LittleEndian.Uint64(data[8:16]))
	vectorHeader.MetadataSize = binary.LittleEndian.Uint32(data[16:20])
	vectorHeader.VectorSize = binary.LittleEndian.Uint32(data[20:24])
	
	// Verify checksum
	if r.config.EnableChecksum {
		checksum := crc32.ChecksumIEEE(data[:20])
		if checksum != binary.LittleEndian.Uint32(data[20:24]) {
			return nil, ErrChecksumMismatch
		}
	}
	
	// Validate sizes
	if vectorHeader.VectorID != entry.VectorID {
		return nil, ErrCorruptedData
	}
	
	expectedSize := 24 + int(vectorHeader.MetadataSize) + int(vectorHeader.VectorSize)
	if len(data) < expectedSize {
		return nil, ErrCorruptedData
	}
	
	// Read metadata
	var metadata []byte
	if vectorHeader.MetadataSize > 0 {
		metadata = data[24 : 24+vectorHeader.MetadataSize]
	}
	
	// Read vector data
	vectorDataOffset := 24 + vectorHeader.MetadataSize
	vectorData := data[vectorDataOffset : vectorDataOffset+vectorHeader.VectorSize]
	
	// Parse vector data
	if vectorHeader.VectorSize%4 != 0 {
		return nil, ErrInvalidVectorSize
	}
	
	vectorDim := vectorHeader.VectorSize / 4
	vectorValues := make([]float32, vectorDim)
	
	for i := 0; i < int(vectorDim); i++ {
		vectorValues[i] = math.Float32frombits(binary.LittleEndian.Uint32(vectorData[i*4 : (i+1)*4]))
	}
	
    // Create vector
    vector := &types.Vector{
        ID:        fmt.Sprintf("%d", vectorHeader.VectorID),
        Data:      vectorValues,
        Timestamp: vectorHeader.Timestamp,
    }
	
	// Parse metadata if present
    if len(metadata) > 0 {
        var md map[string]interface{}
        if err := json.Unmarshal(metadata, &md); err == nil {
            vector.Metadata = md
        }
    }
	
	return vector, nil
}

// ReadVectors reads multiple vectors by IDs
func (r *Reader) ReadVectors(vectorIDs []uint64) ([]*types.Vector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.closed {
		return nil, ErrReaderClosed
	}
	
	if r.segment == nil {
		return nil, ErrSegmentNotOpen
	}
	
	vectors := make([]*types.Vector, 0, len(vectorIDs))
	
	for _, vectorID := range vectorIDs {
		vector, err := r.ReadVector(vectorID)
		if err != nil {
			if errors.Is(err, ErrVectorNotFound) {
				continue // Skip missing vectors
			}
			return nil, fmt.Errorf("failed to read vector %d: %w", vectorID, err)
		}
		vectors = append(vectors, vector)
	}
	
	return vectors, nil
}

// ReadAllVectors reads all vectors in the segment
func (r *Reader) ReadAllVectors() ([]*types.Vector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.closed {
		return nil, ErrReaderClosed
	}
	
	if r.segment == nil {
		return nil, ErrSegmentNotOpen
	}
	
	return r.segment.GetVectors(), nil
}

// HasVector checks if a vector exists in the segment
func (r *Reader) HasVector(vectorID uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.closed || r.segment == nil {
		return false
	}
	
	_, exists := r.index[vectorID]
	return exists
}

// GetVectorIDs returns all vector IDs in the segment
func (r *Reader) GetVectorIDs() []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.closed || r.segment == nil {
		return nil
	}
	
	ids := make([]uint64, 0, len(r.index))
	for id := range r.index {
		ids = append(ids, id)
	}
	
	return ids
}

// GetVectorCount returns the number of vectors in the segment
func (r *Reader) GetVectorCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.segment == nil {
		return 0
	}
	return r.segment.GetVectorCount()
}

// Prefetch prefetches vectors around the given vector ID
func (r *Reader) Prefetch(vectorID uint64) error {
	if !r.config.EnablePrefetch {
		return nil
	}
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.closed || r.segment == nil {
		return nil
	}
	
	// Find the position of the vector in the index
    var targetIndex int
	found := false
	
    for i, entry := range r.segment.index {
        if entry.VectorID == vectorID {
            targetIndex = i
            found = true
            break
        }
    }
	
	if !found {
		return nil
	}
	
    // Prefetch vectors around the target
    startIndex := max(0, targetIndex-r.config.PrefetchDistance)
    endIndex := min(len(r.segment.index), targetIndex+r.config.PrefetchDistance+1)
	
	for i := startIndex; i < endIndex; i++ {
		entry := r.segment.index[i]
            if _, exists := r.cache[entry.VectorID]; !exists {
            if _, err := r.readVectorFromEntry(entry); err == nil {
                if len(r.cache) < r.config.CacheSize {
                    r.cache[entry.VectorID] = r.segment.vectors[entry.VectorID]
                }
            }
            }
	}
	
	return nil
}

// Close closes the reader
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.closed {
		return nil
	}
	
	// Close file
        if r.file != nil {
            if err := r.file.Close(); err != nil {
                if r.metrics != nil { r.metrics.RecordCounter("storage_errors_total", 1, map[string]string{"component":"segment_reader","type":"close_failed"}) }
                r.logger.Error("Failed to close segment file", zap.Error(err))
            }
        }
	
	// Close all open files
	for path, file := range r.openFiles {
            if err := file.Close(); err != nil {
                r.logger.Error("Failed to close file", zap.String("path", path), zap.Error(err))
            }
	}
	
	// Clear memory mapping
	if r.mmapData != nil {
		// This would unmap the memory
		r.mmapData = nil
	}
	
	r.closed = true
	
    r.logger.Info("Closed segment reader", zap.String("path", r.path))
	
	return nil
}

// GetSegment returns the current segment
func (r *Reader) GetSegment() *FileSegment {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	return r.segment
}

// GetPath returns the current segment path
func (r *Reader) GetPath() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	return r.path
}

// IsClosed returns true if the reader is closed
func (r *Reader) IsClosed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	return r.closed
}

// GetConfig returns the reader configuration
func (r *Reader) GetConfig() *ReaderConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	// Return a copy of the config
	config := *r.config
	return &config
}

// UpdateConfig updates the reader configuration
func (r *Reader) UpdateConfig(config *ReaderConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.closed {
		return ErrReaderClosed
	}
	
	r.config = config
	
	// Update read buffer size if needed
	if config.ReadBufferSize != len(r.readBuffer) {
		r.readBuffer = make([]byte, config.ReadBufferSize)
	}
	
	// Clear cache if size changed
	if config.CacheSize != len(r.cache) {
		r.cache = make(map[uint64]*types.Vector)
	}
	
    r.logger.Info("Updated reader configuration", zap.Any("config", config))
	
	return nil
}

// GetStats returns reader statistics
func (r *Reader) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	stats := make(map[string]interface{})
	stats["closed"] = r.closed
	stats["cache_size"] = len(r.cache)
	stats["max_cache_size"] = r.config.CacheSize
	stats["cache_usage"] = float64(len(r.cache)) / float64(r.config.CacheSize) * 100
	stats["open_files"] = len(r.openFiles)
	stats["max_open_files"] = r.config.MaxOpenFiles
	stats["mmap_enabled"] = r.mmapData != nil
	stats["read_buffer_size"] = len(r.readBuffer)
	
	if r.segment != nil {
		stats["vector_count"] = r.segment.GetVectorCount()
		stats["cluster_id"] = r.segment.GetClusterID()
		stats["vector_dim"] = r.segment.GetVectorDim()
		stats["segment_size"] = r.segment.GetSize()
		stats["segment_readonly"] = r.segment.IsReadOnly()
	}
	
	return stats
}

// ClearCache clears the vector cache
func (r *Reader) ClearCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.cache = make(map[uint64]*types.Vector)
}

// GetCacheInfo returns cache information
func (r *Reader) GetCacheInfo() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	info := make(map[string]interface{})
	info["size"] = len(r.cache)
	info["max_size"] = r.config.CacheSize
	info["usage"] = float64(len(r.cache)) / float64(r.config.CacheSize) * 100
	info["enabled"] = r.config.CacheSize > 0
	
	return info
}

// Validate validates the reader state
func (r *Reader) Validate() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.closed {
		return ErrReaderClosed
	}
	
	if r.segment == nil {
		return ErrSegmentNotOpen
	}
	
	// Validate segment
	if err := r.segment.Validate(); err != nil {
		return fmt.Errorf("segment validation failed: %w", err)
	}
	
	// Validate index consistency
	if len(r.index) != len(r.segment.index) {
		return ErrCorruptedIndex
	}
	
	for vectorID, entry := range r.index {
		if _, exists := r.segment.vectors[vectorID]; !exists {
			return ErrCorruptedIndex
		}
		if entry.VectorID != vectorID {
			return ErrCorruptedIndex
		}
	}
	
	return nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
