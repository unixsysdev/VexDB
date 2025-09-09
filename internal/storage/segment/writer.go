package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrSegmentExists    = errors.New("segment already exists")
	ErrSegmentNotFound  = errors.New("segment not found")
	ErrWriterClosed     = errors.New("writer is closed")
	ErrInvalidVector    = errors.New("invalid vector")
	ErrWriteFailed      = errors.New("write failed")
	ErrFlushFailed      = errors.New("flush failed")
	ErrSyncFailed       = errors.New("sync failed")
	ErrCloseFailed      = errors.New("close failed")
)

// WriterConfig represents the configuration for the segment writer
type WriterConfig struct {
	MaxVectorsPerSegment int           `yaml:"max_vectors_per_segment" json:"max_vectors_per_segment"`
	FlushInterval        time.Duration `yaml:"flush_interval" json:"flush_interval"`
	SyncOnWrite          bool          `yaml:"sync_on_write" json:"sync_on_write"`
	CompressionLevel     int           `yaml:"compression_level" json:"compression_level"`
	BufferSize           int           `yaml:"buffer_size" json:"buffer_size"`
	DataDir              string        `yaml:"data_dir" json:"data_dir"`
	EnableCompression    bool          `yaml:"enable_compression" json:"enable_compression"`
	EnableChecksum       bool          `yaml:"enable_checksum" json:"enable_checksum"`
}

// DefaultWriterConfig returns the default writer configuration
func DefaultWriterConfig() *WriterConfig {
	return &WriterConfig{
		MaxVectorsPerSegment: DefaultMaxVectorsPerSegment,
		FlushInterval:        5 * time.Second,
		SyncOnWrite:          false,
		CompressionLevel:     DefaultCompressionLevel,
		BufferSize:           64 * 1024, // 64KB
		DataDir:              "./data",
		EnableCompression:    true,
		EnableChecksum:       true,
	}
}

// Writer represents a segment writer
type Writer struct {
	config      *WriterConfig
	segment     *Segment
	file        *os.File
	path        string
	mu          sync.RWMutex
	closed      bool
	flushTimer  *time.Timer
	logger      logging.Logger
	metrics     *metrics.StorageMetrics
	writeBuffer []byte
	bufferPos   int
}

// NewWriter creates a new segment writer
func NewWriter(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*Writer, error) {
	writerConfig := DefaultWriterConfig()
	
	if cfg != nil {
		if writerCfg, ok := cfg.Get("writer"); ok {
			if cfgMap, ok := writerCfg.(map[string]interface{}); ok {
				if maxVectors, ok := cfgMap["max_vectors_per_segment"].(int); ok {
					writerConfig.MaxVectorsPerSegment = maxVectors
				}
				if flushInterval, ok := cfgMap["flush_interval"].(string); ok {
					if dur, err := time.ParseDuration(flushInterval); err == nil {
						writerConfig.FlushInterval = dur
					}
				}
				if syncOnWrite, ok := cfgMap["sync_on_write"].(bool); ok {
					writerConfig.SyncOnWrite = syncOnWrite
				}
				if compressionLevel, ok := cfgMap["compression_level"].(int); ok {
					writerConfig.CompressionLevel = compressionLevel
				}
				if bufferSize, ok := cfgMap["buffer_size"].(int); ok {
					writerConfig.BufferSize = bufferSize
				}
				if dataDir, ok := cfgMap["data_dir"].(string); ok {
					writerConfig.DataDir = dataDir
				}
				if enableCompression, ok := cfgMap["enable_compression"].(bool); ok {
					writerConfig.EnableCompression = enableCompression
				}
				if enableChecksum, ok := cfgMap["enable_checksum"].(bool); ok {
					writerConfig.EnableChecksum = enableChecksum
				}
			}
		}
	}
	
	// Ensure data directory exists
	if err := os.MkdirAll(writerConfig.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	return &Writer{
		config:      writerConfig,
		logger:      logger,
		metrics:     metrics,
		writeBuffer: make([]byte, writerConfig.BufferSize),
	}, nil
}

// CreateSegment creates a new segment for writing
func (w *Writer) CreateSegment(clusterID uint32, vectorDim uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	if w.segment != nil {
		return ErrSegmentExists
	}
	
	// Generate segment path
	segmentID := time.Now().UnixNano()
	path := filepath.Join(w.config.DataDir, fmt.Sprintf("segment_%d_%d.vex", clusterID, segmentID))
	
	// Create new segment
	w.segment = NewSegment(path, vectorDim, clusterID, w.config.MaxVectorsPerSegment)
	w.path = path
	
	// Create file
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create segment file: %w", err)
	}
	
	w.file = file
	
	// Start flush timer
	w.startFlushTimer()
	
	w.logger.Info("Created new segment",
		"path", path,
		"cluster_id", clusterID,
		"vector_dim", vectorDim,
		"max_vectors", w.config.MaxVectorsPerSegment)
	
	return nil
}

// OpenSegment opens an existing segment for writing
func (w *Writer) OpenSegment(path string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	if w.segment != nil {
		return ErrSegmentExists
	}
	
	// Load segment
	segment, err := LoadSegment(path)
	if err != nil {
		return fmt.Errorf("failed to load segment: %w", err)
	}
	
	// Open file
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}
	
	w.segment = segment
	w.path = path
	w.file = file
	
	// Start flush timer
	w.startFlushTimer()
	
	w.logger.Info("Opened existing segment",
		"path", path,
		"cluster_id", segment.GetClusterID(),
		"vector_dim", segment.GetVectorDim(),
		"vector_count", segment.GetVectorCount())
	
	return nil
}

// WriteVector writes a vector to the segment
func (w *Writer) WriteVector(vector *types.Vector) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	if w.segment == nil {
		return ErrSegmentNotFound
	}
	
	if w.segment.IsFull() {
		return ErrSegmentFull
	}
	
	// Validate vector
	if err := vector.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidVector, err)
	}
	
	// Check vector dimension
	if len(vector.Data) != int(w.segment.GetVectorDim()) {
		return fmt.Errorf("%w: vector dimension mismatch", ErrInvalidVector)
	}
	
	// Add vector to segment
	if err := w.segment.AddVector(vector); err != nil {
		return fmt.Errorf("failed to add vector to segment: %w", err)
	}
	
	// Write vector to buffer
	if err := w.writeVectorToBuffer(vector); err != nil {
		return fmt.Errorf("failed to write vector to buffer: %w", err)
	}
	
	// Update metrics
	w.metrics.WriteOperations.Inc("storage", "write_vector")
	w.metrics.VectorsTotal.Inc("storage", fmt.Sprintf("cluster_%d", w.segment.GetClusterID()))
	
	// Check if buffer needs to be flushed
	if w.bufferPos >= len(w.writeBuffer) {
		if err := w.flushBuffer(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
	}
	
	// Sync if configured
	if w.config.SyncOnWrite {
		if err := w.file.Sync(); err != nil {
			w.metrics.Errors.Inc("storage", "sync_failed")
			return fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	
	return nil
}

// writeVectorToBuffer writes a vector to the write buffer
func (w *Writer) writeVectorToBuffer(vector *types.Vector) error {
	// Serialize vector header
	vectorHeader := &VectorHeader{
		VectorID:     vector.ID,
		Timestamp:    vector.Timestamp,
		VectorSize:   uint32(len(vector.Data) * 4), // float32 = 4 bytes
		Flags:        0,
	}
	
	var metadata []byte
	if vector.Metadata != nil {
		metadata = vector.Metadata.Serialize()
		vectorHeader.MetadataSize = uint32(len(metadata))
	}
	
	// Check buffer capacity
	headerSize := 24
	metadataSize := int(vectorHeader.MetadataSize)
	vectorSize := len(vector.Data) * 4
	totalSize := headerSize + metadataSize + vectorSize
	
	if w.bufferPos+totalSize > len(w.writeBuffer) {
		if err := w.flushBuffer(); err != nil {
			return err
		}
		
		// If still not enough space, write directly
		if totalSize > len(w.writeBuffer) {
			return w.writeVectorDirectly(vector, vectorHeader, metadata)
		}
	}
	
	// Write header to buffer
	binary.LittleEndian.PutUint64(w.writeBuffer[w.bufferPos:w.bufferPos+8], vectorHeader.VectorID)
	binary.LittleEndian.PutUint64(w.writeBuffer[w.bufferPos+8:w.bufferPos+16], uint64(vectorHeader.Timestamp))
	binary.LittleEndian.PutUint32(w.writeBuffer[w.bufferPos+16:w.bufferPos+20], vectorHeader.MetadataSize)
	binary.LittleEndian.PutUint32(w.writeBuffer[w.bufferPos+20:w.bufferPos+24], vectorHeader.VectorSize)
	
	// Calculate and write checksum
	if w.config.EnableChecksum {
		checksum := crc32.ChecksumIEEE(w.writeBuffer[w.bufferPos : w.bufferPos+20])
		binary.LittleEndian.PutUint32(w.writeBuffer[w.bufferPos+20:w.bufferPos+24], checksum)
	}
	
	w.bufferPos += 24
	
	// Write metadata to buffer
	if metadataSize > 0 {
		copy(w.writeBuffer[w.bufferPos:w.bufferPos+metadataSize], metadata)
		w.bufferPos += metadataSize
	}
	
	// Write vector data to buffer
	for i, val := range vector.Data {
		binary.LittleEndian.PutUint32(w.writeBuffer[w.bufferPos+i*4:w.bufferPos+(i+1)*4], math.Float32bits(val))
	}
	w.bufferPos += vectorSize
	
	return nil
}

// writeVectorDirectly writes a vector directly to file (bypassing buffer)
func (w *Writer) writeVectorDirectly(vector *types.Vector, vectorHeader *VectorHeader, metadata []byte) error {
	// Create temporary buffer for this vector
	buf := make([]byte, 24+len(metadata)+len(vector.Data)*4)
	
	// Write header
	binary.LittleEndian.PutUint64(buf[0:8], vectorHeader.VectorID)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(vectorHeader.Timestamp))
	binary.LittleEndian.PutUint32(buf[16:20], vectorHeader.MetadataSize)
	binary.LittleEndian.PutUint32(buf[20:24], vectorHeader.VectorSize)
	
	// Calculate and write checksum
	if w.config.EnableChecksum {
		checksum := crc32.ChecksumIEEE(buf[:20])
		binary.LittleEndian.PutUint32(buf[20:24], checksum)
	}
	
	// Write metadata
	if len(metadata) > 0 {
		copy(buf[24:24+len(metadata)], metadata)
	}
	
	// Write vector data
	for i, val := range vector.Data {
		binary.LittleEndian.PutUint32(buf[24+len(metadata)+i*4:24+len(metadata)+(i+1)*4], math.Float32bits(val))
	}
	
	// Write to file
	if _, err := w.file.Write(buf); err != nil {
		w.metrics.Errors.Inc("storage", "write_failed")
		return fmt.Errorf("%w: %v", ErrWriteFailed, err)
	}
	
	return nil
}

// Flush flushes the write buffer to disk
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	return w.flushBuffer()
}

// flushBuffer flushes the write buffer to disk
func (w *Writer) flushBuffer() error {
	if w.bufferPos == 0 {
		return nil
	}
	
	// Write buffer to file
	if _, err := w.file.Write(w.writeBuffer[:w.bufferPos]); err != nil {
		w.metrics.Errors.Inc("storage", "flush_failed")
		return fmt.Errorf("%w: %v", ErrFlushFailed, err)
	}
	
	// Reset buffer position
	w.bufferPos = 0
	
	return nil
}

// Sync syncs the file to disk
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	if w.file == nil {
		return nil
	}
	
	if err := w.file.Sync(); err != nil {
		w.metrics.Errors.Inc("storage", "sync_failed")
		return fmt.Errorf("%w: %v", ErrSyncFailed, err)
	}
	
	return nil
}

// Close closes the writer
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return nil
	}
	
	// Stop flush timer
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}
	
	// Flush remaining data
	if err := w.flushBuffer(); err != nil {
		w.logger.Error("Failed to flush buffer on close", "error", err)
	}
	
	// Close file
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			w.metrics.Errors.Inc("storage", "close_failed")
			w.logger.Error("Failed to close segment file", "error", err)
			return fmt.Errorf("%w: %v", ErrCloseFailed, err)
		}
	}
	
	// Mark segment as read-only
	if w.segment != nil {
		w.segment.SetReadOnly()
	}
	
	w.closed = true
	
	w.logger.Info("Closed segment writer", "path", w.path)
	
	return nil
}

// startFlushTimer starts the automatic flush timer
func (w *Writer) startFlushTimer() {
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}
	
	w.flushTimer = time.AfterFunc(w.config.FlushInterval, func() {
		if err := w.Flush(); err != nil {
			w.logger.Error("Failed to auto-flush segment", "error", err)
		}
		
		// Restart timer
		w.startFlushTimer()
	})
}

// GetSegment returns the current segment
func (w *Writer) GetSegment() *Segment {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	return w.segment
}

// GetPath returns the current segment path
func (w *Writer) GetPath() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	return w.path
}

// IsClosed returns true if the writer is closed
func (w *Writer) IsClosed() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	return w.closed
}

// GetVectorCount returns the number of vectors in the current segment
func (w *Writer) GetVectorCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	if w.segment == nil {
		return 0
	}
	return w.segment.GetVectorCount()
}

// IsFull returns true if the current segment is full
func (w *Writer) IsFull() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	if w.segment == nil {
		return false
	}
	return w.segment.IsFull()
}

// GetConfig returns the writer configuration
func (w *Writer) GetConfig() *WriterConfig {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	// Return a copy of the config
	config := *w.config
	return &config
}

// UpdateConfig updates the writer configuration
func (w *Writer) UpdateConfig(config *WriterConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	w.config = config
	
	// Update buffer size if needed
	if config.BufferSize != len(w.writeBuffer) {
		newBuffer := make([]byte, config.BufferSize)
		if w.bufferPos > 0 {
			copy(newBuffer, w.writeBuffer[:w.bufferPos])
		}
		w.writeBuffer = newBuffer
	}
	
	// Restart flush timer with new interval
	w.startFlushTimer()
	
	w.logger.Info("Updated writer configuration", "config", config)
	
	return nil
}

// Rotate rotates the current segment (closes current and creates new one)
func (w *Writer) Rotate(clusterID uint32, vectorDim uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	// Close current segment
	if w.segment != nil {
		if err := w.flushBuffer(); err != nil {
			return err
		}
		
		if err := w.file.Close(); err != nil {
			w.metrics.Errors.Inc("storage", "close_failed")
			return fmt.Errorf("%w: %v", ErrCloseFailed, err)
		}
		
		w.segment.SetReadOnly()
	}
	
	// Create new segment
	return w.CreateSegment(clusterID, vectorDim)
}

// GetStats returns writer statistics
func (w *Writer) GetStats() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	stats := make(map[string]interface{})
	stats["closed"] = w.closed
	stats["buffer_size"] = len(w.writeBuffer)
	stats["buffer_pos"] = w.bufferPos
	stats["buffer_usage"] = float64(w.bufferPos) / float64(len(w.writeBuffer)) * 100
	
	if w.segment != nil {
		stats["vector_count"] = w.segment.GetVectorCount()
		stats["cluster_id"] = w.segment.GetClusterID()
		stats["vector_dim"] = w.segment.GetVectorDim()
		stats["segment_full"] = w.segment.IsFull()
		stats["segment_readonly"] = w.segment.IsReadOnly()
		stats["segment_dirty"] = w.segment.IsDirty()
		stats["segment_size"] = w.segment.GetSize()
	}
	
	return stats
}

// Validate validates the writer state
func (w *Writer) Validate() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	if w.closed {
		return ErrWriterClosed
	}
	
	if w.segment == nil {
		return ErrSegmentNotFound
	}
	
	if w.file == nil {
		return errors.New("file handle is nil")
	}
	
	// Validate segment
	if err := w.segment.Validate(); err != nil {
		return fmt.Errorf("segment validation failed: %w", err)
	}
	
	return nil
}