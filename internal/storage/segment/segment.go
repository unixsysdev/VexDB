// Package segment provides segment functionality for VxDB
package segment

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"vxdb/internal/config"
	"vxdb/internal/metrics"
	"vxdb/internal/storage/compression"
	"vxdb/internal/types"

	"go.uber.org/zap"
)

// Segment represents a single segment in the storage system
type Segment struct {
	info       *SegmentInfo
	config     *config.Config
	logger     *zap.Logger
	metrics    *metrics.Collector
	compressor *compression.Compressor

	file   *os.File
	writer *Writer
	reader *Reader

	mu     sync.RWMutex
	closed bool
}

// NewSegment creates a new segment
func NewSegment(info *SegmentInfo, cfg *config.Config, logger *zap.Logger, metrics *metrics.Collector, compressor *compression.Compressor) (*Segment, error) {
	s := &Segment{
		info:       info,
		config:     cfg,
		logger:     logger,
		metrics:    metrics,
		compressor: compressor,
	}

	// Initialize segment file
	if err := s.initializeFile(); err != nil {
		return nil, err
	}

	// Initialize writer and reader
	if err := s.initializeComponents(); err != nil {
		s.Close()
		return nil, err
	}

	return s, nil
}

// initializeFile initializes the segment file
func (s *Segment) initializeFile() error {
	// Create segment directory if it doesn't exist
	segmentDir := filepath.Join("./data", "segments", s.info.ClusterIDString())
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return fmt.Errorf("failed to create segment directory: %w", err)
	}

	// Create segment file path
	segmentPath := filepath.Join(segmentDir, s.info.ID+".seg")

	// Open or create segment file
	var err error
	s.file, err = os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}

	// If file is new, write header
	if info, err := s.file.Stat(); err == nil && info.Size() == 0 {
		if err := s.writeHeader(); err != nil {
			return fmt.Errorf("failed to write segment header: %w", err)
		}
	} else {
		// Read existing header
		if err := s.readHeader(); err != nil {
			return fmt.Errorf("failed to read segment header: %w", err)
		}
	}

	return nil
}

// initializeComponents initializes the writer and reader components
func (s *Segment) initializeComponents() error {
	var err error

	// Initialize writer
	s.writer, err = NewWriter(s.config, s.logger, s.metrics, s.compressor, s.file)
	if err != nil {
		return err
	}

	// Initialize reader
	s.reader, err = NewReader(s.config, s.logger, s.metrics, s.compressor, s.file)
	if err != nil {
		return err
	}

	return nil
}

// writeHeader writes the segment header to the file
func (s *Segment) writeHeader() error {
	header := &SegmentHeader{
		Magic:      SegmentMagic,
		Version:    SegmentVersion,
		ClusterID:  s.info.ClusterID,
		SegmentID:  s.info.ID,
		Status:     string(s.info.Status),
		VectorSize: 0,
		CreatedAt:  s.info.CreatedAt,
		UpdatedAt:  s.info.UpdatedAt,
	}

	// Write header at the beginning of the file
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Write(s.file, binary.LittleEndian, header)
}

// readHeader reads the segment header from the file
func (s *Segment) readHeader() error {
	var header SegmentHeader

	// Read header from the beginning of the file
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := binary.Read(s.file, binary.LittleEndian, &header); err != nil {
		return err
	}

	// Validate header
	if header.Magic != SegmentMagic {
		return types.ErrInvalidSegmentFormat
	}

	if header.Version != SegmentVersion {
		return types.ErrUnsupportedSegmentVersion
	}

	// Update segment info from header
	s.info.ClusterID = header.ClusterID
	s.info.Status = SegmentStatus(header.Status)
	s.info.CreatedAt = header.CreatedAt
	s.info.UpdatedAt = header.UpdatedAt

	return nil
}

// AddVector adds a vector to the segment
func (s *Segment) AddVector(ctx context.Context, vector *types.Vector) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrSegmentClosed
	}

	// Write vector using writer
	if err := s.writer.WriteVector(vector); err != nil {
		return err
	}

	// Update segment info
	s.info.VectorCount++
	s.info.UpdatedAt = types.NowTimestamp()

	return nil
}

// GetVectors retrieves all vectors from the segment
func (s *Segment) GetVectors(ctx context.Context) ([]*types.Vector, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrSegmentClosed
	}

	// Read all vectors using reader
	return s.reader.ReadAllVectors()
}

// SearchVectors performs a similarity search within the segment
func (s *Segment) SearchVectors(ctx context.Context, query *types.Vector, k int) ([]*types.SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, types.ErrSegmentClosed
	}

	// Get all vectors
	vectors, err := s.reader.ReadAllVectors()
	if err != nil {
		return nil, err
	}

	// Perform linear search (euclidean by default)
	results := make([]*types.SearchResult, 0, len(vectors))
	for _, vector := range vectors {
		distance, err := query.Distance(vector, "euclidean")
		if err != nil {
			continue
		}
		results = append(results, &types.SearchResult{
			Vector:   vector,
			Distance: distance,
		})
	}

	// Sort by distance (ascending)
	types.SortSearchResults(results)

	// Return top-k results
	if len(results) > k {
		results = results[:k]
	}

	return results, nil
}

// Close closes the segment
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.logger.Info("Closing segment", zap.String("segment_id", s.info.ID))

	// Close writer and reader
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			s.logger.Error("Failed to close writer", zap.Error(err))
		}
	}

	if s.reader != nil {
		if err := s.reader.Close(); err != nil {
			s.logger.Error("Failed to close reader", zap.Error(err))
		}
	}

	// Close file
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			s.logger.Error("Failed to close segment file", zap.Error(err))
		}
	}

	s.closed = true
	s.logger.Info("Segment closed successfully", zap.String("segment_id", s.info.ID))
	return nil
}

// GetID returns the segment ID
func (s *Segment) GetID() string {
	return s.info.ID
}

// GetClusterID returns the cluster ID
func (s *Segment) GetClusterID() uint32 {
	return s.info.ClusterID
}

// GetVectorCount returns the number of vectors in the segment
func (s *Segment) GetVectorCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.info.VectorCount
}

// GetStatus returns the segment status
func (s *Segment) GetStatus() *types.SegmentStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &types.SegmentStatus{
		ID:          s.info.ID,
		ClusterID:   s.info.ClusterID,
		Status:      string(s.info.Status),
		VectorCount: s.info.VectorCount,
		CreatedAt:   time.Unix(int64(s.info.CreatedAt), 0),
		UpdatedAt:   time.Unix(int64(s.info.UpdatedAt), 0),
	}
}

// Flush flushes any pending data to disk
func (s *Segment) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrSegmentClosed
	}

	if s.writer != nil {
		return s.writer.Flush()
	}

	return nil
}

// Sync syncs the segment file to disk
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return types.ErrSegmentClosed
	}

	if s.file != nil {
		return s.file.Sync()
	}

	return nil
}
