package buffer

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"go.uber.org/zap"
	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrBufferFull     = errors.New("buffer is full")
	ErrBufferClosed   = errors.New("buffer is closed")
	ErrInvalidVector  = errors.New("invalid vector")
	ErrFlushFailed    = errors.New("flush failed")
	ErrVectorNotFound = errors.New("vector not found")
	ErrConfigInvalid  = errors.New("invalid configuration")
)

// BufferEntry represents an entry in the memory buffer
type BufferEntry struct {
	Vector      *types.Vector
	AddedAt     time.Time
	LastAccess  time.Time
	AccessCount int
	Index       int // For heap operations
}

// BufferConfig represents the configuration for the memory buffer
type BufferConfig struct {
	MaxSize           int           `yaml:"max_size" json:"max_size"`                     // Maximum number of vectors in buffer
	MaxMemoryMB       int           `yaml:"max_memory_mb" json:"max_memory_mb"`           // Maximum memory usage in MB
	FlushThreshold    float64       `yaml:"flush_threshold" json:"flush_threshold"`       // Flush when buffer reaches this percentage (0.0-1.0)
	FlushInterval     time.Duration `yaml:"flush_interval" json:"flush_interval"`         // Auto-flush interval
	EvictionPolicy    string        `yaml:"eviction_policy" json:"eviction_policy"`       // lru, lfu, fifo
	EnableCompression bool          `yaml:"enable_compression" json:"enable_compression"` // Enable compression for buffered vectors
	PreallocateSize   int           `yaml:"preallocate_size" json:"preallocate_size"`     // Pre-allocate buffer size
	EnableStats       bool          `yaml:"enable_stats" json:"enable_stats"`             // Enable detailed statistics
}

// DefaultBufferConfig returns the default buffer configuration
func DefaultBufferConfig() *BufferConfig {
	return &BufferConfig{
		MaxSize:           100000,
		MaxMemoryMB:       512,
		FlushThreshold:    0.8,
		FlushInterval:     30 * time.Second,
		EvictionPolicy:    "lru",
		EnableCompression: false,
		PreallocateSize:   10000,
		EnableStats:       true,
	}
}

// Buffer represents a memory buffer for vectors
type Buffer struct {
	config      *BufferConfig
	vectors     map[string]*BufferEntry
	accessOrder *AccessOrderHeap
	mu          sync.RWMutex
	closed      bool
	flushChan   chan struct{}
	logger      logging.Logger
	metrics     *metrics.StorageMetrics
	flushFunc   func([]*types.Vector) error
	stats       *BufferStats
}

// Manager is an alias for Buffer for backwards compatibility.
type Manager = Buffer

// BufferStats represents buffer statistics
type BufferStats struct {
	TotalVectors  int64     `json:"total_vectors"`
	TotalMemoryMB float64   `json:"total_memory_mb"`
	HitCount      int64     `json:"hit_count"`
	MissCount     int64     `json:"miss_count"`
	EvictionCount int64     `json:"eviction_count"`
	FlushCount    int64     `json:"flush_count"`
	LastFlushAt   time.Time `json:"last_flush_at"`
	AvgAccessTime float64   `json:"avg_access_time"`
	MemoryUsage   float64   `json:"memory_usage"`
	BufferUsage   float64   `json:"buffer_usage"`
}

// NewBuffer creates a new memory buffer
func NewBuffer(cfg config.Config, logger logging.Logger, metrics *metrics.StorageMetrics, flushFunc func([]*types.Vector) error) (*Buffer, error) {
	bufferConfig := DefaultBufferConfig()

	if cfg != nil {
		if bufferCfg, ok := cfg.Get("buffer"); ok {
			if cfgMap, ok := bufferCfg.(map[string]interface{}); ok {
				if maxSize, ok := cfgMap["max_size"].(int); ok {
					bufferConfig.MaxSize = maxSize
				}
				if maxMemoryMB, ok := cfgMap["max_memory_mb"].(int); ok {
					bufferConfig.MaxMemoryMB = maxMemoryMB
				}
				if flushThreshold, ok := cfgMap["flush_threshold"].(float64); ok {
					bufferConfig.FlushThreshold = flushThreshold
				}
				if flushInterval, ok := cfgMap["flush_interval"].(string); ok {
					if dur, err := time.ParseDuration(flushInterval); err == nil {
						bufferConfig.FlushInterval = dur
					}
				}
				if evictionPolicy, ok := cfgMap["eviction_policy"].(string); ok {
					bufferConfig.EvictionPolicy = evictionPolicy
				}
				if enableCompression, ok := cfgMap["enable_compression"].(bool); ok {
					bufferConfig.EnableCompression = enableCompression
				}
				if preallocateSize, ok := cfgMap["preallocate_size"].(int); ok {
					bufferConfig.PreallocateSize = preallocateSize
				}
				if enableStats, ok := cfgMap["enable_stats"].(bool); ok {
					bufferConfig.EnableStats = enableStats
				}
			}
		}
	}

	// Validate configuration
	if err := validateBufferConfig(bufferConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConfigInvalid, err)
	}

	buffer := &Buffer{
		config:      bufferConfig,
		vectors:     make(map[string]*BufferEntry),
		accessOrder: NewAccessOrderHeap(bufferConfig.EvictionPolicy),
		flushChan:   make(chan struct{}, 1),
		logger:      logger,
		metrics:     metrics,
		flushFunc:   flushFunc,
		stats:       &BufferStats{},
	}

	// Pre-allocate if configured
	if bufferConfig.PreallocateSize > 0 {
		buffer.vectors = make(map[string]*BufferEntry, bufferConfig.PreallocateSize)
	}

	// Start auto-flush goroutine
	go buffer.autoFlush()

	buffer.logger.Info("Created memory buffer",
		zap.Int("max_size", bufferConfig.MaxSize),
		zap.Int("max_memory_mb", bufferConfig.MaxMemoryMB),
		zap.Float64("flush_threshold", bufferConfig.FlushThreshold),
		zap.String("eviction_policy", bufferConfig.EvictionPolicy))

	return buffer, nil
}

// validateBufferConfig validates the buffer configuration
func validateBufferConfig(cfg *BufferConfig) error {
	if cfg.MaxSize <= 0 {
		return errors.New("max_size must be positive")
	}
	if cfg.MaxMemoryMB <= 0 {
		return errors.New("max_memory_mb must be positive")
	}
	if cfg.FlushThreshold <= 0 || cfg.FlushThreshold > 1 {
		return errors.New("flush_threshold must be between 0 and 1")
	}
	if cfg.FlushInterval <= 0 {
		return errors.New("flush_interval must be positive")
	}
	if cfg.EvictionPolicy != "lru" && cfg.EvictionPolicy != "lfu" && cfg.EvictionPolicy != "fifo" {
		return errors.New("eviction_policy must be lru, lfu, or fifo")
	}
	return nil
}

// Add adds a vector to the buffer
func (b *Buffer) Add(vector *types.Vector) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBufferClosed
	}

	// Validate vector
	if err := vector.ValidateWithConfig(nil); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidVector, err)
	}

	// Check if buffer is full
	if b.shouldEvict() {
		if err := b.evict(); err != nil {
			return fmt.Errorf("failed to evict vectors: %w", err)
		}
	}

	// Check if vector already exists
	if entry, exists := b.vectors[vector.ID]; exists {
		// Update existing entry
		entry.Vector = vector
		entry.LastAccess = time.Now()
		entry.AccessCount++
		heap.Fix(b.accessOrder, entry.Index)
		return nil
	}

	// Create new entry
	now := time.Now()
	entry := &BufferEntry{
		Vector:      vector,
		AddedAt:     now,
		LastAccess:  now,
		AccessCount: 1,
	}

	// Add to buffer
	b.vectors[vector.ID] = entry
	heap.Push(b.accessOrder, entry)

	// Update stats
	b.stats.TotalVectors++
	b.updateMemoryUsage()
	b.updateBufferUsage()

	// Check if we need to flush
	if b.shouldFlush() {
		select {
		case b.flushChan <- struct{}{}:
		default:
			// Flush already pending
		}
	}

	// Update metrics
	b.metrics.BufferOperations.Inc("add", "buffer")
	b.metrics.BufferSize.Set(float64(len(b.vectors)))

	return nil
}

// Get retrieves a vector from the buffer
func (b *Buffer) Get(vectorID string) (*types.Vector, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBufferClosed
	}

	entry, exists := b.vectors[vectorID]
	if !exists {
		b.stats.MissCount++
		b.metrics.BufferMisses.Inc("miss")
		return nil, ErrVectorNotFound
	}

	// Update access information
	entry.LastAccess = time.Now()
	entry.AccessCount++
	heap.Fix(b.accessOrder, entry.Index)

	// Update stats
	b.stats.HitCount++
	b.metrics.BufferHits.Inc("hit")

	return entry.Vector, nil
}

// Remove removes a vector from the buffer
func (b *Buffer) Remove(vectorID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBufferClosed
	}

	entry, exists := b.vectors[vectorID]
	if !exists {
		return ErrVectorNotFound
	}

	// Remove from buffer
	delete(b.vectors, vectorID)
	heap.Remove(b.accessOrder, entry.Index)

	// Update stats
	b.stats.TotalVectors--
	b.updateMemoryUsage()
	b.updateBufferUsage()

	// Update metrics
	b.metrics.BufferOperations.Inc("remove", "buffer")
	b.metrics.BufferSize.Set(float64(len(b.vectors)))

	return nil
}

// Flush flushes all vectors from the buffer
func (b *Buffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBufferClosed
	}

	if len(b.vectors) == 0 {
		return nil
	}

	return b.flush()
}

// flush flushes all vectors from the buffer (internal, assumes lock is held)
func (b *Buffer) flush() error {
	if b.flushFunc == nil {
		return errors.New("flush function not set")
	}

	// Collect all vectors
	vectors := make([]*types.Vector, 0, len(b.vectors))
	for _, entry := range b.vectors {
		vectors = append(vectors, entry.Vector)
	}

	// Call flush function
	if err := b.flushFunc(vectors); err != nil {
		b.metrics.Errors.Inc("buffer", "flush_failed")
		return fmt.Errorf("%w: %v", ErrFlushFailed, err)
	}

	// Clear buffer
	b.vectors = make(map[string]*BufferEntry)
	b.accessOrder = NewAccessOrderHeap(b.config.EvictionPolicy)

	// Update stats
	b.stats.FlushCount++
	b.stats.LastFlushAt = time.Now()
	b.stats.TotalVectors = 0
	b.updateMemoryUsage()
	b.updateBufferUsage()

	// Update metrics
	b.metrics.BufferOperations.Inc("flush", "buffer")
	b.metrics.BufferSize.Set(0)

	b.logger.Info("Flushed buffer", zap.Int("vectors", len(vectors)))

	return nil
}

// shouldEvict returns true if we should evict vectors
func (b *Buffer) shouldEvict() bool {
	// Check size limit
	if len(b.vectors) >= b.config.MaxSize {
		return true
	}

	// Check memory limit
	if b.stats.MemoryUsage >= float64(b.config.MaxMemoryMB) {
		return true
	}

	return false
}

// evict evicts vectors based on the eviction policy
func (b *Buffer) evict() error {
	if b.accessOrder.Len() == 0 {
		return nil
	}

	// Evict 10% of vectors or enough to get under limits
	evictCount := max(1, len(b.vectors)/10)

	for i := 0; i < evictCount && b.accessOrder.Len() > 0; i++ {
		entry := heap.Pop(b.accessOrder).(*BufferEntry)
		delete(b.vectors, entry.Vector.ID)
		b.stats.EvictionCount++
	}

	// Update stats
	b.stats.TotalVectors = int64(len(b.vectors))
	b.updateMemoryUsage()
	b.updateBufferUsage()

	// Update metrics
	b.metrics.BufferOperations.Inc("evict", "buffer")
	b.metrics.BufferSize.Set(float64(len(b.vectors)))

	return nil
}

// shouldFlush returns true if we should flush
func (b *Buffer) shouldFlush() bool {
	return b.stats.BufferUsage >= b.config.FlushThreshold
}

// autoFlush handles automatic flushing
func (b *Buffer) autoFlush() {
	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			if !b.closed && len(b.vectors) > 0 {
				if err := b.flush(); err != nil {
					b.logger.Error("Failed to auto-flush buffer", zap.Error(err))
				}
			}
			b.mu.Unlock()

		case <-b.flushChan:
			b.mu.Lock()
			if !b.closed && len(b.vectors) > 0 {
				if err := b.flush(); err != nil {
					b.logger.Error("Failed to flush buffer", zap.Error(err))
				}
			}
			b.mu.Unlock()
		}
	}
}

// updateMemoryUsage updates the memory usage statistics
func (b *Buffer) updateMemoryUsage() {
	if !b.config.EnableStats {
		return
	}

	var totalMemory int64
	for _, entry := range b.vectors {
		// Estimate memory usage
		vectorSize := int64(len(entry.Vector.Data) * 4) // float32 = 4 bytes
		metadataSize := int64(0)
		if entry.Vector.Metadata != nil {
			// Estimate metadata size (simplified)
			metadataSize = int64(len(entry.Vector.Metadata) * 16) // Approximate
		}
		totalMemory += vectorSize + metadataSize + int64(unsafe.Sizeof(BufferEntry{}))
	}

	b.stats.MemoryUsage = float64(totalMemory) / (1024 * 1024) // Convert to MB
}

// updateBufferUsage updates the buffer usage statistics
func (b *Buffer) updateBufferUsage() {
	if !b.config.EnableStats {
		return
	}

	b.stats.BufferUsage = float64(len(b.vectors)) / float64(b.config.MaxSize)
}

// Close closes the buffer
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	// Flush remaining vectors
	if len(b.vectors) > 0 {
		if err := b.flush(); err != nil {
			b.logger.Error("Failed to flush buffer on close", zap.Error(err))
		}
	}

	b.closed = true
	close(b.flushChan)

	b.logger.Info("Closed memory buffer")

	return nil
}

// GetSize returns the number of vectors in the buffer
func (b *Buffer) GetSize() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.vectors)
}

// GetMemoryUsage returns the memory usage in MB
func (b *Buffer) GetMemoryUsage() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.stats.MemoryUsage
}

// GetUsage returns the buffer usage percentage
func (b *Buffer) GetUsage() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.stats.BufferUsage
}

// GetStats returns buffer statistics
func (b *Buffer) GetStats() *BufferStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return a copy of stats
	stats := *b.stats
	return &stats
}

// GetConfig returns the buffer configuration
func (b *Buffer) GetConfig() *BufferConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return a copy of config
	config := *b.config
	return &config
}

// UpdateConfig updates the buffer configuration
func (b *Buffer) UpdateConfig(config *BufferConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBufferClosed
	}

	// Validate new configuration
	if err := validateBufferConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrConfigInvalid, err)
	}

	b.config = config

	// Update access order heap if eviction policy changed
	if b.accessOrder.policy != config.EvictionPolicy {
		b.accessOrder = NewAccessOrderHeap(config.EvictionPolicy)
		// Rebuild heap with existing entries
		for _, entry := range b.vectors {
			heap.Push(b.accessOrder, entry)
		}
	}

	b.logger.Info("Updated buffer configuration", zap.Any("config", config))

	return nil
}

// IsClosed returns true if the buffer is closed
func (b *Buffer) IsClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.closed
}

// GetVectorIDs returns all vector IDs in the buffer
func (b *Buffer) GetVectorIDs() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.vectors))
	for id := range b.vectors {
		ids = append(ids, id)
	}

	return ids
}

// HasVector checks if a vector exists in the buffer
func (b *Buffer) HasVector(vectorID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, exists := b.vectors[vectorID]
	return exists
}

// Clear clears all vectors from the buffer
func (b *Buffer) Clear() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBufferClosed
	}

	b.vectors = make(map[string]*BufferEntry)
	b.accessOrder = NewAccessOrderHeap(b.config.EvictionPolicy)

	// Update stats
	b.stats.TotalVectors = 0
	b.updateMemoryUsage()
	b.updateBufferUsage()

	// Update metrics
	b.metrics.BufferOperations.Inc("clear", "buffer")
	b.metrics.BufferSize.Set(0)

	return nil
}

// Validate validates the buffer state
func (b *Buffer) Validate() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrBufferClosed
	}

	// Validate vector count
	if int64(len(b.vectors)) != b.stats.TotalVectors {
		return errors.New("vector count mismatch")
	}

	// Validate all vectors
	for _, entry := range b.vectors {
		if err := entry.Vector.ValidateWithConfig(nil); err != nil {
			return fmt.Errorf("invalid vector %s: %w", entry.Vector.ID, err)
		}
	}

	return nil
}

// AccessOrderHeap implements a heap for access order tracking
type AccessOrderHeap struct {
	entries []*BufferEntry
	policy  string
}

// NewAccessOrderHeap creates a new access order heap
func NewAccessOrderHeap(policy string) *AccessOrderHeap {
	return &AccessOrderHeap{
		entries: make([]*BufferEntry, 0),
		policy:  policy,
	}
}

// Len implements heap.Interface
func (h *AccessOrderHeap) Len() int { return len(h.entries) }

// Less implements heap.Interface
func (h *AccessOrderHeap) Less(i, j int) bool {
	switch h.policy {
	case "lru":
		return h.entries[i].LastAccess.Before(h.entries[j].LastAccess)
	case "lfu":
		return h.entries[i].AccessCount < h.entries[j].AccessCount
	case "fifo":
		return h.entries[i].AddedAt.Before(h.entries[j].AddedAt)
	default:
		return h.entries[i].LastAccess.Before(h.entries[j].LastAccess)
	}
}

// Swap implements heap.Interface
func (h *AccessOrderHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].Index = i
	h.entries[j].Index = j
}

// Push implements heap.Interface
func (h *AccessOrderHeap) Push(x interface{}) {
	entry := x.(*BufferEntry)
	entry.Index = len(h.entries)
	h.entries = append(h.entries, entry)
}

// Pop implements heap.Interface
func (h *AccessOrderHeap) Pop() interface{} {
	old := h.entries
	n := len(old)
	entry := old[n-1]
	entry.Index = -1 // for safety
	h.entries = old[0 : n-1]
	return entry
}

// Update updates an entry in the heap
func (h *AccessOrderHeap) Update(index int) {
	if index >= 0 && index < len(h.entries) {
		heap.Fix(h, index)
	}
}

// Remove removes an entry from the heap
func (h *AccessOrderHeap) Remove(index int) {
	heap.Remove(h, index)
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
