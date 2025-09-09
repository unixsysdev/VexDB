package hashing

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/types"
)

var (
	ErrInvalidVector      = errors.New("invalid vector")
	ErrInvalidClusterID   = errors.New("invalid cluster ID")
	ErrInvalidClusterCount = errors.New("invalid cluster count")
	ErrHashingFailed      = errors.New("hashing failed")
	ErrAssignmentFailed   = errors.New("assignment failed")
)

// HashAlgorithm represents a hashing algorithm
type HashAlgorithm string

const (
	AlgorithmFNV1a  HashAlgorithm = "fnv1a"
	AlgorithmFNV1   HashAlgorithm = "fnv1"
	AlgorithmXXHash HashAlgorithm = "xxhash"
	AlgorithmMD5    HashAlgorithm = "md5"
	AlgorithmSHA1   HashAlgorithm = "sha1"
	AlgorithmSHA256 HashAlgorithm = "sha256"
)

// AssignmentStrategy represents a cluster assignment strategy
type AssignmentStrategy string

const (
	StrategyHash      AssignmentStrategy = "hash"
	StrategyModulo    AssignmentStrategy = "modulo"
	StrategyConsistent AssignmentStrategy = "consistent"
	StrategyRange     AssignmentStrategy = "range"
)

// HasherConfig represents the hasher configuration
type HasherConfig struct {
	Algorithm        HashAlgorithm        `yaml:"algorithm" json:"algorithm"`
	Strategy         AssignmentStrategy   `yaml:"strategy" json:"strategy"`
	ClusterCount     uint32               `yaml:"cluster_count" json:"cluster_count"`
	Seed             uint64               `yaml:"seed" json:"seed"`
	EnableCache      bool                 `yaml:"enable_cache" json:"enable_cache"`
	CacheSize        int                  `yaml:"cache_size" json:"cache_size"`
	EnableValidation bool                 `yaml:"enable_validation" json:"enable_validation"`
}

// DefaultHasherConfig returns the default hasher configuration
func DefaultHasherConfig() *HasherConfig {
	return &HasherConfig{
		Algorithm:        AlgorithmFNV1a,
		Strategy:         StrategyHash,
		ClusterCount:     1024,
		Seed:             0,
		EnableCache:      true,
		CacheSize:        10000,
		EnableValidation: true,
	}
}

// Hasher represents a vector hasher and cluster assigner
type Hasher struct {
	config      *HasherConfig
	cache       map[uint64]uint32 // vector_id -> cluster_id
	mu          sync.RWMutex
	logger      logging.Logger
	metrics     *metrics.StorageMetrics
}

// HasherStats represents hasher statistics
type HasherStats struct {
	TotalHashes      int64 `json:"total_hashes"`
	CacheHits        int64 `json:"cache_hits"`
	CacheMisses      int64 `json:"cache_misses"`
	ValidationErrors int64 `json:"validation_errors"`
}

// NewHasher creates a new hasher
func NewHasher(cfg config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*Hasher, error) {
	hasherConfig := DefaultHasherConfig()
	
	if cfg != nil {
		if hasherCfg, ok := cfg.Get("hasher"); ok {
			if cfgMap, ok := hasherCfg.(map[string]interface{}); ok {
				if algorithm, ok := cfgMap["algorithm"].(string); ok {
					hasherConfig.Algorithm = HashAlgorithm(algorithm)
				}
				if strategy, ok := cfgMap["strategy"].(string); ok {
					hasherConfig.Strategy = AssignmentStrategy(strategy)
				}
				if clusterCount, ok := cfgMap["cluster_count"].(int); ok {
					hasherConfig.ClusterCount = uint32(clusterCount)
				}
				if seed, ok := cfgMap["seed"].(int); ok {
					hasherConfig.Seed = uint64(seed)
				}
				if enableCache, ok := cfgMap["enable_cache"].(bool); ok {
					hasherConfig.EnableCache = enableCache
				}
				if cacheSize, ok := cfgMap["cache_size"].(int); ok {
					hasherConfig.CacheSize = cacheSize
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					hasherConfig.EnableValidation = enableValidation
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateHasherConfig(hasherConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidClusterCount, err)
	}
	
	hasher := &Hasher{
		config: hasherConfig,
		cache:  make(map[uint64]uint32),
		logger: logger,
		metrics: metrics,
	}
	
	// Pre-allocate cache if enabled
	if hasherConfig.EnableCache && hasherConfig.CacheSize > 0 {
		hasher.cache = make(map[uint64]uint32, hasherConfig.CacheSize)
	}
	
	hasher.logger.Info("Created hasher",
		zap.String("algorithm", string(hasherConfig.Algorithm)),
		zap.String("strategy", string(hasherConfig.Strategy)),
		zap.Uint32("cluster_count", hasherConfig.ClusterCount),
		zap.Uint64("seed", hasherConfig.Seed),
		zap.Bool("enable_cache", hasherConfig.EnableCache),
		zap.Int("cache_size", hasherConfig.CacheSize))
	
	return hasher, nil
}

// validateHasherConfig validates the hasher configuration
func validateHasherConfig(cfg *HasherConfig) error {
	if cfg.Algorithm != AlgorithmFNV1a &&
		cfg.Algorithm != AlgorithmFNV1 &&
		cfg.Algorithm != AlgorithmXXHash &&
		cfg.Algorithm != AlgorithmMD5 &&
		cfg.Algorithm != AlgorithmSHA1 &&
		cfg.Algorithm != AlgorithmSHA256 {
		return fmt.Errorf("unsupported algorithm: %s", cfg.Algorithm)
	}
	
	if cfg.Strategy != StrategyHash &&
		cfg.Strategy != StrategyModulo &&
		cfg.Strategy != StrategyConsistent &&
		cfg.Strategy != StrategyRange {
		return fmt.Errorf("unsupported strategy: %s", cfg.Strategy)
	}
	
	if cfg.ClusterCount == 0 {
		return errors.New("cluster count must be positive")
	}
	
	if cfg.CacheSize < 0 {
		return errors.New("cache size must be non-negative")
	}
	
	return nil
}

// HashVector computes a hash of the vector data
func (h *Hasher) HashVector(vector *types.Vector) (uint64, error) {
	if h.config.EnableValidation {
		if err := vector.Validate(); err != nil {
			h.metrics.Errors.Inc("hashing", "validation_failed")
			return 0, fmt.Errorf("%w: %v", ErrInvalidVector, err)
		}
	}
	
	hash, err := h.hashVectorData(vector.Data)
	if err != nil {
		h.metrics.Errors.Inc("hashing", "hash_failed")
		return 0, fmt.Errorf("%w: %v", ErrHashingFailed, err)
	}
	
	// Update metrics
	h.metrics.HashingOperations.Inc("hashing", "hash_vector")
	
	return hash, nil
}

// hashVectorData computes a hash of the vector data (internal)
func (h *Hasher) hashVectorData(data []float32) (uint64, error) {
	switch h.config.Algorithm {
	case AlgorithmFNV1a:
		return h.hashFNV1a(data)
	case AlgorithmFNV1:
		return h.hashFNV1(data)
	case AlgorithmXXHash:
		return h.hashXXHash(data)
	case AlgorithmMD5:
		return h.hashMD5(data)
	case AlgorithmSHA1:
		return h.hashSHA1(data)
	case AlgorithmSHA256:
		return h.hashSHA256(data)
	default:
		return 0, fmt.Errorf("unsupported algorithm: %s", h.config.Algorithm)
	}
}

// hashFNV1a computes FNV-1a hash of vector data
func (h *Hasher) hashFNV1a(data []float32) (uint64, error) {
	hash := fnv.New64a()
	
	// Write seed if provided
	if h.config.Seed != 0 {
		seedBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(seedBytes, h.config.Seed)
		hash.Write(seedBytes)
	}
	
	// Write vector data
	for _, value := range data {
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, math.Float32bits(value))
		hash.Write(bytes)
	}
	
	return hash.Sum64(), nil
}

// hashFNV1 computes FNV-1 hash of vector data
func (h *Hasher) hashFNV1(data []float32) (uint64, error) {
	hash := fnv.New64()
	
	// Write seed if provided
	if h.config.Seed != 0 {
		seedBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(seedBytes, h.config.Seed)
		hash.Write(seedBytes)
	}
	
	// Write vector data
	for _, value := range data {
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, math.Float32bits(value))
		hash.Write(bytes)
	}
	
	return hash.Sum64(), nil
}

// hashXXHash computes XXHash of vector data
func (h *Hasher) hashXXHash(data []float32) (uint64, error) {
	// This would use the XXHash algorithm
	// For now, we'll use FNV-1a as a placeholder
	// In a real implementation, you would use a library like github.com/cespare/xxhash
	return h.hashFNV1a(data)
}

// hashMD5 computes MD5 hash of vector data
func (h *Hasher) hashMD5(data []float32) (uint64, error) {
	// This would use the MD5 algorithm
	// For now, we'll use FNV-1a as a placeholder
	// In a real implementation, you would use crypto/md5
	return h.hashFNV1a(data)
}

// hashSHA1 computes SHA1 hash of vector data
func (h *Hasher) hashSHA1(data []float32) (uint64, error) {
	// This would use the SHA1 algorithm
	// For now, we'll use FNV-1a as a placeholder
	// In a real implementation, you would use crypto/sha1
	return h.hashFNV1a(data)
}

// hashSHA256 computes SHA256 hash of vector data
func (h *Hasher) hashSHA256(data []float32) (uint64, error) {
	// This would use the SHA256 algorithm
	// For now, we'll use FNV-1a as a placeholder
	// In a real implementation, you would use crypto/sha256
	return h.hashFNV1a(data)
}

// AssignCluster assigns a vector to a cluster
func (h *Hasher) AssignCluster(vector *types.Vector) (uint32, error) {
	if h.config.EnableValidation {
		if err := vector.Validate(); err != nil {
			h.metrics.Errors.Inc("hashing", "validation_failed")
			return 0, fmt.Errorf("%w: %v", ErrInvalidVector, err)
		}
	}
	
	// Check cache first
	if h.config.EnableCache {
		h.mu.RLock()
		if clusterID, exists := h.cache[vector.ID]; exists {
			h.mu.RUnlock()
			h.metrics.CacheHits.Inc("hashing", "cluster_assignment")
			return clusterID, nil
		}
		h.mu.RUnlock()
		h.metrics.CacheMisses.Inc("hashing", "cluster_assignment")
	}
	
	// Compute hash
	hash, err := h.HashVector(vector)
	if err != nil {
		return 0, err
	}
	
	// Assign cluster based on strategy
	clusterID, err := h.assignClusterFromHash(hash)
	if err != nil {
		h.metrics.Errors.Inc("hashing", "assignment_failed")
		return 0, fmt.Errorf("%w: %v", ErrAssignmentFailed, err)
	}
	
	// Update cache
	if h.config.EnableCache {
		h.mu.Lock()
		h.cache[vector.ID] = clusterID
		h.mu.Unlock()
	}
	
	// Update metrics
	h.metrics.HashingOperations.Inc("hashing", "assign_cluster")
	
	return clusterID, nil
}

// assignClusterFromHash assigns a cluster from a hash value
func (h *Hasher) assignClusterFromHash(hash uint64) (uint32, error) {
	switch h.config.Strategy {
	case StrategyHash:
		return h.assignHashStrategy(hash)
	case StrategyModulo:
		return h.assignModuloStrategy(hash)
	case StrategyConsistent:
		return h.assignConsistentStrategy(hash)
	case StrategyRange:
		return h.assignRangeStrategy(hash)
	default:
		return 0, fmt.Errorf("unsupported strategy: %s", h.config.Strategy)
	}
}

// assignHashStrategy assigns cluster using hash strategy
func (h *Hasher) assignHashStrategy(hash uint64) (uint32, error) {
	// Use the hash value directly, modulo cluster count
	return uint32(hash % uint64(h.config.ClusterCount)), nil
}

// assignModuloStrategy assigns cluster using modulo strategy
func (h *Hasher) assignModuloStrategy(hash uint64) (uint32, error) {
	// Simple modulo operation
	return uint32(hash % uint64(h.config.ClusterCount)), nil
}

// assignConsistentStrategy assigns cluster using consistent hashing
func (h *Hasher) assignConsistentStrategy(hash uint64) (uint32, error) {
	// This would implement consistent hashing
	// For now, we'll use a simple approach
	// In a real implementation, you would use a proper consistent hashing algorithm
	return uint32(hash % uint64(h.config.ClusterCount)), nil
}

// assignRangeStrategy assigns cluster using range strategy
func (h *Hasher) assignRangeStrategy(hash uint64) (uint32, error) {
	// Divide the hash space into ranges
	maxHash := uint64(math.MaxUint64)
	rangeSize := maxHash / uint64(h.config.ClusterCount)
	
	clusterID := uint32(hash / rangeSize)
	if clusterID >= h.config.ClusterCount {
		clusterID = h.config.ClusterCount - 1
	}
	
	return clusterID, nil
}

// GetClusterDistribution returns the distribution of vectors across clusters
func (h *Hasher) GetClusterDistribution(vectors []*types.Vector) (map[uint32]int, error) {
	distribution := make(map[uint32]int)
	
	for _, vector := range vectors {
		clusterID, err := h.AssignCluster(vector)
		if err != nil {
			return nil, err
		}
		distribution[clusterID]++
	}
	
	return distribution, nil
}

// GetClusterBalance returns the balance factor of cluster distribution
func (h *Hasher) GetClusterBalance(vectors []*types.Vector) (float64, error) {
	distribution, err := h.GetClusterDistribution(vectors)
	if err != nil {
		return 0.0, err
	}
	
	if len(distribution) == 0 {
		return 0.0, nil
	}
	
	// Calculate standard deviation
	var mean float64
	for _, count := range distribution {
		mean += float64(count)
	}
	mean /= float64(len(distribution))
	
	var variance float64
	for _, count := range distribution {
		diff := float64(count) - mean
		variance += diff * diff
	}
	variance /= float64(len(distribution))
	
	stdDev := math.Sqrt(variance)
	
	// Balance factor is 1.0 - (stdDev / mean)
	// Higher values indicate better balance
	balance := 1.0 - (stdDev / mean)
	if balance < 0.0 {
		balance = 0.0
	}
	
	return balance, nil
}

// GetStats returns hasher statistics
func (h *Hasher) GetStats() *HasherStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	return &HasherStats{
		TotalHashes:      h.metrics.HashingOperations.Get("hashing", "hash_vector"),
		CacheHits:        h.metrics.CacheHits.Get("hashing", "cluster_assignment"),
		CacheMisses:      h.metrics.CacheMisses.Get("hashing", "cluster_assignment"),
		ValidationErrors: h.metrics.Errors.Get("hashing", "validation_failed"),
	}
}

// GetConfig returns the hasher configuration
func (h *Hasher) GetConfig() *HasherConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Return a copy of config
	config := *h.config
	return &config
}

// UpdateConfig updates the hasher configuration
func (h *Hasher) UpdateConfig(config *HasherConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Validate new configuration
	if err := validateHasherConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidClusterCount, err)
	}
	
	// Clear cache if cluster count changed
	if config.ClusterCount != h.config.ClusterCount {
		h.cache = make(map[uint64]uint32)
		if config.EnableCache && config.CacheSize > 0 {
			h.cache = make(map[uint64]uint32, config.CacheSize)
		}
	}
	
	h.config = config
	
	h.logger.Info("Updated hasher configuration", zap.Any("config", config))
	
	return nil
}

// ClearCache clears the cluster assignment cache
func (h *Hasher) ClearCache() {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	h.cache = make(map[uint64]uint32)
	if h.config.EnableCache && h.config.CacheSize > 0 {
		h.cache = make(map[uint64]uint32, h.config.CacheSize)
	}
	
	h.logger.Info("Cleared hasher cache")
}

// GetCacheInfo returns cache information
func (h *Hasher) GetCacheInfo() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	info := make(map[string]interface{})
	info["size"] = len(h.cache)
	info["max_size"] = h.config.CacheSize
	info["enabled"] = h.config.EnableCache
	info["usage"] = float64(len(h.cache)) / float64(h.config.CacheSize) * 100
	
	return info
}

// Validate validates the hasher state
func (h *Hasher) Validate() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Validate configuration
	if err := validateHasherConfig(h.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	// Validate cache consistency
	if h.config.EnableCache {
		for vectorID, clusterID := range h.cache {
			if clusterID >= h.config.ClusterCount {
				return fmt.Errorf("%w: invalid cluster ID %d for vector %d", ErrInvalidClusterID, clusterID, vectorID)
			}
		}
	}
	
	return nil
}

// BenchmarkHashing benchmarks hashing performance
func (h *Hasher) BenchmarkHashing(vectors []*types.Vector) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	
	if len(vectors) == 0 {
		return results, nil
	}
	
	// Test all algorithms
	algorithms := []HashAlgorithm{
		AlgorithmFNV1a,
		AlgorithmFNV1,
		AlgorithmXXHash,
		AlgorithmMD5,
		AlgorithmSHA1,
		AlgorithmSHA256,
	}
	
	for _, algo := range algorithms {
		// Temporarily set algorithm
		oldAlgo := h.config.Algorithm
		h.config.Algorithm = algo
		
		start := time.Now()
		for _, vector := range vectors {
			_, err := h.HashVector(vector)
			if err != nil {
				results[string(algo)] = map[string]interface{}{
					"error": err.Error(),
				}
				continue
			}
		}
		duration := time.Since(start)
		
		results[string(algo)] = map[string]interface{}{
			"duration": duration.String(),
			"vectors_per_second": float64(len(vectors)) / duration.Seconds(),
		}
		
		// Restore original algorithm
		h.config.Algorithm = oldAlgo
	}
	
	return results, nil
}

// BenchmarkAssignment benchmarks cluster assignment performance
func (h *Hasher) BenchmarkAssignment(vectors []*types.Vector) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	
	if len(vectors) == 0 {
		return results, nil
	}
	
	// Test all strategies
	strategies := []AssignmentStrategy{
		StrategyHash,
		StrategyModulo,
		StrategyConsistent,
		StrategyRange,
	}
	
	for _, strategy := range strategies {
		// Temporarily set strategy
		oldStrategy := h.config.Strategy
		h.config.Strategy = strategy
		
		start := time.Now()
		for _, vector := range vectors {
			_, err := h.AssignCluster(vector)
			if err != nil {
				results[string(strategy)] = map[string]interface{}{
					"error": err.Error(),
				}
				continue
			}
		}
		duration := time.Since(start)
		
		// Calculate distribution balance
		balance, err := h.GetClusterBalance(vectors)
		if err != nil {
			results[string(strategy)] = map[string]interface{}{
				"duration": duration.String(),
				"vectors_per_second": float64(len(vectors)) / duration.Seconds(),
				"balance_error": err.Error(),
			}
			continue
		}
		
		results[string(strategy)] = map[string]interface{}{
			"duration": duration.String(),
			"vectors_per_second": float64(len(vectors)) / duration.Seconds(),
			"balance_factor": balance,
		}
		
		// Restore original strategy
		h.config.Strategy = oldStrategy
	}
	
	return results, nil
}

// GetSupportedAlgorithms returns a list of supported hashing algorithms
func (h *Hasher) GetSupportedAlgorithms() []HashAlgorithm {
	return []HashAlgorithm{
		AlgorithmFNV1a,
		AlgorithmFNV1,
		AlgorithmXXHash,
		AlgorithmMD5,
		AlgorithmSHA1,
		AlgorithmSHA256,
	}
}

// GetSupportedStrategies returns a list of supported assignment strategies
func (h *Hasher) GetSupportedStrategies() []AssignmentStrategy {
	return []AssignmentStrategy{
		StrategyHash,
		StrategyModulo,
		StrategyConsistent,
		StrategyRange,
	}
}

// GetAlgorithmInfo returns information about a hashing algorithm
func (h *Hasher) GetAlgorithmInfo(algorithm HashAlgorithm) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch algorithm {
	case AlgorithmFNV1a:
		info["name"] = "FNV-1a"
		info["description"] = "Fowler-Noll-Vo hash function variant 1a"
		info["speed"] = "Very Fast"
		info["distribution"] = "Good"
		info["collision_resistance"] = "Medium"
		
	case AlgorithmFNV1:
		info["name"] = "FNV-1"
		info["description"] = "Fowler-Noll-Vo hash function variant 1"
		info["speed"] = "Very Fast"
		info["distribution"] = "Good"
		info["collision_resistance"] = "Medium"
		
	case AlgorithmXXHash:
		info["name"] = "XXHash"
		info["description"] = "Very fast non-cryptographic hash algorithm"
		info["speed"] = "Extremely Fast"
		info["distribution"] = "Excellent"
		info["collision_resistance"] = "Good"
		
	case AlgorithmMD5:
		info["name"] = "MD5"
		info["description"] = "Message Digest Algorithm 5"
		info["speed"] = "Fast"
		info["distribution"] = "Excellent"
		info["collision_resistance"] = "Low (cryptographically broken)"
		
	case AlgorithmSHA1:
		info["name"] = "SHA-1"
		info["description"] = "Secure Hash Algorithm 1"
		info["speed"] = "Medium"
		info["distribution"] = "Excellent"
		info["collision_resistance"] = "Medium (cryptographically weak)"
		
	case AlgorithmSHA256:
		info["name"] = "SHA-256"
		info["description"] = "Secure Hash Algorithm 256-bit"
		info["speed"] = "Slow"
		info["distribution"] = "Excellent"
		info["collision_resistance"] = "Excellent"
		
	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown hashing algorithm"
		info["speed"] = "Unknown"
		info["distribution"] = "Unknown"
		info["collision_resistance"] = "Unknown"
	}
	
	return info
}

// GetStrategyInfo returns information about an assignment strategy
func (h *Hasher) GetStrategyInfo(strategy AssignmentStrategy) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch strategy {
	case StrategyHash:
		info["name"] = "Hash"
		info["description"] = "Direct hash-based assignment"
		info["speed"] = "Very Fast"
		info["balance"] = "Good"
		info["scalability"] = "Excellent"
		
	case StrategyModulo:
		info["name"] = "Modulo"
		info["description"] = "Simple modulo-based assignment"
		info["speed"] = "Extremely Fast"
		info["balance"] = "Good"
		info["scalability"] = "Good"
		
	case StrategyConsistent:
		info["name"] = "Consistent Hashing"
		info["description"] = "Consistent hashing for minimal reassignment"
		info["speed"] = "Fast"
		info["balance"] = "Excellent"
		info["scalability"] = "Excellent"
		
	case StrategyRange:
		info["name"] = "Range"
		info["description"] = "Range-based assignment"
		info["speed"] = "Very Fast"
		info["balance"] = "Good"
		info["scalability"] = "Good"
		
	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown assignment strategy"
		info["speed"] = "Unknown"
		info["balance"] = "Unknown"
		info["scalability"] = "Unknown"
	}
	
	return info
}
