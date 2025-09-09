package compression

import (
    "bytes"
    "compress/flate"
    "compress/gzip"
    "compress/lzw"
    "compress/zlib"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

    "github.com/pierrec/lz4/v4"
    "vexdb/internal/config"
    "vexdb/internal/logging"
    "vexdb/internal/metrics"
    "go.uber.org/zap"
)

var (
	ErrCompressionFailed   = errors.New("compression failed")
	ErrDecompressionFailed = errors.New("decompression failed")
	ErrInvalidAlgorithm    = errors.New("invalid compression algorithm")
	ErrInvalidData         = errors.New("invalid data")
	ErrBufferTooSmall      = errors.New("buffer too small")
)

// CompressionAlgorithm represents a compression algorithm
type CompressionAlgorithm string

const (
	AlgorithmNone  CompressionAlgorithm = "none"
	AlgorithmLZ4   CompressionAlgorithm = "lz4"
	AlgorithmGzip  CompressionAlgorithm = "gzip"
	AlgorithmZlib  CompressionAlgorithm = "zlib"
	AlgorithmFlate CompressionAlgorithm = "flate"
	AlgorithmLZW   CompressionAlgorithm = "lzw"
)

// CompressionLevel represents the compression level
type CompressionLevel int

const (
	LevelNoCompression CompressionLevel = 0
	LevelFastest       CompressionLevel = 1
	LevelDefault       CompressionLevel = 6
	LevelBest          CompressionLevel = 9
)

// CompressionConfig represents the compression configuration
type CompressionConfig struct {
	Algorithm CompressionAlgorithm `yaml:"algorithm" json:"algorithm"`
	Level     CompressionLevel     `yaml:"level" json:"level"`
	Enabled   bool                 `yaml:"enabled" json:"enabled"`
	Threshold int                  `yaml:"threshold" json:"threshold"` // Minimum size to compress
	MaxSize   int                  `yaml:"max_size" json:"max_size"`   // Maximum size to compress
}

// DefaultCompressionConfig returns the default compression configuration
func DefaultCompressionConfig() *CompressionConfig {
	return &CompressionConfig{
		Algorithm: AlgorithmLZ4,
		Level:     LevelDefault,
		Enabled:   true,
		Threshold: 1024,  // 1KB
		MaxSize:   10485760, // 10MB
	}
}

// CompressionStats represents compression statistics
type CompressionStats struct {
	TotalCompressions   int64   `json:"total_compressions"`
	TotalDecompressions int64   `json:"total_decompressions"`
	CompressionRatio    float64 `json:"compression_ratio"`
	AverageCompression  float64 `json:"average_compression"`
	FailureCount        int64   `json:"failure_count"`
}

// Compressor represents a compression engine
type Compressor struct {
    config      *CompressionConfig
    stats       *CompressionStats
    mu          sync.RWMutex
    logger      logging.Logger
    metrics     *metrics.StorageMetrics
    pool        *sync.Pool
}

// NewCompressor creates a new compressor
func NewCompressor(cfg config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*Compressor, error) {
    compressionConfig := DefaultCompressionConfig()
    
    if cfg != nil {
        if compressionCfg, ok := cfg.Get("compression"); ok {
            if cfgMap, ok := compressionCfg.(map[string]interface{}); ok {
				if algorithm, ok := cfgMap["algorithm"].(string); ok {
					compressionConfig.Algorithm = CompressionAlgorithm(algorithm)
				}
				if level, ok := cfgMap["level"].(int); ok {
					compressionConfig.Level = CompressionLevel(level)
				}
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					compressionConfig.Enabled = enabled
				}
				if threshold, ok := cfgMap["threshold"].(int); ok {
					compressionConfig.Threshold = threshold
				}
				if maxSize, ok := cfgMap["max_size"].(int); ok {
					compressionConfig.MaxSize = maxSize
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateCompressionConfig(compressionConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidAlgorithm, err)
	}
	
	compressor := &Compressor{
		config:  compressionConfig,
		stats:   &CompressionStats{},
		logger:  logger,
		metrics: metrics,
		pool: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
	
    compressor.logger.Info("Created compressor",
        zap.String("algorithm", string(compressionConfig.Algorithm)),
        zap.Int("level", int(compressionConfig.Level)),
        zap.Bool("enabled", compressionConfig.Enabled),
        zap.Int("threshold", compressionConfig.Threshold),
        zap.Int("max_size", compressionConfig.MaxSize))
	
	return compressor, nil
}

// validateCompressionConfig validates the compression configuration
func validateCompressionConfig(cfg *CompressionConfig) error {
	if cfg.Algorithm != AlgorithmNone &&
		cfg.Algorithm != AlgorithmLZ4 &&
		cfg.Algorithm != AlgorithmGzip &&
		cfg.Algorithm != AlgorithmZlib &&
		cfg.Algorithm != AlgorithmFlate &&
		cfg.Algorithm != AlgorithmLZW {
		return fmt.Errorf("unsupported algorithm: %s", cfg.Algorithm)
	}
	
	if cfg.Level < LevelNoCompression || cfg.Level > LevelBest {
		return fmt.Errorf("invalid compression level: %d", cfg.Level)
	}
	
	if cfg.Threshold < 0 {
		return fmt.Errorf("invalid threshold: %d", cfg.Threshold)
	}
	
	if cfg.MaxSize < 0 {
		return fmt.Errorf("invalid max_size: %d", cfg.MaxSize)
	}
	
	return nil
}

// ShouldCompress returns true if data should be compressed
func (c *Compressor) ShouldCompress(data []byte) bool {
	if !c.config.Enabled {
		return false
	}
	
	if c.config.Algorithm == AlgorithmNone {
		return false
	}
	
	size := len(data)
	if size < c.config.Threshold {
		return false
	}
	
	if c.config.MaxSize > 0 && size > c.config.MaxSize {
		return false
	}
	
	return true
}

// Compress compresses data using the configured algorithm
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	if !c.ShouldCompress(data) {
		return data, nil
	}
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	compressed, err := c.compress(data)
	if err != nil {
		c.stats.FailureCount++
		c.metrics.Errors.Inc("compression", "compress_failed")
		return nil, fmt.Errorf("%w: %v", ErrCompressionFailed, err)
	}
	
	// Update stats
	c.stats.TotalCompressions++
	if len(data) > 0 {
		ratio := float64(len(compressed)) / float64(len(data))
		c.stats.CompressionRatio = (c.stats.CompressionRatio*float64(c.stats.TotalCompressions-1) + ratio) / float64(c.stats.TotalCompressions)
	}
	
	// Update metrics
    c.metrics.CompressionOperations.Inc("compression", "compress")
    c.metrics.CompressionRatio.Set(c.stats.CompressionRatio)
	
	return compressed, nil
}

// compress compresses data using the configured algorithm (internal, assumes lock is held)
func (c *Compressor) compress(data []byte) ([]byte, error) {
	switch c.config.Algorithm {
	case AlgorithmLZ4:
		return c.compressLZ4(data)
	case AlgorithmGzip:
		return c.compressGzip(data)
	case AlgorithmZlib:
		return c.compressZlib(data)
	case AlgorithmFlate:
		return c.compressFlate(data)
	case AlgorithmLZW:
		return c.compressLZW(data)
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidAlgorithm, c.config.Algorithm)
	}
}

// compressLZ4 compresses data using LZ4
func (c *Compressor) compressLZ4(data []byte) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	writer := lz4.NewWriter(buf)
	if c.config.Level == LevelFastest {
		writer.Header = lz4.Header{
			CompressionLevel: lz4.Fast,
		}
	} else if c.config.Level == LevelBest {
		writer.Header = lz4.Header{
			CompressionLevel: lz4.BestCompression,
		}
	}
	
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	
	if err := writer.Close(); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// compressGzip compresses data using Gzip
func (c *Compressor) compressGzip(data []byte) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	writer, err := gzip.NewWriterLevel(buf, int(c.config.Level))
	if err != nil {
		return nil, err
	}
	
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}
	
	if err := writer.Close(); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// compressZlib compresses data using Zlib
func (c *Compressor) compressZlib(data []byte) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	writer, err := zlib.NewWriterLevel(buf, int(c.config.Level))
	if err != nil {
		return nil, err
	}
	
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}
	
	if err := writer.Close(); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// compressFlate compresses data using Flate
func (c *Compressor) compressFlate(data []byte) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	writer, err := flate.NewWriter(buf, int(c.config.Level))
	if err != nil {
		return nil, err
	}
	
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}
	
	if err := writer.Close(); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// compressLZW compresses data using LZW
func (c *Compressor) compressLZW(data []byte) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	writer := lzw.NewWriter(buf, lzw.LSB, 8)
	
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}
	
	if err := writer.Close(); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// Decompress decompresses data using the configured algorithm
func (c *Compressor) Decompress(data []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	decompressed, err := c.decompress(data)
	if err != nil {
		c.stats.FailureCount++
		c.metrics.Errors.Inc("compression", "decompress_failed")
		return nil, fmt.Errorf("%w: %v", ErrDecompressionFailed, err)
	}
	
	// Update stats
	c.stats.TotalDecompressions++
	
	// Update metrics
    c.metrics.CompressionOperations.Inc("compression", "decompress")
	
	return decompressed, nil
}

// decompress decompresses data using the configured algorithm (internal, assumes lock is held)
func (c *Compressor) decompress(data []byte) ([]byte, error) {
	switch c.config.Algorithm {
	case AlgorithmLZ4:
		return c.decompressLZ4(data)
	case AlgorithmGzip:
		return c.decompressGzip(data)
	case AlgorithmZlib:
		return c.decompressZlib(data)
	case AlgorithmFlate:
		return c.decompressFlate(data)
	case AlgorithmLZW:
		return c.decompressLZW(data)
	case AlgorithmNone:
		return data, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidAlgorithm, c.config.Algorithm)
	}
}

// decompressLZ4 decompresses data using LZ4
func (c *Compressor) decompressLZ4(data []byte) ([]byte, error) {
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	reader := lz4.NewReader(bytes.NewReader(data))
	
	if _, err := io.Copy(buf, reader); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// decompressGzip decompresses data using Gzip
func (c *Compressor) decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	if _, err := io.Copy(buf, reader); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// decompressZlib decompresses data using Zlib
func (c *Compressor) decompressZlib(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	if _, err := io.Copy(buf, reader); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// decompressFlate decompresses data using Flate
func (c *Compressor) decompressFlate(data []byte) ([]byte, error) {
	reader := flate.NewReader(bytes.NewReader(data))
	defer reader.Close()
	
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	if _, err := io.Copy(buf, reader); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// decompressLZW decompresses data using LZW
func (c *Compressor) decompressLZW(data []byte) ([]byte, error) {
	reader := lzw.NewReader(bytes.NewReader(data), lzw.LSB, 8)
	defer reader.Close()
	
	buf := c.pool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.pool.Put(buf)
	
	if _, err := io.Copy(buf, reader); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}

// GetStats returns compression statistics
func (c *Compressor) GetStats() *CompressionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Return a copy of stats
	stats := *c.stats
	return &stats
}

// GetConfig returns the compression configuration
func (c *Compressor) GetConfig() *CompressionConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Return a copy of config
	config := *c.config
	return &config
}

// UpdateConfig updates the compression configuration
func (c *Compressor) UpdateConfig(config *CompressionConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Validate new configuration
	if err := validateCompressionConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAlgorithm, err)
	}
	
	c.config = config
	
	c.logger.Info("Updated compression configuration", "config", config)
	
	return nil
}

// EstimateCompressionRatio estimates the compression ratio for given data
func (c *Compressor) EstimateCompressionRatio(data []byte) (float64, error) {
	if !c.ShouldCompress(data) {
		return 1.0, nil
	}
	
	compressed, err := c.compress(data)
	if err != nil {
		return 0.0, err
	}
	
	if len(data) == 0 {
		return 1.0, nil
	}
	
	return float64(len(compressed)) / float64(len(data)), nil
}

// BenchmarkCompression benchmarks compression performance
func (c *Compressor) BenchmarkCompression(data []byte) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	
	originalSize := len(data)
	results["original_size"] = originalSize
	
	// Test all algorithms
	algorithms := []CompressionAlgorithm{
		AlgorithmNone,
		AlgorithmLZ4,
		AlgorithmGzip,
		AlgorithmZlib,
		AlgorithmFlate,
		AlgorithmLZW,
	}
	
	for _, algo := range algorithms {
		// Temporarily set algorithm
		oldAlgo := c.config.Algorithm
		c.config.Algorithm = algo
		
		start := time.Now()
		compressed, err := c.compress(data)
		duration := time.Since(start)
		
		if err != nil {
			results[string(algo)] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}
		
		compressedSize := len(compressed)
		ratio := float64(compressedSize) / float64(originalSize)
		
		// Test decompression
		start = time.Now()
		decompressed, err := c.decompress(compressed)
		decompressDuration := time.Since(start)
		
		if err != nil {
			results[string(algo)] = map[string]interface{}{
				"compressed_size": compressedSize,
				"compression_ratio": ratio,
				"compression_time": duration.String(),
				"decompress_error": err.Error(),
			}
			continue
		}
		
		// Verify decompression
		if !bytes.Equal(data, decompressed) {
			results[string(algo)] = map[string]interface{}{
				"compressed_size": compressedSize,
				"compression_ratio": ratio,
				"compression_time": duration.String(),
				"decompress_time": decompressDuration.String(),
				"verification": "failed",
			}
			continue
		}
		
		results[string(algo)] = map[string]interface{}{
			"compressed_size": compressedSize,
			"compression_ratio": ratio,
			"compression_time": duration.String(),
			"decompress_time": decompressDuration.String(),
			"verification": "passed",
		}
		
		// Restore original algorithm
		c.config.Algorithm = oldAlgo
	}
	
	return results, nil
}

// Validate validates the compressor state
func (c *Compressor) Validate() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Validate configuration
	if err := validateCompressionConfig(c.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	return nil
}

// Reset resets the compressor statistics
func (c *Compressor) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.stats = &CompressionStats{}
	
	c.logger.Info("Reset compressor statistics")
}

// IsEnabled returns true if compression is enabled
func (c *Compressor) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return c.config.Enabled
}

// GetAlgorithm returns the current compression algorithm
func (c *Compressor) GetAlgorithm() CompressionAlgorithm {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return c.config.Algorithm
}

// GetLevel returns the current compression level
func (c *Compressor) GetLevel() CompressionLevel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return c.config.Level
}

// SetAlgorithm sets the compression algorithm
func (c *Compressor) SetAlgorithm(algorithm CompressionAlgorithm) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Validate algorithm
	if algorithm != AlgorithmNone &&
		algorithm != AlgorithmLZ4 &&
		algorithm != AlgorithmGzip &&
		algorithm != AlgorithmZlib &&
		algorithm != AlgorithmFlate &&
		algorithm != AlgorithmLZW {
		return fmt.Errorf("%w: %s", ErrInvalidAlgorithm, algorithm)
	}
	
	c.config.Algorithm = algorithm
	
	c.logger.Info("Set compression algorithm", "algorithm", algorithm)
	
	return nil
}

// SetLevel sets the compression level
func (c *Compressor) SetLevel(level CompressionLevel) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Validate level
	if level < LevelNoCompression || level > LevelBest {
		return fmt.Errorf("invalid compression level: %d", level)
	}
	
	c.config.Level = level
	
	c.logger.Info("Set compression level", "level", level)
	
	return nil
}

// SetEnabled enables or disables compression
func (c *Compressor) SetEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.config.Enabled = enabled
	
	c.logger.Info("Set compression enabled", "enabled", enabled)
}

// GetSupportedAlgorithms returns a list of supported compression algorithms
func (c *Compressor) GetSupportedAlgorithms() []CompressionAlgorithm {
	return []CompressionAlgorithm{
		AlgorithmNone,
		AlgorithmLZ4,
		AlgorithmGzip,
		AlgorithmZlib,
		AlgorithmFlate,
		AlgorithmLZW,
	}
}

// GetAlgorithmInfo returns information about a compression algorithm
func (c *Compressor) GetAlgorithmInfo(algorithm CompressionAlgorithm) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch algorithm {
	case AlgorithmNone:
		info["name"] = "None"
		info["description"] = "No compression"
		info["speed"] = "Fastest"
		info["ratio"] = "1:1"
		info["cpu_usage"] = "Lowest"
		
	case AlgorithmLZ4:
		info["name"] = "LZ4"
		info["description"] = "LZ4 compression algorithm"
		info["speed"] = "Very Fast"
		info["ratio"] = "Good"
		info["cpu_usage"] = "Low"
		
	case AlgorithmGzip:
		info["name"] = "Gzip"
		info["description"] = "Gzip compression algorithm"
		info["speed"] = "Medium"
		info["ratio"] = "Very Good"
		info["cpu_usage"] = "Medium"
		
	case AlgorithmZlib:
		info["name"] = "Zlib"
		info["description"] = "Zlib compression algorithm"
		info["speed"] = "Medium"
		info["ratio"] = "Very Good"
		info["cpu_usage"] = "Medium"
		
	case AlgorithmFlate:
		info["name"] = "Flate"
		info["description"] = "Flate compression algorithm"
		info["speed"] = "Medium"
		info["ratio"] = "Very Good"
		info["cpu_usage"] = "Medium"
		
	case AlgorithmLZW:
		info["name"] = "LZW"
		info["description"] = "Lempel-Ziv-Welch compression algorithm"
		info["speed"] = "Slow"
		info["ratio"] = "Good"
		info["cpu_usage"] = "High"
		
	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown compression algorithm"
		info["speed"] = "Unknown"
		info["ratio"] = "Unknown"
		info["cpu_usage"] = "Unknown"
	}
	
	return info
}
