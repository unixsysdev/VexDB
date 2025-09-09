package rate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"

	"go.uber.org/zap"
)

var (
	ErrRateLimitExceeded     = errors.New("rate limit exceeded")
	ErrInvalidRateLimit      = errors.New("invalid rate limit configuration")
	ErrLimiterNotRunning     = errors.New("rate limiter not running")
	ErrLimiterAlreadyRunning = errors.New("rate limiter already running")
	ErrKeyNotFound           = errors.New("rate limit key not found")
	ErrWindowExpired         = errors.New("rate limit window expired")
)

// RateLimitConfig represents the rate limit configuration
type RateLimitConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	DefaultLimit      int           `yaml:"default_limit" json:"default_limit"`
	DefaultWindow     time.Duration `yaml:"default_window" json:"default_window"`
	EnableMetrics     bool          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableLogging     bool          `yaml:"enable_logging" json:"enable_logging"`
	CleanupInterval   time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
	MaxKeys           int           `yaml:"max_keys" json:"max_keys"`
	EnableBurst       bool          `yaml:"enable_burst" json:"enable_burst"`
	BurstMultiplier   float64       `yaml:"burst_multiplier" json:"burst_multiplier"`
	EnableSliding     bool          `yaml:"enable_sliding" json:"enable_sliding"`
	EnableDistributed bool          `yaml:"enable_distributed" json:"enable_distributed"`
	RedisAddress      string        `yaml:"redis_address" json:"redis_address"`
	RedisPassword     string        `yaml:"redis_password" json:"redis_password"`
	RedisDB           int           `yaml:"redis_db" json:"redis_db"`
}

// DefaultRateLimitConfig returns the default rate limit configuration
func DefaultRateLimitConfig() *RateLimitConfig {
	return &RateLimitConfig{
		Enabled:           true,
		DefaultLimit:      100,
		DefaultWindow:     time.Minute,
		EnableMetrics:     true,
		EnableLogging:     true,
		CleanupInterval:   5 * time.Minute,
		MaxKeys:           10000,
		EnableBurst:       true,
		BurstMultiplier:   2.0,
		EnableSliding:     true,
		EnableDistributed: false,
		RedisAddress:      "",
		RedisPassword:     "",
		RedisDB:           0,
	}
}

// RateLimitEntry represents a rate limit entry
type RateLimitEntry struct {
	Key              string        `json:"key"`
	Limit            int           `json:"limit"`
	Window           time.Duration `json:"window"`
	CurrentCount     int           `json:"current_count"`
	ResetTime        time.Time     `json:"reset_time"`
	TotalRequests    int64         `json:"total_requests"`
	RejectedRequests int64         `json:"rejected_requests"`
	AllowedRequests  int64         `json:"allowed_requests"`
	LastUsed         time.Time     `json:"last_used"`
	CreatedAt        time.Time     `json:"created_at"`
	UpdatedAt        time.Time     `json:"updated_at"`
	BurstTokens      int           `json:"burst_tokens,omitempty"`
	SlidingWindow    []time.Time   `json:"sliding_window,omitempty"`
}

// RateLimiter represents a rate limiter
type RateLimiter struct {
	config  *RateLimitConfig
	logger  logging.Logger
	metrics *metrics.ServiceMetrics

	// Rate limit storage
	limits map[string]*RateLimitEntry
	mu     sync.RWMutex

	// Cleanup
	cleanupChan chan struct{}
	cleanupDone chan struct{}

	// Lifecycle
	started   bool
	stopped   bool
	startTime time.Time

	// Distributed rate limiting (if enabled)
	redisClient interface{} // Placeholder for Redis client
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(cfg config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*RateLimiter, error) {
	limiterConfig := DefaultRateLimitConfig()

	if cfg != nil {
		if limiterCfg, ok := cfg.Get("rate_limit"); ok {
			if cfgMap, ok := limiterCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					limiterConfig.Enabled = enabled
				}
				if defaultLimit, ok := cfgMap["default_limit"].(int); ok {
					limiterConfig.DefaultLimit = defaultLimit
				}
				if defaultWindow, ok := cfgMap["default_window"].(string); ok {
					if duration, err := time.ParseDuration(defaultWindow); err == nil {
						limiterConfig.DefaultWindow = duration
					}
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					limiterConfig.EnableMetrics = enableMetrics
				}
				if enableLogging, ok := cfgMap["enable_logging"].(bool); ok {
					limiterConfig.EnableLogging = enableLogging
				}
				if cleanupInterval, ok := cfgMap["cleanup_interval"].(string); ok {
					if duration, err := time.ParseDuration(cleanupInterval); err == nil {
						limiterConfig.CleanupInterval = duration
					}
				}
				if maxKeys, ok := cfgMap["max_keys"].(int); ok {
					limiterConfig.MaxKeys = maxKeys
				}
				if enableBurst, ok := cfgMap["enable_burst"].(bool); ok {
					limiterConfig.EnableBurst = enableBurst
				}
				if burstMultiplier, ok := cfgMap["burst_multiplier"].(float64); ok {
					limiterConfig.BurstMultiplier = burstMultiplier
				}
				if enableSliding, ok := cfgMap["enable_sliding"].(bool); ok {
					limiterConfig.EnableSliding = enableSliding
				}
				if enableDistributed, ok := cfgMap["enable_distributed"].(bool); ok {
					limiterConfig.EnableDistributed = enableDistributed
				}
				if redisAddress, ok := cfgMap["redis_address"].(string); ok {
					limiterConfig.RedisAddress = redisAddress
				}
				if redisPassword, ok := cfgMap["redis_password"].(string); ok {
					limiterConfig.RedisPassword = redisPassword
				}
				if redisDB, ok := cfgMap["redis_db"].(int); ok {
					limiterConfig.RedisDB = redisDB
				}
			}
		}
	}

	// Validate configuration
	if err := validateRateLimitConfig(limiterConfig); err != nil {
		return nil, fmt.Errorf("invalid rate limit configuration: %w", err)
	}

	limiter := &RateLimiter{
		config:      limiterConfig,
		logger:      logger,
		metrics:     metrics,
		limits:      make(map[string]*RateLimitEntry),
		cleanupChan: make(chan struct{}),
		cleanupDone: make(chan struct{}),
		startTime:   time.Now(),
	}

	// Initialize Redis client if distributed rate limiting is enabled
	if limiterConfig.EnableDistributed {
		if err := limiter.initRedisClient(); err != nil {
			return nil, fmt.Errorf("failed to initialize Redis client: %w", err)
		}
	}

	limiter.logger.Info("Created rate limiter")

	return limiter, nil
}

// validateRateLimitConfig validates the rate limit configuration
func validateRateLimitConfig(config *RateLimitConfig) error {
	if config == nil {
		return errors.New("rate limit configuration cannot be nil")
	}

	if config.DefaultLimit <= 0 {
		return errors.New("default limit must be positive")
	}

	if config.DefaultWindow <= 0 {
		return errors.New("default window must be positive")
	}

	if config.CleanupInterval <= 0 {
		return errors.New("cleanup interval must be positive")
	}

	if config.MaxKeys <= 0 {
		return errors.New("max keys must be positive")
	}

	if config.BurstMultiplier < 1.0 {
		return errors.New("burst multiplier must be >= 1.0")
	}

	if config.EnableDistributed {
		if config.RedisAddress == "" {
			return errors.New("redis address cannot be empty when distributed rate limiting is enabled")
		}
	}

	return nil
}

// initRedisClient initializes the Redis client for distributed rate limiting
func (l *RateLimiter) initRedisClient() error {
	// This is a placeholder for Redis client initialization
	// In a real implementation, you would use a Redis client library
	l.logger.Info("Initializing Redis client for distributed rate limiting",
		zap.String("address", l.config.RedisAddress),
		zap.Int("db", l.config.RedisDB))

	// Placeholder for Redis client initialization
	// l.redisClient = redis.NewClient(&redis.Options{
	//     Addr:     l.config.RedisAddress,
	//     Password: l.config.RedisPassword,
	//     DB:       l.config.RedisDB,
	// })

	return nil
}

// Start starts the rate limiter
func (l *RateLimiter) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.started {
		return ErrLimiterAlreadyRunning
	}

	if !l.config.Enabled {
		l.logger.Info("Rate limiter is disabled")
		return nil
	}

	// Start cleanup routine
	go l.cleanupExpiredEntries()

	l.started = true
	l.stopped = false

	l.logger.Info("Started rate limiter")

	return nil
}

// Stop stops the rate limiter
func (l *RateLimiter) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.stopped {
		return nil
	}

	if !l.started {
		return ErrLimiterNotRunning
	}

	// Stop cleanup routine
	close(l.cleanupChan)

	// Wait for cleanup to finish
	<-l.cleanupDone

	l.stopped = true
	l.started = false

	l.logger.Info("Stopped rate limiter")

	return nil
}

// IsRunning checks if the rate limiter is running
func (l *RateLimiter) IsRunning() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.started && !l.stopped
}

// Allow checks if a request is allowed based on rate limits
func (l *RateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration, error) {
	if !l.config.Enabled {
		return true, 0, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.started {
		return false, 0, ErrLimiterNotRunning
	}

	// Use default values if not provided
	if limit <= 0 {
		limit = l.config.DefaultLimit
	}
	if window <= 0 {
		window = l.config.DefaultWindow
	}

	// Get or create rate limit entry
	entry, exists := l.limits[key]
	if !exists {
		// Check if we've reached the maximum number of keys
		if len(l.limits) >= l.config.MaxKeys {
			// Clean up expired entries first
			l.cleanupExpiredEntriesLocked()
			if len(l.limits) >= l.config.MaxKeys {
				return false, 0, ErrRateLimitExceeded
			}
		}

		entry = l.createRateLimitEntry(key, limit, window)
		l.limits[key] = entry
	}

	// Check if window has expired
	if time.Now().After(entry.ResetTime) {
		l.resetRateLimitEntry(entry, limit, window)
	}

	// Check if request is allowed
	allowed, resetTime := l.checkRateLimit(entry)

	// Update statistics
	entry.TotalRequests++
	entry.LastUsed = time.Now()
	entry.UpdatedAt = time.Now()

	if allowed {
		entry.AllowedRequests++
		entry.CurrentCount++

		// Update sliding window if enabled
		if l.config.EnableSliding {
			entry.SlidingWindow = append(entry.SlidingWindow, time.Now())
		}
	} else {
		entry.RejectedRequests++
	}

	// Log rate limit events
	if l.config.EnableLogging {
		if !allowed {
			l.logger.Warn("Rate limit exceeded",
				zap.String("key", key),
				zap.Int("limit", limit),
				zap.Duration("window", window),
				zap.Int("current_count", entry.CurrentCount),
				zap.Duration("reset_time", resetTime))
		}
	}

	// Update metrics if enabled
	if l.config.EnableMetrics && l.metrics != nil {
		l.updateRateLimitMetrics(key, allowed)
	}

	return allowed, resetTime, nil
}

// GetRateLimitStats returns rate limit statistics for a key
func (l *RateLimiter) GetRateLimitStats(key string) (adapter.RateLimitStats, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entry, exists := l.limits[key]
	if !exists {
		return adapter.RateLimitStats{}, ErrKeyNotFound
	}

	return adapter.RateLimitStats{
		Key:              entry.Key,
		Limit:            entry.Limit,
		Window:           entry.Window,
		CurrentCount:     entry.CurrentCount,
		ResetTime:        entry.ResetTime,
		TotalRequests:    entry.TotalRequests,
		RejectedRequests: entry.RejectedRequests,
		AllowedRequests:  entry.AllowedRequests,
	}, nil
}

// Reset resets the rate limit for a key
func (l *RateLimiter) Reset(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, exists := l.limits[key]
	if !exists {
		return ErrKeyNotFound
	}

	l.resetRateLimitEntry(entry, entry.Limit, entry.Window)

	if l.config.EnableLogging {
		l.logger.Info("Rate limit reset", zap.String("key", key))
	}

	return nil
}

// UpdateLimit updates the rate limit for a key
func (l *RateLimiter) UpdateLimit(key string, limit int, window time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if limit <= 0 {
		return ErrInvalidRateLimit
	}
	if window <= 0 {
		return ErrInvalidRateLimit
	}

	entry, exists := l.limits[key]
	if !exists {
		return ErrKeyNotFound
	}

	entry.Limit = limit
	entry.Window = window
	entry.ResetTime = time.Now().Add(window)

	if l.config.EnableLogging {
		l.logger.Info("Rate limit updated",
			zap.String("key", key),
			zap.Int("new_limit", limit),
			zap.Duration("new_window", window))
	}

	return nil
}

// GetAllStats returns all rate limit statistics
func (l *RateLimiter) GetAllStats() map[string]adapter.RateLimitStats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stats := make(map[string]adapter.RateLimitStats)
	for key, entry := range l.limits {
		stats[key] = adapter.RateLimitStats{
			Key:              entry.Key,
			Limit:            entry.Limit,
			Window:           entry.Window,
			CurrentCount:     entry.CurrentCount,
			ResetTime:        entry.ResetTime,
			TotalRequests:    entry.TotalRequests,
			RejectedRequests: entry.RejectedRequests,
			AllowedRequests:  entry.AllowedRequests,
		}
	}

	return stats
}

// GetConfig returns the rate limit configuration
func (l *RateLimiter) GetConfig() *RateLimitConfig {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Return a copy of config
	config := *l.config
	return &config
}

// UpdateConfig updates the rate limit configuration
func (l *RateLimiter) UpdateConfig(config *RateLimitConfig) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Validate new configuration
	if err := validateRateLimitConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	l.config = config

	l.logger.Info("Updated rate limit configuration", zap.Any("config", config))

	return nil
}

// Validate validates the rate limiter state
func (l *RateLimiter) Validate() error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Validate configuration
	if err := validateRateLimitConfig(l.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	return nil
}

// createRateLimitEntry creates a new rate limit entry
func (l *RateLimiter) createRateLimitEntry(key string, limit int, window time.Duration) *RateLimitEntry {
	now := time.Now()
	entry := &RateLimitEntry{
		Key:              key,
		Limit:            limit,
		Window:           window,
		CurrentCount:     0,
		ResetTime:        now.Add(window),
		TotalRequests:    0,
		RejectedRequests: 0,
		AllowedRequests:  0,
		LastUsed:         now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	// Initialize burst tokens if enabled
	if l.config.EnableBurst {
		entry.BurstTokens = int(float64(limit) * (l.config.BurstMultiplier - 1.0))
	}

	// Initialize sliding window if enabled
	if l.config.EnableSliding {
		entry.SlidingWindow = make([]time.Time, 0)
	}

	return entry
}

// resetRateLimitEntry resets a rate limit entry
func (l *RateLimiter) resetRateLimitEntry(entry *RateLimitEntry, limit int, window time.Duration) {
	now := time.Now()
	entry.CurrentCount = 0
	entry.ResetTime = now.Add(window)
	entry.UpdatedAt = now

	// Reset sliding window if enabled
	if l.config.EnableSliding {
		entry.SlidingWindow = make([]time.Time, 0)
	}

	// Reset burst tokens if enabled
	if l.config.EnableBurst {
		entry.BurstTokens = int(float64(limit) * (l.config.BurstMultiplier - 1.0))
	}
}

// checkRateLimit checks if a request is allowed
func (l *RateLimiter) checkRateLimit(entry *RateLimitEntry) (bool, time.Duration) {
	// Check sliding window if enabled
	if l.config.EnableSliding {
		return l.checkSlidingWindowRateLimit(entry)
	}

	// Check fixed window rate limit
	if entry.CurrentCount < entry.Limit {
		return true, time.Until(entry.ResetTime)
	}

	// Check burst tokens if enabled
	if l.config.EnableBurst && entry.BurstTokens > 0 {
		entry.BurstTokens--
		return true, time.Until(entry.ResetTime)
	}

	return false, time.Until(entry.ResetTime)
}

// checkSlidingWindowRateLimit checks rate limit using sliding window algorithm
func (l *RateLimiter) checkSlidingWindowRateLimit(entry *RateLimitEntry) (bool, time.Duration) {
	now := time.Now()
	windowStart := now.Add(-entry.Window)

	// Remove old timestamps from sliding window
	validTimestamps := make([]time.Time, 0)
	for _, timestamp := range entry.SlidingWindow {
		if timestamp.After(windowStart) {
			validTimestamps = append(validTimestamps, timestamp)
		}
	}
	entry.SlidingWindow = validTimestamps

	// Check if request is allowed
	if len(entry.SlidingWindow) < entry.Limit {
		return true, 0
	}

	return false, entry.SlidingWindow[0].Add(entry.Window).Sub(now)
}

// cleanupExpiredEntries cleans up expired rate limit entries
func (l *RateLimiter) cleanupExpiredEntries() {
	ticker := time.NewTicker(l.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.mu.Lock()
			l.cleanupExpiredEntriesLocked()
			l.mu.Unlock()

		case <-l.cleanupChan:
			close(l.cleanupDone)
			return
		}
	}
}

// cleanupExpiredEntriesLocked cleans up expired rate limit entries (locked version)
func (l *RateLimiter) cleanupExpiredEntriesLocked() {
	now := time.Now()
	expiredKeys := make([]string, 0)

	for key, entry := range l.limits {
		// Remove entries that haven't been used for 3 times the cleanup interval
		if now.Sub(entry.LastUsed) > l.config.CleanupInterval*3 {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		delete(l.limits, key)
	}

	if len(expiredKeys) > 0 && l.config.EnableLogging {
		l.logger.Info("Cleaned up expired rate limit entries",
			zap.Int("count", len(expiredKeys)),
			zap.Int("remaining_entries", len(l.limits)))
	}
}

// updateRateLimitMetrics updates rate limit metrics
func (l *RateLimiter) updateRateLimitMetrics(key string, allowed bool) {
	if l.metrics == nil {
		return
	}

	if allowed {
		l.metrics.ServiceErrors.Inc("rate_limit", "allowed")
	} else {
		l.metrics.ServiceErrors.Inc("rate_limit", "rejected")
	}
}
