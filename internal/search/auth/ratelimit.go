package auth

import (
    "context"
    "sync"
    "time"
)

// RateLimiter implements a token bucket rate limiter
type RateLimiter struct {
	rate            float64 // requests per second
	burst           int     // maximum burst size
	cleanupInterval time.Duration

	buckets map[string]*bucket
	mu      sync.RWMutex
}

// bucket represents a token bucket for rate limiting
type bucket struct {
	tokens    float64
	lastCheck time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rate float64, burst int, cleanupInterval time.Duration) *RateLimiter {
	rl := &RateLimiter{
		rate:            rate,
		burst:           burst,
		cleanupInterval: cleanupInterval,
		buckets:         make(map[string]*bucket),
	}

	// Start cleanup goroutine
	go rl.cleanup()

	return rl
}

// Allow checks if a request is allowed for the given identifier
func (rl *RateLimiter) Allow(identifier string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, exists := rl.buckets[identifier]

	if !exists {
		// Create new bucket
		rl.buckets[identifier] = &bucket{
			tokens:    float64(rl.burst),
			lastCheck: now,
		}
		return true
	}

	// Calculate tokens to add based on elapsed time
	elapsed := now.Sub(b.lastCheck)
	tokensToAdd := elapsed.Seconds() * rl.rate
	b.tokens += tokensToAdd

	// Cap at burst size
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}

	// Check if we have enough tokens
	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		b.lastCheck = now
		return true
	}

	b.lastCheck = now
	return false
}

// GetTokens returns the current number of tokens for an identifier
func (rl *RateLimiter) GetTokens(identifier string) float64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	b, exists := rl.buckets[identifier]
	if !exists {
		return float64(rl.burst)
	}

	// Calculate tokens to add based on elapsed time
	now := time.Now()
	elapsed := now.Sub(b.lastCheck)
	tokensToAdd := elapsed.Seconds() * rl.rate
	currentTokens := b.tokens + tokensToAdd

	// Cap at burst size
	if currentTokens > float64(rl.burst) {
		currentTokens = float64(rl.burst)
	}

	return currentTokens
}

// Reset resets the rate limiter for a specific identifier
func (rl *RateLimiter) Reset(identifier string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	delete(rl.buckets, identifier)
}

// ResetAll resets all rate limiters
func (rl *RateLimiter) ResetAll() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.buckets = make(map[string]*bucket)
}

// GetStats returns statistics about the rate limiter
func (rl *RateLimiter) GetStats() RateLimitStats {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	stats := RateLimitStats{
		TotalBuckets: len(rl.buckets),
		Buckets:      make(map[string]BucketStats),
	}

	for identifier, b := range rl.buckets {
		// Calculate current tokens
		now := time.Now()
		elapsed := now.Sub(b.lastCheck)
		tokensToAdd := elapsed.Seconds() * rl.rate
		currentTokens := b.tokens + tokensToAdd

		if currentTokens > float64(rl.burst) {
			currentTokens = float64(rl.burst)
		}

		stats.Buckets[identifier] = BucketStats{
			CurrentTokens: currentTokens,
			MaxTokens:     float64(rl.burst),
			LastAccess:    b.lastCheck,
		}
	}

	return stats
}

// cleanup periodically removes old buckets
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		
		// Remove buckets that haven't been accessed in a long time
		for identifier, b := range rl.buckets {
			if now.Sub(b.lastCheck) > rl.cleanupInterval*2 {
				delete(rl.buckets, identifier)
			}
		}
		
		rl.mu.Unlock()
	}
}

// RateLimitStats represents rate limiter statistics
type RateLimitStats struct {
	TotalBuckets int                    `json:"total_buckets"`
	Buckets      map[string]BucketStats `json:"buckets"`
}

// BucketStats represents statistics for a single bucket
type BucketStats struct {
	CurrentTokens float64   `json:"current_tokens"`
	MaxTokens     float64   `json:"max_tokens"`
	LastAccess    time.Time `json:"last_access"`
}

// SlidingWindowRateLimiter implements a sliding window rate limiter
type SlidingWindowRateLimiter struct {
	rate            int           // requests per window
	window          time.Duration // time window
	cleanupInterval time.Duration

	requests map[string][]time.Time
	mu       sync.RWMutex
}

// NewSlidingWindowRateLimiter creates a new sliding window rate limiter
func NewSlidingWindowRateLimiter(rate int, window time.Duration, cleanupInterval time.Duration) *SlidingWindowRateLimiter {
	rl := &SlidingWindowRateLimiter{
		rate:            rate,
		window:          window,
		cleanupInterval: cleanupInterval,
		requests:        make(map[string][]time.Time),
	}

	// Start cleanup goroutine
	go rl.cleanup()

	return rl
}

// Allow checks if a request is allowed for the given identifier
func (rl *SlidingWindowRateLimiter) Allow(identifier string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-rl.window)

	// Get or create request history for this identifier
	requests, exists := rl.requests[identifier]
	if !exists {
		rl.requests[identifier] = []time.Time{now}
		return true
	}

	// Remove old requests outside the window
	validRequests := make([]time.Time, 0, len(requests))
	for _, reqTime := range requests {
		if reqTime.After(windowStart) {
			validRequests = append(validRequests, reqTime)
		}
	}

	// Check if we're within the rate limit
	if len(validRequests) >= rl.rate {
		rl.requests[identifier] = validRequests
		return false
	}

	// Add current request
	validRequests = append(validRequests, now)
	rl.requests[identifier] = validRequests

	return true
}

// GetRequestCount returns the number of requests in the current window
func (rl *SlidingWindowRateLimiter) GetRequestCount(identifier string) int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	requests, exists := rl.requests[identifier]
	if !exists {
		return 0
	}

	now := time.Now()
	windowStart := now.Add(-rl.window)

	count := 0
	for _, reqTime := range requests {
		if reqTime.After(windowStart) {
			count++
		}
	}

	return count
}

// Reset resets the rate limiter for a specific identifier
func (rl *SlidingWindowRateLimiter) Reset(identifier string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	delete(rl.requests, identifier)
}

// ResetAll resets all rate limiters
func (rl *SlidingWindowRateLimiter) ResetAll() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.requests = make(map[string][]time.Time)
}

// GetStats returns statistics about the rate limiter
func (rl *SlidingWindowRateLimiter) GetStats() SlidingWindowStats {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	stats := SlidingWindowStats{
		TotalIdentifiers: len(rl.requests),
		Identifiers:      make(map[string]SlidingWindowIdentifierStats),
	}

	for identifier, requests := range rl.requests {
		now := time.Now()
		windowStart := now.Add(-rl.window)

		count := 0
		for _, reqTime := range requests {
			if reqTime.After(windowStart) {
				count++
			}
		}

		stats.Identifiers[identifier] = SlidingWindowIdentifierStats{
			RequestCount: count,
			WindowStart:  windowStart,
			WindowEnd:    now,
			RateLimit:    rl.rate,
		}
	}

	return stats
}

// cleanup periodically removes old request histories
func (rl *SlidingWindowRateLimiter) cleanup() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		cleanupThreshold := now.Add(-rl.window * 2)

		// Remove request histories that haven't been accessed in a long time
		for identifier, requests := range rl.requests {
			if len(requests) == 0 {
				delete(rl.requests, identifier)
				continue
			}

			// Check if the last request is older than the cleanup threshold
			lastRequest := requests[len(requests)-1]
			if lastRequest.Before(cleanupThreshold) {
				delete(rl.requests, identifier)
			}
		}
		
		rl.mu.Unlock()
	}
}

// SlidingWindowStats represents sliding window rate limiter statistics
type SlidingWindowStats struct {
	TotalIdentifiers int                                      `json:"total_identifiers"`
	Identifiers      map[string]SlidingWindowIdentifierStats `json:"identifiers"`
}

// SlidingWindowIdentifierStats represents statistics for a single identifier
type SlidingWindowIdentifierStats struct {
	RequestCount int       `json:"request_count"`
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	RateLimit    int       `json:"rate_limit"`
}

// DistributedRateLimiter provides a distributed rate limiter interface
type DistributedRateLimiter interface {
	Allow(ctx context.Context, identifier string) (bool, error)
	GetStats(ctx context.Context) (DistributedRateLimitStats, error)
	Reset(ctx context.Context, identifier string) error
}

// DistributedRateLimitStats represents distributed rate limiter statistics
type DistributedRateLimitStats struct {
	TotalRequests   int64     `json:"total_requests"`
	AllowedRequests int64     `json:"allowed_requests"`
	DeniedRequests  int64     `json:"denied_requests"`
	LastReset       time.Time `json:"last_reset"`
	Nodes           []string  `json:"nodes"`
}

// RedisRateLimiter implements a distributed rate limiter using Redis
type RedisRateLimiter struct {
	// This would integrate with Redis for distributed rate limiting
	// Implementation would depend on Redis client library
}

// NewRedisRateLimiter creates a new Redis-based distributed rate limiter
func NewRedisRateLimiter(redisURL string, rate int, window time.Duration) *RedisRateLimiter {
	// Initialize Redis connection and rate limiter
	return &RedisRateLimiter{}
}

// MockDistributedRateLimiter is a mock implementation for testing
type MockDistributedRateLimiter struct {
	allowFunc func(ctx context.Context, identifier string) (bool, error)
	statsFunc func(ctx context.Context) (DistributedRateLimitStats, error)
	resetFunc func(ctx context.Context, identifier string) error
}

// NewMockDistributedRateLimiter creates a new mock distributed rate limiter
func NewMockDistributedRateLimiter() *MockDistributedRateLimiter {
	return &MockDistributedRateLimiter{
		allowFunc: func(ctx context.Context, identifier string) (bool, error) { return true, nil },
		statsFunc: func(ctx context.Context) (DistributedRateLimitStats, error) {
			return DistributedRateLimitStats{}, nil
		},
		resetFunc: func(ctx context.Context, identifier string) error { return nil },
	}
}

// Allow implements the DistributedRateLimiter interface
func (m *MockDistributedRateLimiter) Allow(ctx context.Context, identifier string) (bool, error) {
	return m.allowFunc(ctx, identifier)
}

// GetStats implements the DistributedRateLimiter interface
func (m *MockDistributedRateLimiter) GetStats(ctx context.Context) (DistributedRateLimitStats, error) {
	return m.statsFunc(ctx)
}

// Reset implements the DistributedRateLimiter interface
func (m *MockDistributedRateLimiter) Reset(ctx context.Context, identifier string) error {
	return m.resetFunc(ctx, identifier)
}

// SetAllowFunc sets the mock allow function
func (m *MockDistributedRateLimiter) SetAllowFunc(f func(ctx context.Context, identifier string) (bool, error)) {
	m.allowFunc = f
}

// SetStatsFunc sets the mock stats function
func (m *MockDistributedRateLimiter) SetStatsFunc(f func(ctx context.Context) (DistributedRateLimitStats, error)) {
	m.statsFunc = f
}

// SetResetFunc sets the mock reset function
func (m *MockDistributedRateLimiter) SetResetFunc(f func(ctx context.Context, identifier string) error) {
	m.resetFunc = f
}
