package rate

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"vxdb/internal/metrics"
)

// TestValidateRateLimitConfig ensures configuration validation catches invalid settings.
func TestValidateRateLimitConfig(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*RateLimitConfig)
	}{
		{
			name:   "non-positive default limit",
			modify: func(cfg *RateLimitConfig) { cfg.DefaultLimit = 0 },
		},
		{
			name:   "non-positive window",
			modify: func(cfg *RateLimitConfig) { cfg.DefaultWindow = 0 },
		},
		{
			name: "distributed enabled without redis address",
			modify: func(cfg *RateLimitConfig) {
				cfg.EnableDistributed = true
				cfg.RedisAddress = ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultRateLimitConfig()
			tt.modify(cfg)
			if err := validateRateLimitConfig(cfg); err == nil {
				t.Fatalf("expected error for %s", tt.name)
			}
		})
	}
}

// newTestLimiter creates a RateLimiter with default config and disables optional features
// that may interfere with deterministic testing.
func newTestLimiter(t *testing.T) *RateLimiter {
	t.Helper()
	m, _ := metrics.NewMetrics(nil)
	sm := metrics.NewServiceMetrics(m, "test")
	logger := zap.NewNop()

	limiter, err := NewRateLimiter(nil, *logger, sm)
	if err != nil {
		t.Fatalf("NewRateLimiter returned error: %v", err)
	}

	// Disable features to make behaviour deterministic for tests
	limiter.config.EnableBurst = false
	limiter.config.EnableSliding = false
	limiter.config.DefaultLimit = 2
	limiter.config.DefaultWindow = time.Second

	return limiter
}

// TestRateLimiterStartStop verifies lifecycle operations.
func TestRateLimiterStartStop(t *testing.T) {
	rl := newTestLimiter(t)

	if err := rl.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if !rl.IsRunning() {
		t.Fatalf("limiter should be running after Start")
	}
	if err := rl.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if rl.IsRunning() {
		t.Fatalf("limiter should not be running after Stop")
	}
}

// TestRateLimiterAllow ensures requests beyond the limit are denied.
func TestRateLimiterAllow(t *testing.T) {
	rl := newTestLimiter(t)
	if err := rl.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer rl.Stop()

	ctx := context.Background()
	// First two requests should be allowed
	for i := 0; i < 2; i++ {
		allowed, _, err := rl.Allow(ctx, "client", 0, 0)
		if err != nil {
			t.Fatalf("Allow returned error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d unexpectedly rejected", i)
		}
	}
	// Third request should be rejected
	allowed, _, err := rl.Allow(ctx, "client", 0, 0)
	if err != nil {
		t.Fatalf("Allow returned error: %v", err)
	}
	if allowed {
		t.Fatalf("expected request to be rate limited")
	}
}

// TestRateLimiterBurst verifies burst tokens allow temporary exceeding of the limit.
func TestRateLimiterBurst(t *testing.T) {
	rl := newTestLimiter(t)
	rl.config.EnableBurst = true
	rl.config.BurstMultiplier = 2.0

	if err := rl.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer rl.Stop()

	ctx := context.Background()
	// With limit 2 and burst multiplier 2, first four requests succeed.
	for i := 0; i < 4; i++ {
		allowed, _, err := rl.Allow(ctx, "client", 0, 0)
		if err != nil {
			t.Fatalf("Allow returned error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d unexpectedly rejected", i)
		}
	}

	// Fifth request should be rejected as burst tokens are exhausted.
	allowed, _, err := rl.Allow(ctx, "client", 0, 0)
	if err != nil {
		t.Fatalf("Allow returned error: %v", err)
	}
	if allowed {
		t.Fatalf("expected request to be rate limited after burst tokens")
	}
}

// TestRateLimiterSlidingWindow ensures sliding window algorithm drops expired timestamps.
func TestRateLimiterSlidingWindow(t *testing.T) {
	rl := newTestLimiter(t)
	rl.config.EnableSliding = true
	rl.config.DefaultWindow = 50 * time.Millisecond

	if err := rl.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer rl.Stop()

	ctx := context.Background()
	// First two requests within window allowed.
	for i := 0; i < 2; i++ {
		allowed, _, err := rl.Allow(ctx, "client", 0, 0)
		if err != nil {
			t.Fatalf("Allow returned error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d unexpectedly rejected", i)
		}
	}

	// Third request should be rejected.
	allowed, _, err := rl.Allow(ctx, "client", 0, 0)
	if err != nil {
		t.Fatalf("Allow returned error: %v", err)
	}
	if allowed {
		t.Fatalf("expected third request to be rate limited")
	}

	// After window passes, request should be allowed again.
	time.Sleep(60 * time.Millisecond)
	allowed, _, err = rl.Allow(ctx, "client", 0, 0)
	if err != nil {
		t.Fatalf("Allow returned error: %v", err)
	}
	if !allowed {
		t.Fatalf("expected request after window to be allowed")
	}
}
