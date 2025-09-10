package health

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

// TestValidateHealthConfig checks validation logic for the health config.
func TestValidateHealthConfig(t *testing.T) {
	cfg := DefaultHealthConfig()
	if err := validateHealthConfig(cfg); err != nil {
		t.Fatalf("valid config returned error: %v", err)
	}

	cfg.Port = -1
	if err := validateHealthConfig(cfg); err == nil {
		t.Fatalf("expected error for invalid port")
	}

	cfg.Port = 8080
	cfg.Path = ""
	if err := validateHealthConfig(cfg); err == nil {
		t.Fatalf("expected error for empty path")
	}
}

// TestHandlers verifies HTTP responses for health endpoints.
func TestHandlers(t *testing.T) {
	he, err := NewHealthEndpoints(nil, *zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("failed to create endpoints: %v", err)
	}

	// Simulate healthy state
	he.results["storage"] = HealthCheckResult{Name: "storage", Status: StatusHealthy}
	he.results["search"] = HealthCheckResult{Name: "search", Status: StatusHealthy}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	he.handleHealth(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for healthy, got %d", w.Code)
	}

	// Simulate unhealthy state for critical check
	he.results["storage"] = HealthCheckResult{Name: "storage", Status: StatusUnhealthy}
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	w = httptest.NewRecorder()
	he.handleHealth(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for unhealthy, got %d", w.Code)
	}

	// Liveness when running
	he.started = true
	req = httptest.NewRequest(http.MethodGet, "/health/live", nil)
	w = httptest.NewRecorder()
	he.handleLiveness(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for live, got %d", w.Code)
	}

	// Liveness when stopped
	he.started = false
	he.stopped = true
	req = httptest.NewRequest(http.MethodGet, "/health/live", nil)
	w = httptest.NewRecorder()
	he.handleLiveness(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for dead, got %d", w.Code)
	}

	// Readiness reflects health status
	he.results["storage"] = HealthCheckResult{Name: "storage", Status: StatusHealthy}
	he.results["search"] = HealthCheckResult{Name: "search", Status: StatusHealthy}
	req = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w = httptest.NewRecorder()
	he.handleReadiness(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for ready, got %d", w.Code)
	}

	he.results["storage"] = HealthCheckResult{Name: "storage", Status: StatusUnhealthy}
	req = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w = httptest.NewRecorder()
	he.handleReadiness(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for not ready, got %d", w.Code)
	}
}
