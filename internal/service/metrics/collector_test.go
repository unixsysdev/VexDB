package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type mapConfig map[string]interface{}

func (m mapConfig) Validate() error                    { return nil }
func (m mapConfig) Get(key string) (interface{}, bool) { v, ok := m[key]; return v, ok }
func (m mapConfig) GetMetricsConfig() interface{}      { return nil }

// TestValidateMetricsConfig ensures validation catches invalid ports and paths.
func TestValidateMetricsConfig(t *testing.T) {
	cfg := DefaultMetricsConfig()
	if err := validateMetricsConfig(cfg); err != nil {
		t.Fatalf("valid config returned error: %v", err)
	}

	cfg.Port = -1
	if err := validateMetricsConfig(cfg); err == nil {
		t.Fatalf("expected error for invalid port")
	}

	cfg.Port = 9090
	cfg.Path = ""
	if err := validateMetricsConfig(cfg); err == nil {
		t.Fatalf("expected error for empty path")
	}
}

// TestStartStop verifies collector lifecycle transitions.
func TestStartStop(t *testing.T) {
	// Reset default registry to avoid duplicate registrations across tests
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	prometheus.DefaultGatherer = prometheus.NewRegistry()

	logger := zap.NewNop()

	// Disable Go metrics to avoid duplicate Go collector registration
	cfg := mapConfig{
		"metrics": map[string]interface{}{
			"enable_go_metrics": false,
		},
	}

	collector, err := NewMetricsCollector(cfg, *logger)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	if collector.IsRunning() {
		t.Fatalf("collector should not be running before Start")
	}

	if err := collector.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if !collector.IsRunning() {
		t.Fatalf("collector should be running after Start")
	}

	if err := collector.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	if collector.IsRunning() {
		t.Fatalf("collector should not be running after Stop")
	}

	if err := collector.Stop(); err != nil {
		t.Fatalf("second stop should not error: %v", err)
	}
}
