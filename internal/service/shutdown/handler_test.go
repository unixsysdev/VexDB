package shutdown

import (
	"context"
	"testing"

	"go.uber.org/zap"
)

type mapConfig map[string]interface{}

func (m mapConfig) Validate() error                    { return nil }
func (m mapConfig) Get(key string) (interface{}, bool) { v, ok := m[key]; return v, ok }
func (m mapConfig) GetMetricsConfig() interface{}      { return nil }

// TestValidateShutdownConfig verifies the config validation rules.
func TestValidateShutdownConfig(t *testing.T) {
	cfg := DefaultShutdownConfig()
	if err := validateShutdownConfig(cfg); err != nil {
		t.Fatalf("valid config returned error: %v", err)
	}

	cfg.Timeout = 0
	if err := validateShutdownConfig(cfg); err == nil {
		t.Fatalf("expected error for zero timeout")
	}

	cfg = DefaultShutdownConfig()
	cfg.GracePeriod = cfg.Timeout
	if err := validateShutdownConfig(cfg); err == nil {
		t.Fatalf("expected error when grace period >= timeout")
	}

	cfg = DefaultShutdownConfig()
	cfg.Signals = []string{""}
	if err := validateShutdownConfig(cfg); err == nil {
		t.Fatalf("expected error for empty signal")
	}
}

// TestStartStopLifecycle ensures handler start/stop transitions.
func TestStartStopLifecycle(t *testing.T) {
	cfg := mapConfig{"shutdown": map[string]interface{}{"enable_signals": false}}
	h, err := NewShutdownHandler(cfg, *zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("handler creation failed: %v", err)
	}

	if h.IsRunning() {
		t.Fatalf("handler should not be running before Start")
	}

	if err := h.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if !h.IsRunning() {
		t.Fatalf("handler should be running after Start")
	}

	if err := h.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if h.IsRunning() {
		t.Fatalf("handler should not be running after Stop")
	}

	if err := h.Stop(); err != nil {
		t.Fatalf("second stop should not error: %v", err)
	}
}

// TestRegisterHookAndShutdown checks hook execution during shutdown.
func TestRegisterHookAndShutdown(t *testing.T) {
	cfg := mapConfig{"shutdown": map[string]interface{}{"enable_signals": false}}
	h, err := NewShutdownHandler(cfg, *zap.NewNop(), nil)
	if err != nil {
		t.Fatalf("handler creation failed: %v", err)
	}

	if err := h.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	executed := false
	hook := func(ctx context.Context) error {
		executed = true
		return nil
	}
	if err := h.RegisterHook("test", hook, ShutdownHookConfig{Order: 1}); err != nil {
		t.Fatalf("register hook failed: %v", err)
	}

	if err := h.Shutdown(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
	if !executed {
		t.Fatalf("expected hook to execute during shutdown")
	}

	report := h.GetShutdownReport()
	if report == nil || report.Summary.TotalHooks != 1 || len(report.Results) != 1 || !report.Results[0].Success {
		t.Fatalf("unexpected shutdown report: %+v", report)
	}
}
