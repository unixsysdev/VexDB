package health

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"vxdb/internal/metrics"
)

func TestDefaultMonitorConfig(t *testing.T) {
	cfg := DefaultMonitorConfig()
	if cfg.CheckInterval != 30*time.Second {
		t.Fatalf("unexpected check interval: %v", cfg.CheckInterval)
	}
	if cfg.Timeout != 5*time.Second {
		t.Fatalf("unexpected timeout: %v", cfg.Timeout)
	}
	if cfg.HealthyThreshold != 3 || cfg.UnhealthyThreshold != 5 {
		t.Fatalf("unexpected thresholds: %+v", cfg)
	}
	if !cfg.EnableMetrics {
		t.Fatalf("metrics should be enabled by default")
	}
}

func TestConnectionHealthMonitor_RegisterAndUnregister(t *testing.T) {
	m, _ := metrics.NewMetrics(nil)
	im := metrics.NewIngestionMetrics(m)
	logger := zap.NewNop()

	monitor := NewConnectionHealthMonitor(nil, *logger, im)
	conn := monitor.RegisterConnection("c1", "grpc", "addr")

	if conn.Status != StatusHealthy {
		t.Fatalf("expected new connection to be healthy, got %v", conn.Status)
	}
	if _, ok := monitor.GetConnectionHealth("c1"); !ok {
		t.Fatalf("connection not registered")
	}

	monitor.UnregisterConnection("c1")
	if _, ok := monitor.GetConnectionHealth("c1"); ok {
		t.Fatalf("connection not unregistered")
	}
}

func TestConnectionHealthMonitor_PerformChecksAndClose(t *testing.T) {
	m, _ := metrics.NewMetrics(nil)
	im := metrics.NewIngestionMetrics(m)
	logger := zap.NewNop()

	cfg := &MonitorConfig{CheckInterval: time.Millisecond, Timeout: 10 * time.Millisecond, HealthyThreshold: 1, UnhealthyThreshold: 1, EnableMetrics: false}
	monitor := NewConnectionHealthMonitor(cfg, *logger, im)
	conn := monitor.RegisterConnection("c1", "grpc", "addr")

	// simulate inactivity to trigger unhealthy status
	conn.mu.Lock()
	conn.LastActivity = time.Now().Add(-30 * time.Millisecond)
	conn.mu.Unlock()

	monitor.performHealthChecks()
	if conn.Status != StatusUnhealthy {
		t.Fatalf("expected connection to be unhealthy, got %v", conn.Status)
	}

	closed := monitor.CloseUnhealthyConnections()
	if closed != 1 {
		t.Fatalf("expected 1 closed connection, got %d", closed)
	}
}

func TestConnectionHealthMonitor_StartStop(t *testing.T) {
	m, _ := metrics.NewMetrics(nil)
	im := metrics.NewIngestionMetrics(m)
	logger := zap.NewNop()

	cfg := &MonitorConfig{CheckInterval: time.Millisecond, Timeout: time.Millisecond}
	monitor := NewConnectionHealthMonitor(cfg, *logger, im)

	ctx, cancel := context.WithCancel(context.Background())
	if err := monitor.Start(ctx); err != nil {
		t.Fatalf("start returned error: %v", err)
	}
	cancel()
	monitor.Stop()
}
