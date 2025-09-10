package monitoring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func TestCollectorRegisterCustomMetric(t *testing.T) {
	cfg := &Config{Enabled: false, Namespace: "custom_ns", ServiceName: "svc", ServiceInstance: "inst", ServiceVersion: "v"}
	c := NewCollector(zap.NewNop(), cfg)

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "custom_metric", Help: "custom"})
	if err := c.RegisterCustomMetric("custom_metric", gauge); err != nil {
		t.Fatalf("register metric failed: %v", err)
	}
	metrics := c.GetCustomMetrics()
	if _, ok := metrics["custom_metric"]; !ok {
		t.Fatalf("metric not found after registration")
	}
	if err := c.RegisterCustomMetric("custom_metric", gauge); err == nil {
		t.Fatalf("expected error on duplicate metric registration")
	}
}

func TestCollectorAlerts(t *testing.T) {
	cfg := &Config{Enabled: false, Namespace: "alert_ns", ServiceName: "svc", ServiceInstance: "inst", ServiceVersion: "v"}
	c := NewCollector(zap.NewNop(), cfg)

	fired := false
	c.RegisterAlert("a1", "test", "desc", "critical", func() bool { return fired }, true)
	c.checkAlert(c.alerts["a1"])
	if c.GetAlerts()["a1"].Count != 0 {
		t.Fatalf("alert should not fire yet")
	}
	fired = true
	c.checkAlert(c.alerts["a1"])
	if c.GetAlerts()["a1"].Count != 1 {
		t.Fatalf("alert should fire once after condition met")
	}
}

func TestCollectorHealthCheck(t *testing.T) {
	cfg := &Config{Enabled: false, Namespace: "health_ns", ServiceName: "svc", ServiceInstance: "inst", ServiceVersion: "v"}
	c := NewCollector(zap.NewNop(), cfg)

	c.RegisterHealthCheck("ok", func(ctx context.Context) error { return nil }, time.Millisecond, time.Millisecond)
	c.runHealthCheck(c.healthChecks["ok"])
	if status := c.GetHealthStatus()["ok"]; !status.LastStatus || status.LastError != nil {
		t.Fatalf("expected successful health check")
	}

	c.RegisterHealthCheck("fail", func(ctx context.Context) error { return errors.New("bad") }, time.Millisecond, time.Millisecond)
	c.runHealthCheck(c.healthChecks["fail"])
	if status := c.GetHealthStatus()["fail"]; status.LastStatus || status.LastError == nil {
		t.Fatalf("expected failing health check")
	}
}
