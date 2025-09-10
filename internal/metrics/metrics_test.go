package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewMetrics_DefaultConfig(t *testing.T) {
	m, err := NewMetrics(nil)
	if err != nil {
		t.Fatalf("NewMetrics returned error: %v", err)
	}
	if m.GetRegistry() == nil {
		t.Fatal("registry should not be nil")
	}
	cfg := m.GetConfig()
	if cfg.Path != "/metrics" || cfg.Namespace != "vexdb" {
		t.Fatalf("unexpected default config: %#v", cfg)
	}
}

func TestMetrics_NewCounterReuse(t *testing.T) {
	m, _ := NewMetrics(nil)
	c1 := m.NewCounter("requests_total", "help", []string{"service"})
	c1.Inc("search")
	c2 := m.NewCounter("requests_total", "help", []string{"service"})
	if c1.metric != c2.metric {
		t.Fatal("expected existing counter to be reused")
	}
	if got := testutil.ToFloat64(c2.metric.WithLabelValues("search")); got != 1 {
		t.Fatalf("expected counter to be 1, got %v", got)
	}
}

func TestMetrics_RecordCounter(t *testing.T) {
	m, _ := NewMetrics(nil)
	m.RecordCounter("requests_total", 2, map[string]string{"service": "search"})
	cv, ok := m.collectors["counter:requests_total:service"].(*prometheus.CounterVec)
	if !ok {
		t.Fatalf("counter not registered")
	}
	if got := testutil.ToFloat64(cv.WithLabelValues("search")); got != 2 {
		t.Fatalf("expected counter to be 2, got %v", got)
	}
}

func TestIngestionMetrics_IncrementConnectionCount(t *testing.T) {
	m, _ := NewMetrics(nil)
	im := NewIngestionMetrics(m)
	im.IncrementConnectionCount("grpc")
	if got := testutil.ToFloat64(im.totalConnections.metric.WithLabelValues("grpc")); got != 1 {
		t.Fatalf("expected counter to be 1, got %v", got)
	}
}
