package grpc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"vexdb/internal/metrics"
)

// getFreePort returns an available TCP port for testing.
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestValidateGRPCConfig(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*GRPCConfig)
		want   string
	}{
		{
			name:   "empty host",
			modify: func(cfg *GRPCConfig) { cfg.Host = "" },
			want:   "host cannot be empty",
		},
		{
			name:   "invalid port",
			modify: func(cfg *GRPCConfig) { cfg.Port = 0 },
			want:   "port must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultGRPCConfig()
			tt.modify(cfg)
			if err := validateGRPCConfig(cfg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestGRPCAdapterStartStop(t *testing.T) {
	m, _ := metrics.NewMetrics(nil)
	im := metrics.NewIngestionMetrics(m)
	logger := zap.NewNop()
	adapter, err := NewGRPCAdapter(nil, *logger, im, nil, nil)
	if err != nil {
		t.Fatalf("NewGRPCAdapter returned error: %v", err)
	}
	adapter.config.Port = getFreePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	// Allow server to start
	time.Sleep(50 * time.Millisecond)

	// Ensure server is listening by dialing
	conn, err := net.DialTimeout("tcp", adapter.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("failed to dial gRPC server: %v", err)
	}
	conn.Close()

	if err := adapter.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}
