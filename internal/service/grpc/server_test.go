package grpc

import (
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
)

type mapConfig map[string]interface{}

func (m mapConfig) Validate() error                    { return nil }
func (m mapConfig) Get(key string) (interface{}, bool) { v, ok := m[key]; return v, ok }
func (m mapConfig) GetMetricsConfig() interface{}      { return nil }

// TestValidateServerConfig ensures configuration validation catches errors.
func TestValidateServerConfig(t *testing.T) {
	cfg := DefaultServerConfig()
	if err := validateServerConfig(cfg); err != nil {
		t.Fatalf("valid config returned error: %v", err)
	}

	cfg.Host = ""
	if err := validateServerConfig(cfg); err == nil {
		t.Fatalf("expected error for empty host")
	}
	cfg.Host = "127.0.0.1"

	cfg.Port = 70000
	if err := validateServerConfig(cfg); err == nil {
		t.Fatalf("expected error for invalid port")
	}
	cfg.Port = 50051

	cfg.MaxConnectionAge = 0
	if err := validateServerConfig(cfg); err == nil {
		t.Fatalf("expected error for non-positive max connection age")
	}
	cfg.MaxConnectionAge = time.Second
	cfg.MaxConnectionAgeGrace = time.Second

	cfg.EnableTLS = true
	cfg.TLSCertFile = ""
	cfg.TLSKeyFile = ""
	if err := validateServerConfig(cfg); err == nil {
		t.Fatalf("expected error for missing TLS files")
	}

	cfg.EnableTLS = false
	cfg.KeepaliveParams = &KeepaliveParams{}
	if err := validateServerConfig(cfg); err == nil {
		t.Fatalf("expected error for keepalive params")
	}
}

// TestStartStop verifies server lifecycle transitions.
func TestStartStop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cfg := mapConfig{"grpc": map[string]interface{}{
		"host":                "127.0.0.1",
		"port":                port,
		"enable_reflection":   false,
		"enable_health_check": false,
	}}

	srv, err := NewServer(cfg, *zap.NewNop(), nil, nil)
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}

	if srv.IsRunning() {
		t.Fatalf("server should not be running before Start")
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if !srv.IsRunning() {
		t.Fatalf("server should be running after Start")
	}

	if err := srv.Start(); err != ErrServerAlreadyRunning {
		t.Fatalf("expected ErrServerAlreadyRunning, got %v", err)
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	if srv.IsRunning() {
		t.Fatalf("server should not be running after Stop")
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("second stop should not error: %v", err)
	}
}

// TestStopWithoutStart ensures stopping before start returns appropriate error.
func TestStopWithoutStart(t *testing.T) {
	srv, err := NewServer(nil, *zap.NewNop(), nil, nil)
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}
	if err := srv.Stop(); err != ErrServerNotRunning {
		t.Fatalf("expected ErrServerNotRunning, got %v", err)
	}
}

// TestRegisterService ensures services can be registered and duplicate names are rejected.
func TestRegisterService(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cfg := mapConfig{"grpc": map[string]interface{}{
		"host":                "127.0.0.1",
		"port":                port,
		"enable_reflection":   false,
		"enable_health_check": false,
	}}

	srv, err := NewServer(cfg, *zap.NewNop(), nil, nil)
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer srv.Stop()

	if err := srv.RegisterService("test", struct{}{}); err != nil {
		t.Fatalf("register service failed: %v", err)
	}
	if err := srv.RegisterService("test", struct{}{}); err == nil {
		t.Fatalf("expected error for duplicate service registration")
	}

	if err := srv.UnregisterService("test"); err != nil {
		t.Fatalf("unregister service failed: %v", err)
	}
	if err := srv.UnregisterService("test"); err == nil {
		t.Fatalf("expected error for unregistered service")
	}
}
