package connection

import (
    "testing"
    "time"

    "go.uber.org/zap"
    "vexdb/internal/protocol/adapter"
)

func TestValidateConnectionManagerConfig(t *testing.T) {
    cfg := DefaultConnectionManagerConfig()
    if err := validateConnectionManagerConfig(cfg); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    bad := *cfg
    bad.MaxConnections = 0
    if err := validateConnectionManagerConfig(&bad); err == nil {
        t.Errorf("expected error for max connections")
    }

    bad = *cfg
    bad.IdleTimeout = 0
    if err := validateConnectionManagerConfig(&bad); err == nil {
        t.Errorf("expected error for idle timeout")
    }

    bad = *cfg
    bad.ReadTimeout = -1
    if err := validateConnectionManagerConfig(&bad); err == nil {
        t.Errorf("expected error for read timeout")
    }

    bad = *cfg
    bad.WriteTimeout = -1
    if err := validateConnectionManagerConfig(&bad); err == nil {
        t.Errorf("expected error for write timeout")
    }

    bad = *cfg
    bad.CleanupInterval = 0
    if err := validateConnectionManagerConfig(&bad); err == nil {
        t.Errorf("expected error for cleanup interval")
    }

    bad = *cfg
    bad.BufferSize = 0
    if err := validateConnectionManagerConfig(&bad); err == nil {
        t.Errorf("expected error for buffer size")
    }
}

func newTestConnection(id string) *adapter.Connection {
    now := time.Now()
    return &adapter.Connection{
        ID:           id,
        RemoteAddr:   "127.0.0.1",
        LocalAddr:    "",
        Protocol:     adapter.ProtocolHTTP,
        Established:  now,
        LastActivity: now,
        Active:       true,
    }
}

func TestConnectionManagerLifecycle(t *testing.T) {
    baseLogger := zap.NewNop()
    manager, err := NewConnectionManager(nil, *baseLogger, nil)
    if err != nil {
        t.Fatalf("failed to create manager: %v", err)
    }

    if err := manager.Start(); err != nil {
        t.Fatalf("start failed: %v", err)
    }
    t.Log("started")
    if !manager.IsRunning() {
        t.Fatal("manager should be running")
    }
    if err := manager.Start(); err != ErrManagerAlreadyRunning {
        t.Errorf("expected ErrManagerAlreadyRunning, got %v", err)
    }
    t.Log("double start checked")

    cfg := manager.GetConfig()
    cfg.MaxConnections = 1
    if err := manager.UpdateConfig(cfg); err != nil {
        t.Fatalf("update config: %v", err)
    }
    t.Log("config updated")

    c1 := newTestConnection("c1")
    if err := manager.AddConnection(c1); err != nil {
        t.Fatalf("add first connection: %v", err)
    }
    t.Log("added first connection")
    c2 := newTestConnection("c2")
    if err := manager.AddConnection(c2); err != ErrConnectionLimitExceeded {
        t.Errorf("expected limit error, got %v", err)
    }
    t.Log("limit enforced")

    got, ok := manager.GetConnection("c1")
    if !ok || got.ID != "c1" {
        t.Fatalf("expected to retrieve connection c1")
    }
    t.Log("retrieved connection")

    manager.UpdateConnectionStats("c1", 10, 20, 1)
    if got.BytesRead != 10 || got.BytesWritten != 20 || got.Requests != 1 {
        t.Errorf("unexpected connection stats: %+v", got)
    }
    stats := manager.GetStats()
    if stats.TotalBytesRead != 10 || stats.TotalBytesWritten != 20 || stats.TotalRequests != 1 {
        t.Errorf("unexpected manager stats: %+v", stats)
    }
    t.Log("stats updated")

    if err := manager.CloseConnection("c1"); err != nil {
        t.Fatalf("close connection: %v", err)
    }
    if got.Active {
        t.Error("connection should be inactive after close")
    }
    t.Log("connection closed")

    if err := manager.RemoveConnection("c1"); err != nil {
        t.Fatalf("remove connection: %v", err)
    }
    if _, ok := manager.GetConnection("c1"); ok {
        t.Error("connection should be removed")
    }
    if err := manager.RemoveConnection("missing"); err != ErrConnectionNotFound {
        t.Errorf("expected ErrConnectionNotFound, got %v", err)
    }
    t.Log("removal validated")

    if err := manager.Stop(); err != nil {
        t.Fatalf("stop failed: %v", err)
    }
    if manager.IsRunning() {
        t.Error("manager should not be running")
    }
    if err := manager.Stop(); err != nil {
        t.Fatalf("second stop should be nil, got %v", err)
    }
    t.Log("stopped")
}

func TestCloseAllConnections(t *testing.T) {
    baseLogger := zap.NewNop()
    manager, err := NewConnectionManager(nil, *baseLogger, nil)
    if err != nil {
        t.Fatalf("create manager: %v", err)
    }
    if err := manager.Start(); err != nil {
        t.Fatalf("start manager: %v", err)
    }

    c1 := newTestConnection("a")
    c2 := newTestConnection("b")
    manager.AddConnection(c1)
    manager.AddConnection(c2)

    if err := manager.CloseAllConnections(); err != nil {
        t.Fatalf("close all connections: %v", err)
    }
    for _, c := range manager.GetAllConnections() {
        if c.Active {
            t.Errorf("connection %s should be inactive", c.ID)
        }
    }
}

