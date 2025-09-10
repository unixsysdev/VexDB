package adapter

import (
	"testing"
	"time"
)

func TestValidateProtocolConfig(t *testing.T) {
	cfg := DefaultProtocolConfig()
	if err := ValidateProtocolConfig(cfg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if err := ValidateProtocolConfig(nil); err == nil {
		t.Fatal("expected error for nil config")
	}

	badPort := *cfg
	badPort.Port = 0
	if err := ValidateProtocolConfig(&badPort); err == nil {
		t.Error("expected error for invalid port")
	}

	badTLS := *cfg
	badTLS.EnableTLS = true
	if err := ValidateProtocolConfig(&badTLS); err == nil {
		t.Error("expected error when TLS enabled without cert files")
	}
}

func TestConstructors(t *testing.T) {
	req := NewRequest("GET", "/path", []byte("body"))
	if req.ID == "" || req.Method != "GET" || req.Path != "/path" {
		t.Fatalf("unexpected request: %+v", req)
	}

	resp := NewResponse(200, []byte("ok"))
	if resp.Status != 200 || string(resp.Body) != "ok" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	conn := NewConnection("remote", "local", ProtocolHTTP)
	if !conn.Active || conn.Protocol != ProtocolHTTP || conn.RemoteAddr != "remote" {
		t.Fatalf("unexpected connection: %+v", conn)
	}
	if time.Since(conn.Established) > time.Second {
		t.Error("expected recent established timestamp")
	}
}
