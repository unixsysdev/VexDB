package config_test

import (
	"os"
	"testing"

	cfg "vxdb/internal/config"
)

func TestLoadConfigUnknownService(t *testing.T) {
	if _, err := cfg.LoadConfig("unknown", ""); err == nil {
		t.Fatalf("expected error for unknown service type")
	}
}

func TestInsertConfigValidateAndGet(t *testing.T) {
	c := &cfg.InsertConfig{Server: cfg.ServerConfig{Host: "localhost", Port: 8080}}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if val, ok := c.Get("server"); !ok || val.(cfg.ServerConfig).Host != "localhost" {
		t.Fatalf("get server failed: %#v", val)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatalf("expected missing key to return false")
	}
	if c.GetMetricsConfig() != nil {
		t.Fatalf("expected nil metrics config")
	}
}

func TestInsertConfigValidateErrors(t *testing.T) {
	c := &cfg.InsertConfig{Server: cfg.ServerConfig{Port: -1}}
	if err := c.Validate(); err == nil {
		t.Fatalf("expected validation error for missing host and negative port")
	}
}

func TestLoadInsertConfig(t *testing.T) {
	data := []byte("server:\n  host: example\n  port: 9000\n")
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()
	cfgFile := f.Name()
	conf, err := cfg.LoadInsertConfig(cfgFile)
	if err != nil {
		t.Fatalf("load insert config: %v", err)
	}
	if conf.Server.Host != "example" || conf.Server.Port != 9000 {
		t.Fatalf("unexpected config: %#v", conf.Server)
	}
}
