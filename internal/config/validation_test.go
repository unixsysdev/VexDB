package config_test

import (
	"testing"

	cfg "vxdb/internal/config"
)

type simple struct {
	Port int    `yaml:"port"`
	Name string `yaml:"name"`
}

func TestConfigValidator(t *testing.T) {
	v := cfg.NewConfigValidator()
	v.AddSchema("port", &cfg.Schema{Type: "int", Required: true, Rules: []cfg.ValidationRule{cfg.Port()}})
	v.AddSchema("name", &cfg.Schema{Type: "string", Default: "vx"})

	s := &simple{Port: 8080}
	if err := v.Validate(s); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if s.Name != "vx" {
		t.Fatalf("expected default name applied, got %s", s.Name)
	}

	s.Port = 0
	if err := v.Validate(s); err == nil {
		t.Fatalf("expected validation error for port")
	}
}

func TestValidationRules(t *testing.T) {
	if err := cfg.Positive().Validate(1); err != nil {
		t.Fatalf("positive failed: %v", err)
	}
	if err := cfg.Positive().Validate(-1); err == nil {
		t.Fatalf("expected positive rule to fail")
	}
	if err := cfg.NonNegative().Validate(-1); err == nil {
		t.Fatalf("expected non-negative rule to fail")
	}
	if err := cfg.Port().Validate(70000); err == nil {
		t.Fatalf("expected port rule to fail for out of range")
	}
	if err := cfg.URL().Validate("http://example.com"); err != nil {
		t.Fatalf("url rule unexpected error: %v", err)
	}
	if err := cfg.URL().Validate("invalid"); err == nil {
		t.Fatalf("expected url rule to fail")
	}
	if err := cfg.Email().Validate("user@example.com"); err != nil {
		t.Fatalf("email rule unexpected error: %v", err)
	}
	if err := cfg.Email().Validate("not-email"); err == nil {
		t.Fatalf("expected email rule to fail")
	}
}
