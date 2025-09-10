package error

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateErrorConfig(t *testing.T) {
	if err := validateErrorConfig(nil); err == nil {
		t.Fatalf("expected error for nil config")
	}

	cfg := DefaultErrorConfig()
	cfg.MaxErrorSize = 0
	if err := validateErrorConfig(cfg); err == nil {
		t.Fatalf("expected error for max error size")
	}

	cfg = DefaultErrorConfig()
	cfg.RecoveryStrategies = append(cfg.RecoveryStrategies, RecoveryStrategy{Name: "", Type: "retry", MaxAttempts: 1})
	if err := validateErrorConfig(cfg); err == nil {
		t.Fatalf("expected error for empty recovery strategy name")
	}

	cfg = DefaultErrorConfig()
	cfg.NotificationChannels = append(cfg.NotificationChannels, NotificationChannel{Name: "", Type: "email"})
	if err := validateErrorConfig(cfg); err == nil {
		t.Fatalf("expected error for empty notification channel name")
	}

	if err := validateErrorConfig(DefaultErrorConfig()); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

func TestGetErrorType(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{errors.New("connection refused"), "connection_refused"},
		{errors.New("timeout occurred"), "timeout"},
		{errors.New("not found"), "not_found"},
		{errors.New("permission denied"), "permission_denied"},
		{errors.New("invalid argument"), "invalid_argument"},
		{errors.New("some other error"), "unknown"},
		{nil, "unknown"},
	}
	for _, tt := range tests {
		if got := getErrorType(tt.err); got != tt.want {
			t.Errorf("getErrorType(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func TestGenerateErrorID(t *testing.T) {
	id1 := generateErrorID()
	id2 := generateErrorID()
	if !strings.HasPrefix(id1, "err_") || !strings.HasPrefix(id2, "err_") {
		t.Fatalf("IDs should have err_ prefix: %s %s", id1, id2)
	}
	if id1 == id2 {
		t.Fatalf("expected unique IDs, got %s twice", id1)
	}
}

func TestGetRetryableFromMetadata(t *testing.T) {
	if !getRetryableFromMetadata(map[string]interface{}{"retryable": true}) {
		t.Fatalf("expected retryable true")
	}
	if getRetryableFromMetadata(map[string]interface{}{"retryable": "yes"}) {
		t.Fatalf("non-bool retryable should be false")
	}
	if getRetryableFromMetadata(nil) {
		t.Fatalf("nil metadata should be false")
	}
}
