package logging_test

import (
	"testing"

	"vexdb/internal/logging"
)

func TestNewLogger(t *testing.T) {
	logger, err := logging.NewLogger()
	if err != nil {
		t.Fatalf("NewLogger returned error: %v", err)
	}
	// Basic usage to ensure returned logger is usable
	logger.Info("hello")
}
