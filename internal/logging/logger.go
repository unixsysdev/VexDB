package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger creates and configures a new zap logger
func NewLogger() (*zap.Logger, error) {
	// Configure zap logger with production settings
	config := zap.NewProductionConfig()
	
	// Adjust configuration as needed
	config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}
	
	// Build the logger
	logger, err := config.Build()
	if err != nil {
		return nil, err
	}
	
	return logger, nil
}