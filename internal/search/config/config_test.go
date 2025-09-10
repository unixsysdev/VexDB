package config

import (
	"testing"
	"time"
)

func TestDefaultSearchServiceConfig(t *testing.T) {
	config := DefaultSearchServiceConfig()

	if config == nil {
		t.Fatal("Expected config to be created, got nil")
	}

	// Test server configuration
	if config.Server.Host != "0.0.0.0" {
		t.Errorf("Expected Server.Host to be '0.0.0.0', got '%s'", config.Server.Host)
	}

	if config.Server.Port != 8080 {
		t.Errorf("Expected Server.Port to be 8080, got %d", config.Server.Port)
	}

	if config.Server.ReadTimeout != 30*time.Second {
		t.Errorf("Expected Server.ReadTimeout to be 30s, got %v", config.Server.ReadTimeout)
	}

	// Test engine configuration
	if config.Engine.MaxResults != 1000 {
		t.Errorf("Expected Engine.MaxResults to be 1000, got %d", config.Engine.MaxResults)
	}

	if config.Engine.DefaultK != 10 {
		t.Errorf("Expected Engine.DefaultK to be 10, got %d", config.Engine.DefaultK)
	}

	if config.Engine.DistanceType != "euclidean" {
		t.Errorf("Expected Engine.DistanceType to be 'euclidean', got '%s'", config.Engine.DistanceType)
	}

	if !config.Engine.EnableCache {
		t.Error("Expected Engine.EnableCache to be true")
	}

	if config.Engine.CacheSize != 1000 {
		t.Errorf("Expected Engine.CacheSize to be 1000, got %d", config.Engine.CacheSize)
	}

	// Test API configuration
	if !config.API.HTTP.Enabled {
		t.Error("Expected API.HTTP.Enabled to be true")
	}

	if config.API.HTTP.BasePath != "/api/v1" {
		t.Errorf("Expected API.HTTP.BasePath to be '/api/v1', got '%s'", config.API.HTTP.BasePath)
	}

	if !config.API.GRPC.Enabled {
		t.Error("Expected API.GRPC.Enabled to be true")
	}

	// Test authentication configuration
	if config.Auth.Enabled {
		t.Error("Expected Auth.Enabled to be false by default")
	}

	if config.Auth.Type != "none" {
		t.Errorf("Expected Auth.Type to be 'none', got '%s'", config.Auth.Type)
	}

	// Test rate limiting configuration
	if config.RateLimit.Enabled {
		t.Error("Expected RateLimit.Enabled to be false by default")
	}

	if config.RateLimit.RequestsPerSecond != 100.0 {
		t.Errorf("Expected RateLimit.RequestsPerSecond to be 100.0, got %f", config.RateLimit.RequestsPerSecond)
	}

	// Test metrics configuration
	if !config.Metrics.Enabled {
		t.Error("Expected Metrics.Enabled to be true")
	}

	if config.Metrics.Namespace != "vxdb" {
		t.Errorf("Expected Metrics.Namespace to be 'vxdb', got '%s'", config.Metrics.Namespace)
	}

	// Test logging configuration
	if config.Logging.Level != "info" {
		t.Errorf("Expected Logging.Level to be 'info', got '%s'", config.Logging.Level)
	}

	if config.Logging.Format != "json" {
		t.Errorf("Expected Logging.Format to be 'json', got '%s'", config.Logging.Format)
	}

	// Test health configuration
	if !config.Health.Enabled {
		t.Error("Expected Health.Enabled to be true")
	}

	if config.Health.CheckInterval != 30*time.Second {
		t.Errorf("Expected Health.CheckInterval to be 30s, got %v", config.Health.CheckInterval)
	}
}

func TestSearchServiceConfig_Validate_ValidConfig(t *testing.T) {
	config := DefaultSearchServiceConfig()

	err := config.Validate()

	if err != nil {
		t.Errorf("Expected no validation error for default config, got: %v", err)
	}
}

func TestSearchServiceConfig_Validate_InvalidServerHost(t *testing.T) {
	config := DefaultSearchServiceConfig()
	config.Server.Host = ""

	err := config.Validate()

	if err == nil {
		t.Error("Expected validation error for empty server host, got nil")
	}

	if err.Error() != "server host is required" {
		t.Errorf("Expected 'server host is required', got: %v", err)
	}
}

func TestSearchServiceConfig_Validate_InvalidServerPort(t *testing.T) {
	config := DefaultSearchServiceConfig()

	// Test port too low
	config.Server.Port = 0
	err := config.Validate()
	if err == nil {
		t.Error("Expected validation error for port 0, got nil")
	}

	// Test port too high
	config.Server.Port = 70000
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for port 70000, got nil")
	}
}

func TestSearchServiceConfig_Validate_InvalidServerTimeouts(t *testing.T) {
	config := DefaultSearchServiceConfig()

	// Test negative read timeout
	config.Server.ReadTimeout = -1 * time.Second
	err := config.Validate()
	if err == nil {
		t.Error("Expected validation error for negative read timeout, got nil")
	}

	// Reset and test zero read timeout
	config = DefaultSearchServiceConfig()
	config.Server.ReadTimeout = 0
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for zero read timeout, got nil")
	}

	// Test negative write timeout
	config = DefaultSearchServiceConfig()
	config.Server.WriteTimeout = -1 * time.Second
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for negative write timeout, got nil")
	}
}

func TestSearchServiceConfig_Validate_InvalidEngineConfig(t *testing.T) {
	config := DefaultSearchServiceConfig()

	// Test negative max results
	config.Engine.MaxResults = -1
	err := config.Validate()
	if err == nil {
		t.Error("Expected validation error for negative max results, got nil")
	}

	// Test zero max results
	config = DefaultSearchServiceConfig()
	config.Engine.MaxResults = 0
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for zero max results, got nil")
	}

	// Test negative default k
	config = DefaultSearchServiceConfig()
	config.Engine.DefaultK = -1
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for negative default k, got nil")
	}

	// Test zero default k
	config = DefaultSearchServiceConfig()
	config.Engine.DefaultK = 0
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for zero default k, got nil")
	}

	// Test invalid distance type
	config = DefaultSearchServiceConfig()
	config.Engine.DistanceType = "invalid"
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid distance type, got nil")
	}
}

func TestSearchServiceConfig_Validate_InvalidAPIConfig(t *testing.T) {
	config := DefaultSearchServiceConfig()

	// Test empty base path
	config.API.HTTP.BasePath = ""
	err := config.Validate()
	if err == nil {
		t.Error("Expected validation error for empty base path, got nil")
	}

	// Test negative gRPC max message size
	config = DefaultSearchServiceConfig()
	config.API.GRPC.MaxMessageSize = -1
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for negative gRPC max message size, got nil")
	}

	// Test zero gRPC max message size
	config = DefaultSearchServiceConfig()
	config.API.GRPC.MaxMessageSize = 0
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for zero gRPC max message size, got nil")
	}
}

func TestSearchServiceConfig_Validate_InvalidAuthConfig(t *testing.T) {
	config := DefaultSearchServiceConfig()
	config.Auth.Enabled = true

	// Test invalid auth type
	config.Auth.Type = "invalid"
	err := config.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid auth type, got nil")
	}
}

func TestSearchServiceConfig_Validate_InvalidRateLimitConfig(t *testing.T) {
	config := DefaultSearchServiceConfig()
	config.RateLimit.Enabled = true

	// Test negative requests per second
	config.RateLimit.RequestsPerSecond = -1.0
	err := config.Validate()
	if err == nil {
		t.Error("Expected validation error for negative requests per second, got nil")
	}

	// Test zero requests per second
	config = DefaultSearchServiceConfig()
	config.RateLimit.Enabled = true
	config.RateLimit.RequestsPerSecond = 0.0
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for zero requests per second, got nil")
	}

	// Test negative burst size
	config = DefaultSearchServiceConfig()
	config.RateLimit.Enabled = true
	config.RateLimit.BurstSize = -1
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for negative burst size, got nil")
	}

	// Test zero burst size
	config = DefaultSearchServiceConfig()
	config.RateLimit.Enabled = true
	config.RateLimit.BurstSize = 0
	err = config.Validate()
	if err == nil {
		t.Error("Expected validation error for zero burst size, got nil")
	}
}

func TestGetSearchConfig(t *testing.T) {
	config := GetSearchConfig(nil)

	if config == nil {
		t.Fatal("Expected config to be returned, got nil")
	}

	// Should return default config
	defaultConfig := DefaultSearchServiceConfig()

	if config.Server.Host != defaultConfig.Server.Host {
		t.Errorf("Expected Server.Host to be '%s', got '%s'", defaultConfig.Server.Host, config.Server.Host)
	}

	if config.Server.Port != defaultConfig.Server.Port {
		t.Errorf("Expected Server.Port to be %d, got %d", defaultConfig.Server.Port, config.Server.Port)
	}
}

func TestServerConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  ServerConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: ServerConfig{
				Host:           "localhost",
				Port:           8080,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   30 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1048576,
			},
			wantErr: false,
		},
		{
			name: "empty host",
			config: ServerConfig{
				Host:           "",
				Port:           8080,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   30 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1048576,
			},
			wantErr: true,
			errMsg:  "server host is required",
		},
		{
			name: "invalid port - too low",
			config: ServerConfig{
				Host:           "localhost",
				Port:           0,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   30 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1048576,
			},
			wantErr: true,
			errMsg:  "server port must be between 1 and 65535",
		},
		{
			name: "invalid port - too high",
			config: ServerConfig{
				Host:           "localhost",
				Port:           70000,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   30 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1048576,
			},
			wantErr: true,
			errMsg:  "server port must be between 1 and 65535",
		},
		{
			name: "negative read timeout",
			config: ServerConfig{
				Host:           "localhost",
				Port:           8080,
				ReadTimeout:    -1 * time.Second,
				WriteTimeout:   30 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1048576,
			},
			wantErr: true,
			errMsg:  "server read timeout must be positive",
		},
		{
			name: "negative write timeout",
			config: ServerConfig{
				Host:           "localhost",
				Port:           8080,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   -1 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1048576,
			},
			wantErr: true,
			errMsg:  "server write timeout must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &SearchServiceConfig{
				Server:    tt.config,
				Engine:    DefaultSearchServiceConfig().Engine,
				API:       DefaultSearchServiceConfig().API,
				Auth:      DefaultSearchServiceConfig().Auth,
				RateLimit: DefaultSearchServiceConfig().RateLimit,
				Metrics:   DefaultSearchServiceConfig().Metrics,
				Logging:   DefaultSearchServiceConfig().Logging,
				Health:    DefaultSearchServiceConfig().Health,
			}

			err := config.Validate()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected validation error, got nil")
				} else if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, got: %v", err)
				}
			}
		})
	}
}

func TestEngineConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  EngineConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: EngineConfig{
				MaxResults:     1000,
				DefaultK:       10,
				DistanceType:   "euclidean",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: false,
		},
		{
			name: "negative max results",
			config: EngineConfig{
				MaxResults:     -1,
				DefaultK:       10,
				DistanceType:   "euclidean",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: true,
			errMsg:  "engine max results must be positive",
		},
		{
			name: "zero max results",
			config: EngineConfig{
				MaxResults:     0,
				DefaultK:       10,
				DistanceType:   "euclidean",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: true,
			errMsg:  "engine max results must be positive",
		},
		{
			name: "negative default k",
			config: EngineConfig{
				MaxResults:     1000,
				DefaultK:       -1,
				DistanceType:   "euclidean",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: true,
			errMsg:  "engine default k must be positive",
		},
		{
			name: "zero default k",
			config: EngineConfig{
				MaxResults:     1000,
				DefaultK:       0,
				DistanceType:   "euclidean",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: true,
			errMsg:  "engine default k must be positive",
		},
		{
			name: "invalid distance type",
			config: EngineConfig{
				MaxResults:     1000,
				DefaultK:       10,
				DistanceType:   "invalid",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: true,
			errMsg:  "engine distance type must be one of: euclidean, cosine, manhattan",
		},
		{
			name: "valid distance types",
			config: EngineConfig{
				MaxResults:     1000,
				DefaultK:       10,
				DistanceType:   "cosine",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: false,
		},
		{
			name: "manhattan distance type",
			config: EngineConfig{
				MaxResults:     1000,
				DefaultK:       10,
				DistanceType:   "manhattan",
				EnableCache:    true,
				CacheSize:      1000,
				CacheTTL:       5 * time.Minute,
				ParallelSearch: true,
				MaxWorkers:     4,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &SearchServiceConfig{
				Server:    DefaultSearchServiceConfig().Server,
				Engine:    tt.config,
				API:       DefaultSearchServiceConfig().API,
				Auth:      DefaultSearchServiceConfig().Auth,
				RateLimit: DefaultSearchServiceConfig().RateLimit,
				Metrics:   DefaultSearchServiceConfig().Metrics,
				Logging:   DefaultSearchServiceConfig().Logging,
				Health:    DefaultSearchServiceConfig().Health,
			}

			err := config.Validate()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected validation error, got nil")
				} else if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, got: %v", err)
				}
			}
		})
	}
}
