package config

import (
	"fmt"
	"time"
)

// SearchServiceConfig represents the configuration for the search service
type SearchServiceConfig struct {
	// Server configuration
	Server ServerConfig `yaml:"server" json:"server"`

	// Search engine configuration
	Engine EngineConfig `yaml:"engine" json:"engine"`

	// API configuration
	API APIConfig `yaml:"api" json:"api"`

	// Authentication configuration
	Auth AuthConfig `yaml:"auth" json:"auth"`

	// Rate limiting configuration
	RateLimit RateLimitConfig `yaml:"rate_limit" json:"rate_limit"`

	// Metrics configuration
	Metrics MetricsConfig `yaml:"metrics" json:"metrics"`

	// Logging configuration
	Logging LoggingConfig `yaml:"logging" json:"logging"`

	// Health check configuration
	Health HealthConfig `yaml:"health" json:"health"`
}

// ServerConfig represents server-specific configuration
type ServerConfig struct {
	Host           string        `yaml:"host" json:"host"`
	Port           int           `yaml:"port" json:"port"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	MaxHeaderBytes int           `yaml:"max_header_bytes" json:"max_header_bytes"`
}

// EngineConfig represents search engine configuration
type EngineConfig struct {
	MaxResults     int           `yaml:"max_results" json:"max_results"`
	DefaultK       int           `yaml:"default_k" json:"default_k"`
	DistanceType   string        `yaml:"distance_type" json:"distance_type"`
	EnableCache    bool          `yaml:"enable_cache" json:"enable_cache"`
	CacheSize      int           `yaml:"cache_size" json:"cache_size"`
	CacheTTL       time.Duration `yaml:"cache_ttl" json:"cache_ttl"`
	ParallelSearch bool          `yaml:"parallel_search" json:"parallel_search"`
	MaxWorkers     int           `yaml:"max_workers" json:"max_workers"`
}

// APIConfig represents API-specific configuration
type APIConfig struct {
	// HTTP configuration
	HTTP HTTPConfig `yaml:"http" json:"http"`

	// gRPC configuration
	GRPC GRPCConfig `yaml:"grpc" json:"grpc"`

	// Response formatting
	Response ResponseConfig `yaml:"response" json:"response"`
}

// HTTPConfig represents HTTP API configuration
type HTTPConfig struct {
	Enabled           bool       `yaml:"enabled" json:"enabled"`
	BasePath          string     `yaml:"base_path" json:"base_path"`
	CORS              CORSConfig `yaml:"cors" json:"cors"`
	RequestSizeLimit  int64      `yaml:"request_size_limit" json:"request_size_limit"`
	EnableCompression bool       `yaml:"enable_compression" json:"enable_compression"`
	CompressionLevel  int        `yaml:"compression_level" json:"compression_level"`
	TLS               *TLSConfig `yaml:"tls" json:"tls"`
}

// GRPCConfig represents gRPC API configuration
type GRPCConfig struct {
	Enabled              bool            `yaml:"enabled" json:"enabled"`
	MaxMessageSize       int             `yaml:"max_message_size" json:"max_message_size"`
	MaxConcurrentStreams int             `yaml:"max_concurrent_streams" json:"max_concurrent_streams"`
	Keepalive            KeepaliveConfig `yaml:"keepalive" json:"keepalive"`
}

// CORSConfig represents CORS configuration
type CORSConfig struct {
	Enabled          bool     `yaml:"enabled" json:"enabled"`
	AllowedOrigins   []string `yaml:"allowed_origins" json:"allowed_origins"`
	AllowedMethods   []string `yaml:"allowed_methods" json:"allowed_methods"`
	AllowedHeaders   []string `yaml:"allowed_headers" json:"allowed_headers"`
	ExposedHeaders   []string `yaml:"exposed_headers" json:"exposed_headers"`
	MaxAge           int      `yaml:"max_age" json:"max_age"`
	AllowCredentials bool     `yaml:"allow_credentials" json:"allow_credentials"`
}

// KeepaliveConfig represents keepalive configuration
type KeepaliveConfig struct {
	Time                time.Duration `yaml:"time" json:"time"`
	Timeout             time.Duration `yaml:"timeout" json:"timeout"`
	PermitWithoutStream bool          `yaml:"permit_without_stream" json:"permit_without_stream"`
}

// ResponseConfig represents response formatting configuration
type ResponseConfig struct {
	IncludeQueryVector bool `yaml:"include_query_vector" json:"include_query_vector"`
	IncludeMetadata    bool `yaml:"include_metadata" json:"include_metadata"`
	IncludeTimestamps  bool `yaml:"include_timestamps" json:"include_timestamps"`
	PrettyPrint        bool `yaml:"pretty_print" json:"pretty_print"`
}

// AuthConfig represents authentication configuration
type AuthConfig struct {
	Enabled     bool             `yaml:"enabled" json:"enabled"`
	Type        string           `yaml:"type" json:"type"` // "none", "basic", "bearer", "api_key"
	APIKeys     []string         `yaml:"api_keys" json:"api_keys"`
	BasicAuth   BasicAuthConfig  `yaml:"basic_auth" json:"basic_auth"`
	BearerAuth  BearerAuthConfig `yaml:"bearer_auth" json:"bearer_auth"`
	RateLimitBy string           `yaml:"rate_limit_by" json:"rate_limit_by"` // "ip", "user", "api_key"
}

// BasicAuthConfig represents basic authentication configuration
type BasicAuthConfig struct {
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

// BearerAuthConfig represents bearer token authentication configuration
type BearerAuthConfig struct {
	Secret string `yaml:"secret" json:"secret"`
}

// RateLimitConfig represents rate limiting configuration
type RateLimitConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	RequestsPerSecond float64       `yaml:"requests_per_second" json:"requests_per_second"`
	BurstSize         int           `yaml:"burst_size" json:"burst_size"`
	CleanupInterval   time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
	ByIP              bool          `yaml:"by_ip" json:"by_ip"`
	ByUser            bool          `yaml:"by_user" json:"by_user"`
	ByAPIKey          bool          `yaml:"by_api_key" json:"by_api_key"`
}

// MetricsConfig represents metrics configuration
type MetricsConfig struct {
	Enabled   bool   `yaml:"enabled" json:"enabled"`
	Namespace string `yaml:"namespace" json:"namespace"`
	Subsystem string `yaml:"subsystem" json:"subsystem"`
	Path      string `yaml:"path" json:"path"`
}

// LoggingConfig represents logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level" json:"level"`
	Format string `yaml:"format" json:"format"`
	Output string `yaml:"output" json:"output"`
}

// HealthConfig represents health check configuration
type HealthConfig struct {
	Enabled       bool          `yaml:"enabled" json:"enabled"`
	CheckInterval time.Duration `yaml:"check_interval" json:"check_interval"`
	Timeout       time.Duration `yaml:"timeout" json:"timeout"`
}

// TLSConfig represents TLS configuration
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	CertFile   string `yaml:"cert_file" json:"cert_file"`
	KeyFile    string `yaml:"key_file" json:"key_file"`
	ClientAuth bool   `yaml:"client_auth" json:"client_auth"`
}

// DefaultSearchServiceConfig returns the default search service configuration
func DefaultSearchServiceConfig() *SearchServiceConfig {
	return &SearchServiceConfig{
		Server: ServerConfig{
			Host:           "0.0.0.0",
			Port:           8080,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			IdleTimeout:    120 * time.Second,
			MaxHeaderBytes: 1048576, // 1MB
		},
		Engine: EngineConfig{
			MaxResults:     1000,
			DefaultK:       10,
			DistanceType:   "euclidean",
			EnableCache:    true,
			CacheSize:      1000,
			CacheTTL:       5 * time.Minute,
			ParallelSearch: true,
			MaxWorkers:     4,
		},
		API: APIConfig{
			HTTP: HTTPConfig{
				Enabled:           true,
				BasePath:          "/api/v1",
				RequestSizeLimit:  10485760, // 10MB
				EnableCompression: true,
				CompressionLevel:  6,
				CORS: CORSConfig{
					Enabled:        true,
					AllowedOrigins: []string{"*"},
					AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
					AllowedHeaders: []string{"*"},
					MaxAge:         3600,
				},
			},
			GRPC: GRPCConfig{
				Enabled:              true,
				MaxMessageSize:       4194304, // 4MB
				MaxConcurrentStreams: 100,
				Keepalive: KeepaliveConfig{
					Time:                10 * time.Second,
					Timeout:             20 * time.Second,
					PermitWithoutStream: true,
				},
			},
			Response: ResponseConfig{
				IncludeQueryVector: false,
				IncludeMetadata:    true,
				IncludeTimestamps:  true,
				PrettyPrint:        false,
			},
		},
		Auth: AuthConfig{
			Enabled:     false,
			Type:        "none",
			RateLimitBy: "ip",
		},
		RateLimit: RateLimitConfig{
			Enabled:           false,
			RequestsPerSecond: 100.0,
			BurstSize:         200,
			CleanupInterval:   1 * time.Minute,
			ByIP:              true,
		},
		Metrics: MetricsConfig{
			Enabled:   true,
			Namespace: "vxdb",
			Subsystem: "search",
			Path:      "/metrics",
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
		Health: HealthConfig{
			Enabled:       true,
			CheckInterval: 30 * time.Second,
			Timeout:       5 * time.Second,
		},
	}
}

// Validate validates the search service configuration
func (c *SearchServiceConfig) Validate() error {
	// Validate server configuration
	if c.Server.Host == "" {
		return fmt.Errorf("server host is required")
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server port must be between 1 and 65535")
	}
	if c.Server.ReadTimeout <= 0 {
		return fmt.Errorf("server read timeout must be positive")
	}
	if c.Server.WriteTimeout <= 0 {
		return fmt.Errorf("server write timeout must be positive")
	}

	// Validate engine configuration
	if c.Engine.MaxResults <= 0 {
		return fmt.Errorf("engine max results must be positive")
	}
	if c.Engine.DefaultK <= 0 {
		return fmt.Errorf("engine default k must be positive")
	}
	if c.Engine.DistanceType != "euclidean" && c.Engine.DistanceType != "cosine" && c.Engine.DistanceType != "manhattan" {
		return fmt.Errorf("engine distance type must be one of: euclidean, cosine, manhattan")
	}

	// Validate API configuration
	if c.API.HTTP.BasePath == "" {
		return fmt.Errorf("API HTTP base path is required")
	}
	if c.API.GRPC.MaxMessageSize <= 0 {
		return fmt.Errorf("API gRPC max message size must be positive")
	}

	// Validate authentication configuration
	if c.Auth.Enabled {
		if c.Auth.Type != "none" && c.Auth.Type != "basic" && c.Auth.Type != "bearer" && c.Auth.Type != "api_key" {
			return fmt.Errorf("auth type must be one of: none, basic, bearer, api_key")
		}
	}

	// Validate rate limiting configuration
	if c.RateLimit.Enabled {
		if c.RateLimit.RequestsPerSecond <= 0 {
			return fmt.Errorf("rate limit requests per second must be positive")
		}
		if c.RateLimit.BurstSize <= 0 {
			return fmt.Errorf("rate limit burst size must be positive")
		}
	}

	return nil
}

// GetSearchConfig returns the search configuration from the generic config
func GetSearchConfig(cfg interface{}) *SearchServiceConfig {
	// For now, return default config
	// In a real implementation, this would extract search-specific config from the generic config
	return DefaultSearchServiceConfig()
}
