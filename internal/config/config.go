package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// Config is a generic interface for all configuration types
type Config interface {
	Validate() error
	Get(key string) (interface{}, bool)
	GetMetricsConfig() interface{}
}

// LoadConfig loads configuration based on the service type
func LoadConfig(serviceType, path string) (Config, error) {
	switch serviceType {
	case "insert":
		return LoadInsertConfig(path)
	case "storage":
		return LoadStorageConfig(path)
	case "search":
		return LoadSearchConfig(path)
	default:
		return nil, fmt.Errorf("unknown service type: %s", serviceType)
	}
}

// InsertConfig holds configuration for the vexinsert service
type InsertConfig struct {
	Server   ServerConfig   `yaml:"server"`
	Cluster  ClusterConfig  `yaml:"cluster"`
	Protocols ProtocolConfig `yaml:"protocols"`
}

// StorageConfig holds configuration for the vexstorage service
type StorageConfig struct {
	Server   ServerConfig    `yaml:"server"`
	Storage  StorageEngineConfig `yaml:"storage"`
	Cluster  ClusterConfig   `yaml:"cluster"`
}

// SearchConfig holds configuration for the vexsearch service
type SearchConfig struct {
	Server  ServerConfig  `yaml:"server"`
	Cluster ClusterConfig `yaml:"cluster"`
}

// ServerConfig holds HTTP/gRPC server configuration
type ServerConfig struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	ReadTimeout  int    `yaml:"read_timeout"`
	WriteTimeout int    `yaml:"write_timeout"`
}

// ClusterConfig holds cluster-wide configuration
type ClusterConfig struct {
	NodeID       string   `yaml:"node_id"`
	ClusterNodes []string `yaml:"cluster_nodes"`
	Replication  struct {
		Enabled      bool   `yaml:"enabled"`
		Factor       int    `yaml:"factor"`
		Strategy     string `yaml:"strategy"`
	} `yaml:"replication"`
}

// ProtocolConfig holds protocol adapter configuration
type ProtocolConfig struct {
	HTTP struct {
		Enabled bool   `yaml:"enabled"`
		Port    int    `yaml:"port"`
	} `yaml:"http"`
	WebSocket struct {
		Enabled bool   `yaml:"enabled"`
		Port    int    `yaml:"port"`
	} `yaml:"websocket"`
	GRPC struct {
		Enabled bool   `yaml:"enabled"`
		Port    int    `yaml:"port"`
	} `yaml:"grpc"`
	Redis struct {
		Enabled bool   `yaml:"enabled"`
		Port    int    `yaml:"port"`
	} `yaml:"redis"`
}

// StorageEngineConfig holds storage engine configuration
type StorageEngineConfig struct {
	DataDir           string `yaml:"data_dir"`
	SegmentSize       int    `yaml:"segment_size"`
	BufferSize        int    `yaml:"buffer_size"`
	FlushInterval     int    `yaml:"flush_interval"`
	Compression       string `yaml:"compression"`
}

// LoadInsertConfig loads configuration for vexinsert service
func LoadInsertConfig(path string) (*InsertConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config InsertConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// LoadStorageConfig loads configuration for vexstorage service
func LoadStorageConfig(path string) (*StorageConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config StorageConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// LoadSearchConfig loads configuration for vexsearch service
func LoadSearchConfig(path string) (*SearchConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config SearchConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// Get returns a configuration value by key
func (c *InsertConfig) Get(key string) (interface{}, bool) {
	switch key {
	case "server":
		return c.Server, true
	case "cluster":
		return c.Cluster, true
	case "protocols":
		return c.Protocols, true
	default:
		return nil, false
	}
}

// GetMetricsConfig returns the metrics configuration
func (c *InsertConfig) GetMetricsConfig() interface{} {
	// For now, return nil as metrics config is not defined in InsertConfig
	return nil
}

// Validate validates the configuration and returns any errors
func (c *InsertConfig) Validate() error {
	if c.Server.Host == "" {
		return fmt.Errorf("server host is required")
	}
	if c.Server.Port <= 0 {
		return fmt.Errorf("server port must be positive")
	}
	return nil
}

// Get returns a configuration value by key
func (c *StorageConfig) Get(key string) (interface{}, bool) {
	switch key {
	case "server":
		return c.Server, true
	case "storage":
		return c.Storage, true
	case "cluster":
		return c.Cluster, true
	default:
		return nil, false
	}
}

// GetMetricsConfig returns the metrics configuration
func (c *StorageConfig) GetMetricsConfig() interface{} {
	// For now, return nil as metrics config is not defined in StorageConfig
	return nil
}

// Validate validates the configuration and returns any errors
func (c *StorageConfig) Validate() error {
	if c.Server.Host == "" {
		return fmt.Errorf("server host is required")
	}
	if c.Server.Port <= 0 {
		return fmt.Errorf("server port must be positive")
	}
	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage data directory is required")
	}
	return nil
}

// Get returns a configuration value by key
func (c *SearchConfig) Get(key string) (interface{}, bool) {
	switch key {
	case "server":
		return c.Server, true
	case "cluster":
		return c.Cluster, true
	default:
		return nil, false
	}
}

// GetMetricsConfig returns the metrics configuration
func (c *SearchConfig) GetMetricsConfig() interface{} {
	// For now, return nil as metrics config is not defined in SearchConfig
	return nil
}

// Validate validates the configuration and returns any errors
func (c *SearchConfig) Validate() error {
	if c.Server.Host == "" {
		return fmt.Errorf("server host is required")
	}
	if c.Server.Port <= 0 {
		return fmt.Errorf("server port must be positive")
	}
	return nil
}