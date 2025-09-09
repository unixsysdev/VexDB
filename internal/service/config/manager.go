package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vexdb/vexdb/internal/config"
	"github.com/vexdb/vexdb/internal/logging"
	"github.com/vexdb/vexdb/internal/metrics"
)

var (
	ErrConfigNotLoaded    = errors.New("configuration not loaded")
	ErrConfigReloadFailed = errors.New("configuration reload failed")
	ErrInvalidConfigPath  = errors.New("invalid configuration path")
	ErrConfigWatchFailed  = errors.New("configuration watch failed")
)

// ConfigManager represents a configuration manager
type ConfigManager struct {
	config     *config.Config
	logger     logging.Logger
	metrics    *metrics.ServiceMetrics
	
	// Configuration state
	configPath string
	watcher    *fsnotify.Watcher
	mu         sync.RWMutex
	
	// Reload state
	reloadChan chan struct{}
	reloadDone chan struct{}
	
	// Lifecycle
	started    bool
	stopped    bool
}

// ConfigManagerConfig represents the configuration manager configuration
type ConfigManagerConfig struct {
	Enabled        bool          `yaml:"enabled" json:"enabled"`
	ConfigPath     string        `yaml:"config_path" json:"config_path"`
	WatchInterval  time.Duration `yaml:"watch_interval" json:"watch_interval"`
	ReloadInterval time.Duration `yaml:"reload_interval" json:"reload_interval"`
	EnableWatch    bool          `yaml:"enable_watch" json:"enable_watch"`
	EnableReload   bool          `yaml:"enable_reload" json:"enable_reload"`
	EnableMetrics  bool          `yaml:"enable_metrics" json:"enable_metrics"`
}

// DefaultConfigManagerConfig returns the default configuration manager configuration
func DefaultConfigManagerConfig() *ConfigManagerConfig {
	return &ConfigManagerConfig{
		Enabled:        true,
		ConfigPath:     "config.yaml",
		WatchInterval:  5 * time.Second,
		ReloadInterval: 10 * time.Second,
		EnableWatch:    true,
		EnableReload:   true,
		EnableMetrics:  true,
	}
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(cfg *config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*ConfigManager, error) {
	managerConfig := DefaultConfigManagerConfig()
	
	if cfg != nil {
		if managerCfg, ok := cfg.Get("config_manager"); ok {
			if cfgMap, ok := managerCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					managerConfig.Enabled = enabled
				}
				if configPath, ok := cfgMap["config_path"].(string); ok {
					managerConfig.ConfigPath = configPath
				}
				if watchInterval, ok := cfgMap["watch_interval"].(string); ok {
					if duration, err := time.ParseDuration(watchInterval); err == nil {
						managerConfig.WatchInterval = duration
					}
				}
				if reloadInterval, ok := cfgMap["reload_interval"].(string); ok {
					if duration, err := time.ParseDuration(reloadInterval); err == nil {
						managerConfig.ReloadInterval = duration
					}
				}
				if enableWatch, ok := cfgMap["enable_watch"].(bool); ok {
					managerConfig.EnableWatch = enableWatch
				}
				if enableReload, ok := cfgMap["enable_reload"].(bool); ok {
					managerConfig.EnableReload = enableReload
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					managerConfig.EnableMetrics = enableMetrics
				}
			}
		}
	}
	
	// Validate configuration path
	if err := validateConfigPath(managerConfig.ConfigPath); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfigPath, err)
	}
	
	// Load initial configuration
	configData, err := config.LoadConfig(managerConfig.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load initial configuration: %w", err)
	}
	
	manager := &ConfigManager{
		config:      configData,
		logger:      logger,
		metrics:     metrics,
		configPath:  managerConfig.ConfigPath,
		reloadChan:  make(chan struct{}),
		reloadDone:  make(chan struct{}),
	}
	
	manager.logger.Info("Created configuration manager",
		"enabled", managerConfig.Enabled,
		"config_path", managerConfig.ConfigPath,
		"enable_watch", managerConfig.EnableWatch,
		"enable_reload", managerConfig.EnableReload)
	
	return manager, nil
}

// validateConfigPath validates the configuration path
func validateConfigPath(path string) error {
	if path == "" {
		return errors.New("configuration path cannot be empty")
	}
	
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("configuration file does not exist: %s", path)
	}
	
	// Check if file is readable
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("configuration file is not readable: %s", path)
	}
	file.Close()
	
	// Get absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}
	
	// Check if file has valid extension
	ext := filepath.Ext(absPath)
	if ext != ".yaml" && ext != ".yml" && ext != ".json" {
		return fmt.Errorf("configuration file must have .yaml, .yml, or .json extension")
	}
	
	return nil
}

// Start starts the configuration manager
func (m *ConfigManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.started {
		return nil
	}
	
	if !m.config.Enabled {
		m.logger.Info("Configuration manager is disabled")
		return nil
	}
	
	// Initialize file watcher if enabled
	if m.config.EnableWatch {
		if err := m.initializeWatcher(); err != nil {
			return fmt.Errorf("failed to initialize file watcher: %w", err)
		}
	}
	
	// Start configuration reloader if enabled
	if m.config.EnableReload {
		go m.startConfigReloader()
	}
	
	m.started = true
	m.stopped = false
	
	m.logger.Info("Started configuration manager")
	
	return nil
}

// Stop stops the configuration manager
func (m *ConfigManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.stopped {
		return nil
	}
	
	if !m.started {
		return ErrConfigNotLoaded
	}
	
	// Stop file watcher
	if m.watcher != nil {
		if err := m.watcher.Close(); err != nil {
			m.logger.Error("Failed to close file watcher", "error", err)
		}
		m.watcher = nil
	}
	
	// Stop configuration reloader
	if m.config.EnableReload {
		close(m.reloadDone)
	}
	
	m.stopped = true
	m.started = false
	
	m.logger.Info("Stopped configuration manager")
	
	return nil
}

// IsRunning checks if the configuration manager is running
func (m *ConfigManager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.started && !m.stopped
}

// GetConfig returns the current configuration
func (m *ConfigManager) GetConfig() *config.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.config
}

// Reload reloads the configuration
func (m *ConfigManager) Reload() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.config.EnableReload {
		return errors.New("configuration reload is disabled")
	}
	
	return m.reloadConfig()
}

// Watch watches for configuration changes
func (m *ConfigManager) Watch() (<-chan struct{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if !m.config.EnableWatch {
		return nil, errors.New("configuration watch is disabled")
	}
	
	return m.reloadChan, nil
}

// GetConfigPath returns the configuration path
func (m *ConfigManager) GetConfigPath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.configPath
}

// SetConfigPath sets the configuration path
func (m *ConfigManager) SetConfigPath(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Validate new path
	if err := validateConfigPath(path); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfigPath, err)
	}
	
	m.configPath = path
	
	m.logger.Info("Updated configuration path", "path", path)
	
	return nil
}

// GetConfigManagerConfig returns the configuration manager configuration
func (m *ConfigManager) GetConfigManagerConfig() *ConfigManagerConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Return a copy of config
	config := *m.config
	return &config
}

// UpdateConfigManagerConfig updates the configuration manager configuration
func (m *ConfigManager) UpdateConfigManagerConfig(config *ConfigManagerConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Validate configuration path
	if err := validateConfigPath(config.ConfigPath); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfigPath, err)
	}
	
	m.config = config
	
	m.logger.Info("Updated configuration manager configuration", "config", config)
	
	return nil
}

// Validate validates the configuration manager state
func (m *ConfigManager) Validate() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Validate configuration path
	if err := validateConfigPath(m.configPath); err != nil {
		return fmt.Errorf("invalid configuration path: %w", err)
	}
	
	// Validate configuration
	if m.config == nil {
		return ErrConfigNotLoaded
	}
	
	return nil
}

// initializeWatcher initializes the file watcher
func (m *ConfigManager) initializeWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}
	
	// Add configuration file to watcher
	if err := watcher.Add(m.configPath); err != nil {
		watcher.Close()
		return fmt.Errorf("failed to add configuration file to watcher: %w", err)
	}
	
	m.watcher = watcher
	
	// Start watching for changes
	go m.watchConfigChanges()
	
	return nil
}

// watchConfigChanges watches for configuration changes
func (m *ConfigManager) watchConfigChanges() {
	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}
			
			// Check if the event is for our configuration file
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				m.logger.Info("Configuration file changed", "file", event.Name)
				
				// Trigger reload
				if m.config.EnableReload {
					go func() {
						if err := m.Reload(); err != nil {
							m.logger.Error("Failed to reload configuration", "error", err)
						}
					}()
				}
				
				// Notify watchers
				select {
				case m.reloadChan <- struct{}{}:
				default:
				}
			}
			
		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			
			m.logger.Error("File watcher error", "error", err)
		}
	}
}

// startConfigReloader starts the configuration reloader
func (m *ConfigManager) startConfigReloader() {
	ticker := time.NewTicker(m.config.ReloadInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := m.Reload(); err != nil {
				m.logger.Error("Failed to reload configuration", "error", err)
			}
		case <-m.reloadDone:
			return
		}
	}
}

// reloadConfig reloads the configuration
func (m *ConfigManager) reloadConfig() error {
	start := time.Now()
	
	// Load new configuration
	newConfig, err := config.LoadConfig(m.configPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConfigReloadFailed, err)
	}
	
	// Update configuration
	m.config = newConfig
	
	duration := time.Since(start)
	
	m.logger.Info("Configuration reloaded successfully",
		"duration", duration,
		"config_path", m.configPath)
	
	// Update metrics if enabled
	if m.config.EnableMetrics && m.metrics != nil {
		m.metrics.ServiceErrors.WithLabelValues("config", "reload_success").Inc()
	}
	
	return nil
}

// GetConfigValue returns a configuration value
func (m *ConfigManager) GetConfigValue(key string) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.config == nil {
		return nil, false
	}
	
	return m.config.Get(key)
}

// SetConfigValue sets a configuration value
func (m *ConfigManager) SetConfigValue(key string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.config == nil {
		return ErrConfigNotLoaded
	}
	
	m.config.Set(key, value)
	
	m.logger.Info("Configuration value set", "key", key, "value", value)
	
	return nil
}

// GetConfigSection returns a configuration section
func (m *ConfigManager) GetConfigSection(section string) (map[string]interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.config == nil {
		return nil, false
	}
	
	if sectionData, ok := m.config.Get(section); ok {
		if sectionMap, ok := sectionData.(map[string]interface{}); ok {
			return sectionMap, true
		}
	}
	
	return nil, false
}

// SetConfigSection sets a configuration section
func (m *ConfigManager) SetConfigSection(section string, data map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.config == nil {
		return ErrConfigNotLoaded
	}
	
	m.config.Set(section, data)
	
	m.logger.Info("Configuration section set", "section", section)
	
	return nil
}

// SaveConfig saves the current configuration to file
func (m *ConfigManager) SaveConfig() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.config == nil {
		return ErrConfigNotLoaded
	}
	
	// Save configuration to file
	if err := m.config.Save(m.configPath); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}
	
	m.logger.Info("Configuration saved", "path", m.configPath)
	
	return nil
}

// BackupConfig creates a backup of the current configuration
func (m *ConfigManager) BackupConfig() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.config == nil {
		return "", ErrConfigNotLoaded
	}
	
	// Create backup filename
	timestamp := time.Now().Format("20060102_150405")
	backupPath := fmt.Sprintf("%s.backup.%s", m.configPath, timestamp)
	
	// Save configuration to backup file
	if err := m.config.Save(backupPath); err != nil {
		return "", fmt.Errorf("failed to create backup: %w", err)
	}
	
	m.logger.Info("Configuration backup created", "path", backupPath)
	
	return backupPath, nil
}

// RestoreConfig restores configuration from a backup
func (m *ConfigManager) RestoreConfig(backupPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Load backup configuration
	backupConfig, err := config.LoadConfig(backupPath)
	if err != nil {
		return fmt.Errorf("failed to load backup configuration: %w", err)
	}
	
	// Update configuration
	m.config = backupConfig
	
	// Save to original path
	if err := m.config.Save(m.configPath); err != nil {
		return fmt.Errorf("failed to restore configuration: %w", err)
	}
	
	m.logger.Info("Configuration restored from backup", "backup_path", backupPath)
	
	return nil
}