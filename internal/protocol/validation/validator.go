package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"vexdb/internal/config"
	"vexdb/internal/logging"
	"vexdb/internal/metrics"
	"vexdb/internal/protocol/adapter"
)

var (
	ErrValidationFailed     = errors.New("validation failed")
	ErrInvalidRule          = errors.New("invalid validation rule")
	ErrRuleNotFound         = errors.New("validation rule not found")
	ErrRuleAlreadyExists    = errors.New("validation rule already exists")
	ErrValidatorNotRunning  = errors.New("validator not running")
	ErrValidatorAlreadyRunning = errors.New("validator already running")
	ErrInvalidConfig        = errors.New("invalid validation configuration")
)

// ValidationConfig represents the validation configuration
type ValidationConfig struct {
	Enabled           bool                          `yaml:"enabled" json:"enabled"`
	EnableMetrics     bool                          `yaml:"enable_metrics" json:"enable_metrics"`
	EnableLogging     bool                          `yaml:"enable_logging" json:"enable_logging"`
	EnableTracing     bool                          `yaml:"enable_tracing" json:"enable_tracing"`
	StrictMode        bool                          `yaml:"strict_mode" json:"strict_mode"`
	FailFast          bool                          `yaml:"fail_fast" json:"fail_fast"`
	DefaultRules      []adapter.ValidationRule      `yaml:"default_rules" json:"default_rules"`
	CustomRules       []adapter.ValidationRule      `yaml:"custom_rules" json:"custom_rules"`
	RulePriority      string                        `yaml:"rule_priority" json:"rule_priority"` // "first_match" or "all_rules"
	MaxBodySize       int64                         `yaml:"max_body_size" json:"max_body_size"`
	MaxHeaderSize     int64                         `yaml:"max_header_size" json:"max_header_size"`
	MaxQueryParams    int                           `yaml:"max_query_params" json:"max_query_params"`
	AllowedMethods    []string                      `yaml:"allowed_methods" json:"allowed_methods"`
	AllowedProtocols  []adapter.Protocol            `yaml:"allowed_protocols" json:"allowed_protocols"`
	AllowedOrigins    []string                      `yaml:"allowed_origins" json:"allowed_origins"`
	BlockedIPs        []string                      `yaml:"blocked_ips" json:"blocked_ips"`
	AllowedIPs        []string                      `yaml:"allowed_ips" json:"allowed_ips"`
	EnableRateLimit   bool                          `yaml:"enable_rate_limit" json:"enable_rate_limit"`
	RateLimitPerIP    int                           `yaml:"rate_limit_per_ip" json:"rate_limit_per_ip"`
	RateLimitWindow   time.Duration                 `yaml:"rate_limit_window" json:"rate_limit_window"`
	EnableWAF         bool                          `yaml:"enable_waf" json:"enable_waf"`
	WAFRules          []adapter.ValidationRule      `yaml:"waf_rules" json:"waf_rules"`
	EnableSanitization bool                          `yaml:"enable_sanitization" json:"enable_sanitization"`
	SanitizeFields    []string                      `yaml:"sanitize_fields" json:"sanitize_fields"`
}

// DefaultValidationConfig returns the default validation configuration
func DefaultValidationConfig() *ValidationConfig {
	return &ValidationConfig{
		Enabled:          true,
		EnableMetrics:    true,
		EnableLogging:    true,
		EnableTracing:    false,
		StrictMode:       false,
		FailFast:         false,
		DefaultRules:     getDefaultValidationRules(),
		CustomRules:      []adapter.ValidationRule{},
		RulePriority:     "first_match",
		MaxBodySize:      10 * 1024 * 1024, // 10MB
		MaxHeaderSize:    8 * 1024,        // 8KB
		MaxQueryParams:   100,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"},
		AllowedProtocols: []adapter.Protocol{adapter.ProtocolHTTP, adapter.ProtocolHTTPS},
		AllowedOrigins:   []string{"*"},
		BlockedIPs:       []string{},
		AllowedIPs:       []string{},
		EnableRateLimit:  true,
		RateLimitPerIP:   100,
		RateLimitWindow:  time.Minute,
		EnableWAF:        true,
		WAFRules:         getWAFRules(),
		EnableSanitization: true,
		SanitizeFields:   []string{"password", "token", "secret", "key"},
	}
}

// getDefaultValidationRules returns the default validation rules
func getDefaultValidationRules() []adapter.ValidationRule {
	return []adapter.ValidationRule{
		{
			Name:       "required_method",
			Type:       "method",
			Conditions: map[string]interface{}{"required": true},
			Message:    "Method is required",
			Severity:   "error",
			Enabled:    true,
			Priority:   1,
		},
		{
			Name:       "required_path",
			Type:       "path",
			Conditions: map[string]interface{}{"required": true},
			Message:    "Path is required",
			Severity:   "error",
			Enabled:    true,
			Priority:   1,
		},
		{
			Name:       "valid_method",
			Type:       "method",
			Conditions: map[string]interface{}{"allowed": []string{"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"}},
			Message:    "Invalid HTTP method",
			Severity:   "error",
			Enabled:    true,
			Priority:   2,
		},
		{
			Name:       "path_length",
			Type:       "path",
			Conditions: map[string]interface{}{"max_length": 2048},
			Message:    "Path too long",
			Severity:   "error",
			Enabled:    true,
			Priority:   2,
		},
		{
			Name:       "header_size",
			Type:       "headers",
			Conditions: map[string]interface{}{"max_size": 8192},
			Message:    "Headers too large",
			Severity:   "error",
			Enabled:    true,
			Priority:   3,
		},
		{
			Name:       "body_size",
			Type:       "body",
			Conditions: map[string]interface{}{"max_size": 10485760},
			Message:    "Body too large",
			Severity:   "error",
			Enabled:    true,
			Priority:   3,
		},
	}
}

// getWAFRules returns the default WAF rules
func getWAFRules() []adapter.ValidationRule {
	return []adapter.ValidationRule{
		{
			Name:       "sql_injection",
			Type:       "waf",
			Conditions: map[string]interface{}{"pattern": "(?i)(union|select|insert|update|delete|drop|alter|create|exec|execute)"},
			Message:    "Potential SQL injection detected",
			Severity:   "critical",
			Enabled:    true,
			Priority:   10,
		},
		{
			Name:       "xss",
			Type:       "waf",
			Conditions: map[string]interface{}{"pattern": "(?i)(script|javascript|onload|onerror|onclick|onmouseover)"},
			Message:    "Potential XSS attack detected",
			Severity:   "critical",
			Enabled:    true,
			Priority:   10,
		},
		{
			Name:       "path_traversal",
			Type:       "waf",
			Conditions: map[string]interface{}{"pattern": "(?i)(\\.\\.|/etc|/proc|/usr|/var|/bin|/sbin)"},
			Message:    "Potential path traversal attack detected",
			Severity:   "critical",
			Enabled:    true,
			Priority:   10,
		},
		{
			Name:       "command_injection",
			Type:       "waf",
			Conditions: map[string]interface{}{"pattern": "(?i)(;|\\||&|`|\\$|>|<|\\{|\\})"},
			Message:    "Potential command injection detected",
			Severity:   "critical",
			Enabled:    true,
			Priority:   10,
		},
	}
}

// ValidationResult represents the result of a validation
type ValidationResult struct {
	Valid      bool                   `json:"valid"`
	Errors     []ValidationError      `json:"errors"`
	Warnings   []ValidationWarning    `json:"warnings"`
	Sanitized  map[string]interface{} `json:"sanitized"`
	Metadata   map[string]interface{} `json:"metadata"`
	Duration   time.Duration          `json:"duration"`
	Rules      []ValidationRuleResult  `json:"rules"`
}

// ValidationError represents a validation error
type ValidationError struct {
	Rule    string                 `json:"rule"`
	Message string                 `json:"message"`
	Field   string                 `json:"field"`
	Value   interface{}            `json:"value"`
	Details map[string]interface{} `json:"details"`
}

// ValidationWarning represents a validation warning
type ValidationWarning struct {
	Rule    string                 `json:"rule"`
	Message string                 `json:"message"`
	Field   string                 `json:"field"`
	Value   interface{}            `json:"value"`
	Details map[string]interface{} `json:"details"`
}

// ValidationRuleResult represents the result of a validation rule
type ValidationRuleResult struct {
	Rule      adapter.ValidationRule `json:"rule"`
	Passed    bool                   `json:"passed"`
	Error     *ValidationError       `json:"error,omitempty"`
	Warning   *ValidationWarning     `json:"warning,omitempty"`
	Duration  time.Duration          `json:"duration"`
}

// Validator represents a request validator
type Validator struct {
	config     *ValidationConfig
	logger     logging.Logger
	metrics    *metrics.ServiceMetrics
	
	// Validation rules
	rules      []adapter.ValidationRule
	ruleIndex  map[string]int
	mu         sync.RWMutex
	
	// Rate limiting
	rateLimits map[string]*RateLimitEntry
	rateMu     sync.RWMutex
	
	// IP filtering
	blockedIPs map[string]bool
	allowedIPs map[string]bool
	ipMu       sync.RWMutex
	
	// Lifecycle
	started    bool
	stopped    bool
	startTime  time.Time
	
	// Statistics
	stats      *ValidatorStats
}

// RateLimitEntry represents a rate limit entry
type RateLimitEntry struct {
	Count      int       `json:"count"`
	ResetTime  time.Time `json:"reset_time"`
	Window     time.Duration `json:"window"`
}

// ValidatorStats represents validator statistics
type ValidatorStats struct {
	TotalRequests    int64         `json:"total_requests"`
	ValidRequests    int64         `json:"valid_requests"`
	InvalidRequests  int64         `json:"invalid_requests"`
	Warnings         int64         `json:"warnings"`
	TotalErrors      int64         `json:"total_errors"`
	AvgValidationTime time.Duration `json:"avg_validation_time"`
	MaxValidationTime time.Duration `json:"max_validation_time"`
	MinValidationTime time.Duration `json:"min_validation_time"`
	RateLimitHits    int64         `json:"rate_limit_hits"`
	BlockedRequests  int64         `json:"blocked_requests"`
	WAFBlocks        int64         `json:"waf_blocks"`
	StartTime        time.Time     `json:"start_time"`
	Uptime           time.Duration `json:"uptime"`
}

// NewValidator creates a new request validator
func NewValidator(cfg *config.Config, logger logging.Logger, metrics *metrics.ServiceMetrics) (*Validator, error) {
	validatorConfig := DefaultValidationConfig()
	
	if cfg != nil {
		if validationCfg, ok := cfg.Get("validation"); ok {
			if cfgMap, ok := validationCfg.(map[string]interface{}); ok {
				if enabled, ok := cfgMap["enabled"].(bool); ok {
					validatorConfig.Enabled = enabled
				}
				if enableMetrics, ok := cfgMap["enable_metrics"].(bool); ok {
					validatorConfig.EnableMetrics = enableMetrics
				}
				if enableLogging, ok := cfgMap["enable_logging"].(bool); ok {
					validatorConfig.EnableLogging = enableLogging
				}
				if enableTracing, ok := cfgMap["enable_tracing"].(bool); ok {
					validatorConfig.EnableTracing = enableTracing
				}
				if strictMode, ok := cfgMap["strict_mode"].(bool); ok {
					validatorConfig.StrictMode = strictMode
				}
				if failFast, ok := cfgMap["fail_fast"].(bool); ok {
					validatorConfig.FailFast = failFast
				}
				if rulePriority, ok := cfgMap["rule_priority"].(string); ok {
					validatorConfig.RulePriority = rulePriority
				}
				if maxBodySize, ok := cfgMap["max_body_size"].(int64); ok {
					validatorConfig.MaxBodySize = maxBodySize
				}
				if maxHeaderSize, ok := cfgMap["max_header_size"].(int64); ok {
					validatorConfig.MaxHeaderSize = maxHeaderSize
				}
				if maxQueryParams, ok := cfgMap["max_query_params"].(int); ok {
					validatorConfig.MaxQueryParams = maxQueryParams
				}
				if allowedMethods, ok := cfgMap["allowed_methods"].([]interface{}); ok {
					validatorConfig.AllowedMethods = make([]string, len(allowedMethods))
					for i, method := range allowedMethods {
						validatorConfig.AllowedMethods[i] = fmt.Sprintf("%v", method)
					}
				}
				if allowedOrigins, ok := cfgMap["allowed_origins"].([]interface{}); ok {
					validatorConfig.AllowedOrigins = make([]string, len(allowedOrigins))
					for i, origin := range allowedOrigins {
						validatorConfig.AllowedOrigins[i] = fmt.Sprintf("%v", origin)
					}
				}
				if blockedIPs, ok := cfgMap["blocked_ips"].([]interface{}); ok {
					validatorConfig.BlockedIPs = make([]string, len(blockedIPs))
					for i, ip := range blockedIPs {
						validatorConfig.BlockedIPs[i] = fmt.Sprintf("%v", ip)
					}
				}
				if allowedIPs, ok := cfgMap["allowed_ips"].([]interface{}); ok {
					validatorConfig.AllowedIPs = make([]string, len(allowedIPs))
					for i, ip := range allowedIPs {
						validatorConfig.AllowedIPs[i] = fmt.Sprintf("%v", ip)
					}
				}
				if enableRateLimit, ok := cfgMap["enable_rate_limit"].(bool); ok {
					validatorConfig.EnableRateLimit = enableRateLimit
				}
				if rateLimitPerIP, ok := cfgMap["rate_limit_per_ip"].(int); ok {
					validatorConfig.RateLimitPerIP = rateLimitPerIP
				}
				if rateLimitWindow, ok := cfgMap["rate_limit_window"].(string); ok {
					if duration, err := time.ParseDuration(rateLimitWindow); err == nil {
						validatorConfig.RateLimitWindow = duration
					}
				}
				if enableWAF, ok := cfgMap["enable_waf"].(bool); ok {
					validatorConfig.EnableWAF = enableWAF
				}
				if enableSanitization, ok := cfgMap["enable_sanitization"].(bool); ok {
					validatorConfig.EnableSanitization = enableSanitization
				}
				if sanitizeFields, ok := cfgMap["sanitize_fields"].([]interface{}); ok {
					validatorConfig.SanitizeFields = make([]string, len(sanitizeFields))
					for i, field := range sanitizeFields {
						validatorConfig.SanitizeFields[i] = fmt.Sprintf("%v", field)
					}
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateValidationConfig(validatorConfig); err != nil {
		return nil, fmt.Errorf("invalid validation configuration: %w", err)
	}
	
	validator := &Validator{
		config:     validatorConfig,
		logger:     logger,
		metrics:    metrics,
		rules:      make([]adapter.ValidationRule, 0),
		ruleIndex:  make(map[string]int),
		rateLimits: make(map[string]*RateLimitEntry),
		blockedIPs: make(map[string]bool),
		allowedIPs: make(map[string]bool),
		startTime:  time.Now(),
		stats: &ValidatorStats{
			StartTime: time.Now(),
		},
	}
	
	// Initialize rules
	validator.initializeRules()
	
	// Initialize IP filters
	validator.initializeIPFilters()
	
	validator.logger.Info("Created request validator",
		"enabled", validatorConfig.Enabled,
		"strict_mode", validatorConfig.StrictMode,
		"rules_count", len(validator.rules))
	
	return validator, nil
}

// validateValidationConfig validates the validation configuration
func validateValidationConfig(config *ValidationConfig) error {
	if config == nil {
		return errors.New("validation configuration cannot be nil")
	}
	
	if config.MaxBodySize <= 0 {
		return errors.New("max body size must be positive")
	}
	
	if config.MaxHeaderSize <= 0 {
		return errors.New("max header size must be positive")
	}
	
	if config.MaxQueryParams <= 0 {
		return errors.New("max query params must be positive")
	}
	
	if len(config.AllowedMethods) == 0 {
		return errors.New("allowed methods cannot be empty")
	}
	
	if len(config.AllowedProtocols) == 0 {
		return errors.New("allowed protocols cannot be empty")
	}
	
	if config.RateLimitPerIP <= 0 {
		return errors.New("rate limit per IP must be positive")
	}
	
	if config.RateLimitWindow <= 0 {
		return errors.New("rate limit window must be positive")
	}
	
	if config.RulePriority != "first_match" && config.RulePriority != "all_rules" {
		return errors.New("rule priority must be 'first_match' or 'all_rules'")
	}
	
	return nil
}

// initializeRules initializes the validation rules
func (v *Validator) initializeRules() {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	// Add default rules
	for _, rule := range v.config.DefaultRules {
		v.rules = append(v.rules, rule)
		v.ruleIndex[rule.Name] = len(v.rules) - 1
	}
	
	// Add custom rules
	for _, rule := range v.config.CustomRules {
		if _, exists := v.ruleIndex[rule.Name]; !exists {
			v.rules = append(v.rules, rule)
			v.ruleIndex[rule.Name] = len(v.rules) - 1
		}
	}
	
	// Add WAF rules
	if v.config.EnableWAF {
		for _, rule := range v.config.WAFRules {
			wafRule := rule
			wafRule.Name = "waf_" + wafRule.Name
			v.rules = append(v.rules, wafRule)
			v.ruleIndex[wafRule.Name] = len(v.rules) - 1
		}
	}
	
	// Sort rules by priority
	v.sortRulesByPriority()
}

// initializeIPFilters initializes the IP filters
func (v *Validator) initializeIPFilters() {
	v.ipMu.Lock()
	defer v.ipMu.Unlock()
	
	// Initialize blocked IPs
	for _, ip := range v.config.BlockedIPs {
		v.blockedIPs[ip] = true
	}
	
	// Initialize allowed IPs
	for _, ip := range v.config.AllowedIPs {
		v.allowedIPs[ip] = true
	}
}

// sortRulesByPriority sorts rules by priority
func (v *Validator) sortRulesByPriority() {
	// Sort rules by priority (lower number = higher priority)
	for i := 0; i < len(v.rules)-1; i++ {
		for j := i + 1; j < len(v.rules); j++ {
			if v.rules[i].Priority > v.rules[j].Priority {
				v.rules[i], v.rules[j] = v.rules[j], v.rules[i]
				// Update index
				v.ruleIndex[v.rules[i].Name] = i
				v.ruleIndex[v.rules[j].Name] = j
			}
		}
	}
}

// Start starts the validator
func (v *Validator) Start() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	if v.started {
		return ErrValidatorAlreadyRunning
	}
	
	if !v.config.Enabled {
		v.logger.Info("Validator is disabled")
		return nil
	}
	
	v.started = true
	v.stopped = false
	
	v.logger.Info("Started validator")
	
	return nil
}

// Stop stops the validator
func (v *Validator) Stop() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	if v.stopped {
		return nil
	}
	
	if !v.started {
		return ErrValidatorNotRunning
	}
	
	v.stopped = true
	v.started = false
	
	v.logger.Info("Stopped validator")
	
	return nil
}

// IsRunning checks if the validator is running
func (v *Validator) IsRunning() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	return v.started && !v.stopped
}

// Validate validates a request
func (v *Validator) Validate(ctx context.Context, req *adapter.Request) (*ValidationResult, error) {
	if !v.config.Enabled {
		return &ValidationResult{
			Valid:    true,
			Errors:   []ValidationError{},
			Warnings: []ValidationWarning{},
			Sanitized: make(map[string]interface{}),
			Metadata:  make(map[string]interface{}),
			Duration:  0,
			Rules:     []ValidationRuleResult{},
		}, nil
	}
	
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	if !v.started {
		return nil, ErrValidatorNotRunning
	}
	
	startTime := time.Now()
	result := &ValidationResult{
		Valid:     true,
		Errors:    []ValidationError{},
		Warnings:  []ValidationWarning{},
		Sanitized: make(map[string]interface{}),
		Metadata:  make(map[string]interface{}),
		Rules:     make([]ValidationRuleResult, 0),
	}
	
	// Check IP filtering
	if err := v.checkIPFiltering(req.ClientIP); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Rule:    "ip_filter",
			Message: err.Error(),
			Field:   "client_ip",
			Value:   req.ClientIP,
			Details: map[string]interface{}{"type": "blocked_ip"},
		})
		v.stats.BlockedRequests++
		
		if v.config.FailFast {
			result.Duration = time.Since(startTime)
			v.updateStats(result)
			return result, nil
		}
	}
	
	// Check rate limiting
	if v.config.EnableRateLimit {
		if err := v.checkRateLimit(req.ClientIP); err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, ValidationError{
				Rule:    "rate_limit",
				Message: err.Error(),
				Field:   "client_ip",
				Value:   req.ClientIP,
				Details: map[string]interface{}{"type": "rate_limit_exceeded"},
			})
			v.stats.RateLimitHits++
			
			if v.config.FailFast {
				result.Duration = time.Since(startTime)
				v.updateStats(result)
				return result, nil
			}
		}
	}
	
	// Check basic request validation
	if err := v.validateBasicRequest(req); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Rule:    "basic_validation",
			Message: err.Error(),
			Field:   "request",
			Value:   req,
			Details: map[string]interface{}{"type": "basic_validation"},
		})
		
		if v.config.FailFast {
			result.Duration = time.Since(startTime)
			v.updateStats(result)
			return result, nil
		}
	}
	
	// Apply validation rules
	for _, rule := range v.rules {
		if !rule.Enabled {
			continue
		}
		
		ruleResult := v.applyRule(ctx, rule, req)
		result.Rules = append(result.Rules, ruleResult)
		
		if !ruleResult.Passed {
			result.Valid = false
			
			if ruleResult.Error != nil {
				result.Errors = append(result.Errors, *ruleResult.Error)
			}
			
			if ruleResult.Warning != nil {
				result.Warnings = append(result.Warnings, *ruleResult.Warning)
			}
			
			if v.config.FailFast {
				break
			}
			
			if v.config.RulePriority == "first_match" {
				break
			}
		}
	}
	
	// Sanitize request if enabled
	if v.config.EnableSanitization {
		v.sanitizeRequest(req, result)
	}
	
	result.Duration = time.Since(startTime)
	v.updateStats(result)
	
	// Log validation results
	if v.config.EnableLogging {
		v.logValidationResult(req, result)
	}
	
	// Update metrics if enabled
	if v.config.EnableMetrics && v.metrics != nil {
		v.updateValidationMetrics(result)
	}
	
	return result, nil
}

// checkIPFiltering checks IP filtering
func (v *Validator) checkIPFiltering(clientIP string) error {
	v.ipMu.RLock()
	defer v.ipMu.RUnlock()
	
	// Check if IP is blocked
	if v.blockedIPs[clientIP] {
		return fmt.Errorf("IP %s is blocked", clientIP)
	}
	
	// Check if IP is allowed (if allowed IPs are configured)
	if len(v.allowedIPs) > 0 && !v.allowedIPs[clientIP] {
		return fmt.Errorf("IP %s is not allowed", clientIP)
	}
	
	return nil
}

// checkRateLimit checks rate limiting
func (v *Validator) checkRateLimit(clientIP string) error {
	v.rateMu.Lock()
	defer v.rateMu.Unlock()
	
	now := time.Now()
	entry, exists := v.rateLimits[clientIP]
	
	if !exists {
		entry = &RateLimitEntry{
			Count:     1,
			ResetTime: now.Add(v.config.RateLimitWindow),
			Window:    v.config.RateLimitWindow,
		}
		v.rateLimits[clientIP] = entry
		return nil
	}
	
	// Check if window has expired
	if now.After(entry.ResetTime) {
		entry.Count = 1
		entry.ResetTime = now.Add(v.config.RateLimitWindow)
		return nil
	}
	
	// Check if limit exceeded
	if entry.Count >= v.config.RateLimitPerIP {
		return fmt.Errorf("rate limit exceeded for IP %s", clientIP)
	}
	
	entry.Count++
	return nil
}

// validateBasicRequest validates basic request properties
func (v *Validator) validateBasicRequest(req *adapter.Request) error {
	// Check method
	if req.Method == "" {
		return errors.New("method is required")
	}
	
	// Check if method is allowed
	methodAllowed := false
	for _, allowedMethod := range v.config.AllowedMethods {
		if strings.ToUpper(req.Method) == strings.ToUpper(allowedMethod) {
			methodAllowed = true
			break
		}
	}
	
	if !methodAllowed {
		return fmt.Errorf("method %s is not allowed", req.Method)
	}
	
	// Check path
	if req.Path == "" {
		return errors.New("path is required")
	}
	
	// Check path length
	if len(req.Path) > 2048 {
		return errors.New("path too long")
	}
	
	// Check protocol
	protocolAllowed := false
	for _, allowedProtocol := range v.config.AllowedProtocols {
		if req.Protocol == allowedProtocol {
			protocolAllowed = true
			break
		}
	}
	
	if !protocolAllowed {
		return fmt.Errorf("protocol %s is not allowed", req.Protocol)
	}
	
	// Check body size
	if len(req.Body) > int(v.config.MaxBodySize) {
		return fmt.Errorf("body size %d exceeds maximum allowed size %d", len(req.Body), v.config.MaxBodySize)
	}
	
	// Check headers size
	totalHeaderSize := 0
	for key, value := range req.Headers {
		totalHeaderSize += len(key) + len(value)
	}
	if totalHeaderSize > int(v.config.MaxHeaderSize) {
		return fmt.Errorf("headers size %d exceeds maximum allowed size %d", totalHeaderSize, v.config.MaxHeaderSize)
	}
	
	// Check query parameters count
	if len(req.Query) > v.config.MaxQueryParams {
		return fmt.Errorf("query parameters count %d exceeds maximum allowed count %d", len(req.Query), v.config.MaxQueryParams)
	}
	
	return nil
}

// applyRule applies a validation rule
func (v *Validator) applyRule(ctx context.Context, rule adapter.ValidationRule, req *adapter.Request) ValidationRuleResult {
	startTime := time.Now()
	result := ValidationRuleResult{
		Rule:     rule,
		Passed:   true,
		Duration: 0,
	}
	
	defer func() {
		result.Duration = time.Since(startTime)
	}()
	
	switch rule.Type {
	case "method":
		result.Passed = v.validateMethod(req, rule)
	case "path":
		result.Passed = v.validatePath(req, rule)
	case "headers":
		result.Passed = v.validateHeaders(req, rule)
	case "body":
		result.Passed = v.validateBody(req, rule)
	case "query":
		result.Passed = v.validateQuery(req, rule)
	case "waf":
		result.Passed = v.validateWAF(req, rule)
	default:
		result.Passed = false
		result.Error = &ValidationError{
			Rule:    rule.Name,
			Message: fmt.Sprintf("unknown rule type: %s", rule.Type),
			Field:   "rule_type",
			Value:   rule.Type,
			Details: map[string]interface{}{"type": "unknown_rule_type"},
		}
		return result
	}
	
	if !result.Passed {
		if rule.Severity == "error" {
			result.Error = &ValidationError{
				Rule:    rule.Name,
				Message: rule.Message,
				Field:   rule.Type,
				Value:   req,
				Details: rule.Conditions,
			}
		} else {
			result.Warning = &ValidationWarning{
				Rule:    rule.Name,
				Message: rule.Message,
				Field:   rule.Type,
				Value:   req,
				Details: rule.Conditions,
			}
		}
	}
	
	return result
}

// validateMethod validates the request method
func (v *Validator) validateMethod(req *adapter.Request, rule adapter.ValidationRule) bool {
	if required, ok := rule.Conditions["required"].(bool); ok && required {
		if req.Method == "" {
			return false
		}
	}
	
	if allowed, ok := rule.Conditions["allowed"].([]interface{}); ok {
		methodAllowed := false
		for _, allowedMethod := range allowed {
			if strings.ToUpper(req.Method) == strings.ToUpper(fmt.Sprintf("%v", allowedMethod)) {
				methodAllowed = true
				break
			}
		}
		return methodAllowed
	}
	
	return true
}

// validatePath validates the request path
func (v *Validator) validatePath(req *adapter.Request, rule adapter.ValidationRule) bool {
	if required, ok := rule.Conditions["required"].(bool); ok && required {
		if req.Path == "" {
			return false
		}
	}
	
	if maxLength, ok := rule.Conditions["max_length"].(float64); ok {
		if len(req.Path) > int(maxLength) {
			return false
		}
	}
	
	if pattern, ok := rule.Conditions["pattern"].(string); ok {
		if matched, err := regexp.MatchString(pattern, req.Path); err != nil || !matched {
			return false
		}
	}
	
	return true
}

// validateHeaders validates the request headers
func (v *Validator) validateHeaders(req *adapter.Request, rule adapter.ValidationRule) bool {
	if maxSize, ok := rule.Conditions["max_size"].(float64); ok {
		totalSize := 0
		for key, value := range req.Headers {
			totalSize += len(key) + len(value)
		}
		if totalSize > int(maxSize) {
			return false
		}
	}
	
	if required, ok := rule.Conditions["required"].(map[string]interface{}); ok {
		for key := range required {
			if _, exists := req.Headers[key]; !exists {
				return false
			}
		}
	}
	
	return true
}

// validateBody validates the request body
func (v *Validator) validateBody(req *adapter.Request, rule adapter.ValidationRule) bool {
	if maxSize, ok := rule.Conditions["max_size"].(float64); ok {
		if len(req.Body) > int(maxSize) {
			return false
		}
	}
	
	if required, ok := rule.Conditions["required"].(bool); ok && required {
		if len(req.Body) == 0 {
			return false
		}
	}
	
	if pattern, ok := rule.Conditions["pattern"].(string); ok {
		if matched, err := regexp.MatchString(pattern, string(req.Body)); err != nil || !matched {
			return false
		}
	}
	
	return true
}

// validateQuery validates the request query parameters
func (v *Validator) validateQuery(req *adapter.Request, rule adapter.ValidationRule) bool {
	if maxCount, ok := rule.Conditions["max_count"].(float64); ok {
		if len(req.Query) > int(maxCount) {
			return false
		}
	}
	
	if required, ok := rule.Conditions["required"].(map[string]interface{}); ok {
		for key := range required {
			if _, exists := req.Query[key]; !exists {
				return false
			}
		}
	}
	
	return true
}

// validateWAF validates the request against WAF rules
func (v *Validator) validateWAF(req *adapter.Request, rule adapter.ValidationRule) bool {
	pattern, ok := rule.Conditions["pattern"].(string)
	if !ok {
		return true
	}
	
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return true // Don't fail on invalid regex
	}
	
	// Check method
	if regex.MatchString(req.Method) {
		return false
	}
	
	// Check path
	if regex.MatchString(req.Path) {
		return false
	}
	
	// Check headers
	for key, value := range req.Headers {
		if regex.MatchString(key) || regex.MatchString(value) {
			return false
		}
	}
	
	// Check query parameters
	for key, value := range req.Query {
		if regex.MatchString(key) || regex.MatchString(value) {
			return false
		}
	}
	
	// Check body
	if len(req.Body) > 0 && regex.Match(string(req.Body)) {
		return false
	}
	
	return true
}

// sanitizeRequest sanitizes the request
func (v *Validator) sanitizeRequest(req *adapter.Request, result *ValidationResult) {
	// Sanitize headers
	for key, value := range req.Headers {
		for _, field := range v.config.SanitizeFields {
			if strings.Contains(strings.ToLower(key), strings.ToLower(field)) {
				result.Sanitized["header_"+key] = value
				req.Headers[key] = "***REDACTED***"
			}
		}
	}
	
	// Sanitize query parameters
	for key, value := range req.Query {
		for _, field := range v.config.SanitizeFields {
			if strings.Contains(strings.ToLower(key), strings.ToLower(field)) {
				result.Sanitized["query_"+key] = value
				req.Query[key] = "***REDACTED***"
			}
		}
	}
	
	// Sanitize body (if it's JSON)
	if len(req.Body) > 0 {
		var bodyMap map[string]interface{}
		if err := json.Unmarshal(req.Body, &bodyMap); err == nil {
			sanitizedBody := v.sanitizeMap(bodyMap, v.config.SanitizeFields, result.Sanitized)
			if sanitizedBytes, err := json.Marshal(sanitizedBody); err == nil {
				result.Sanitized["body"] = req.Body
				req.Body = sanitizedBytes
			}
		}
	}
}

// sanitizeMap sanitizes a map
func (v *Validator) sanitizeMap(m map[string]interface{}, fields []string, sanitized map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	
	for key, value := range m {
		shouldSanitize := false
		for _, field := range fields {
			if strings.Contains(strings.ToLower(key), strings.ToLower(field)) {
				shouldSanitize = true
				break
			}
		}
		
		if shouldSanitize {
			sanitized[key] = value
			result[key] = "***REDACTED***"
		} else {
			switch val := value.(type) {
			case map[string]interface{}:
				result[key] = v.sanitizeMap(val, fields, sanitized)
			default:
				result[key] = value
			}
		}
	}
	
	return result
}

// updateStats updates validator statistics
func (v *Validator) updateStats(result *ValidationResult) {
	v.stats.TotalRequests++
	
	if result.Valid {
		v.stats.ValidRequests++
	} else {
		v.stats.InvalidRequests++
	}
	
	v.stats.Warnings += int64(len(result.Warnings))
	v.stats.TotalErrors += int64(len(result.Errors))
	
	// Update validation time stats
	if v.stats.MaxValidationTime == 0 || result.Duration > v.stats.MaxValidationTime {
		v.stats.MaxValidationTime = result.Duration
	}
	
	if v.stats.MinValidationTime == 0 || result.Duration < v.stats.MinValidationTime {
		v.stats.MinValidationTime = result.Duration
	}
	
	v.stats.AvgValidationTime = time.Duration(
		(int64(v.stats.AvgValidationTime)*(v.stats.TotalRequests-1) + int64(result.Duration)) /
		v.stats.TotalRequests,
	)
}

// logValidationResult logs the validation result
func (v *Validator) logValidationResult(req *adapter.Request, result *ValidationResult) {
	if result.Valid {
		v.logger.Debug("Request validation passed",
			"request_id", req.ID,
			"method", req.Method,
			"path", req.Path,
			"duration", result.Duration)
	} else {
		v.logger.Warn("Request validation failed",
			"request_id", req.ID,
			"method", req.Method,
			"path", req.Path,
			"errors", len(result.Errors),
			"warnings", len(result.Warnings),
			"duration", result.Duration)
	}
}

// updateValidationMetrics updates validation metrics
func (v *Validator) updateValidationMetrics(result *ValidationResult) {
	if v.metrics == nil {
		return
	}
	
	if result.Valid {
		v.metrics.ServiceCounter.WithLabelValues("validation", "valid").Inc()
	} else {
		v.metrics.ServiceCounter.WithLabelValues("validation", "invalid").Inc()
	}
	
	v.metrics.ServiceCounter.WithLabelValues("validation", "errors").Add(float64(len(result.Errors)))
	v.metrics.ServiceCounter.WithLabelValues("validation", "warnings").Add(float64(len(result.Warnings)))
	v.metrics.ServiceCounter.WithLabelValues("validation", "rate_limit_hits").Add(float64(v.stats.RateLimitHits))
	v.metrics.ServiceCounter.WithLabelValues("validation", "blocked_requests").Add(float64(v.stats.BlockedRequests))
	v.metrics.ServiceCounter.WithLabelValues("validation", "waf_blocks").Add(float64(v.stats.WAFBlocks))
	
	v.metrics.ServiceGauge.WithLabelValues("validation", "avg_duration").Set(float64(v.stats.AvgValidationTime.Nanoseconds()))
	v.metrics.ServiceGauge.WithLabelValues("validation", "max_duration").Set(float64(v.stats.MaxValidationTime.Nanoseconds()))
}

// GetValidationRules returns all validation rules
func (v *Validator) GetValidationRules() []adapter.ValidationRule {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	rules := make([]adapter.ValidationRule, len(v.rules))
	copy(rules, v.rules)
	return rules
}

// AddRule adds a validation rule
func (v *Validator) AddRule(rule adapter.ValidationRule) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	if _, exists := v.ruleIndex[rule.Name]; exists {
		return ErrRuleAlreadyExists
	}
	
	v.rules = append(v.rules, rule)
	v.ruleIndex[rule.Name] = len(v.rules) - 1
	
	// Re-sort rules by priority
	v.sortRulesByPriority()
	
	if v.config.EnableLogging {
		v.logger.Info("Added validation rule", "rule_name", rule.Name, "rule_type", rule.Type)
	}
	
	return nil
}

// RemoveRule removes a validation rule
func (v *Validator) RemoveRule(name string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	index, exists := v.ruleIndex[name]
	if !exists {
		return ErrRuleNotFound
	}
	
	// Remove rule
	v.rules = append(v.rules[:index], v.rules[index+1:]...)
	
	// Update index
	delete(v.ruleIndex, name)
	for i := index; i < len(v.rules); i++ {
		v.ruleIndex[v.rules[i].Name] = i
	}
	
	if v.config.EnableLogging {
		v.logger.Info("Removed validation rule", "rule_name", name)
	}
	
	return nil
}

// UpdateRule updates a validation rule
func (v *Validator) UpdateRule(rule adapter.ValidationRule) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	index, exists := v.ruleIndex[rule.Name]
	if !exists {
		return ErrRuleNotFound
	}
	
	v.rules[index] = rule
	
	// Re-sort rules by priority
	v.sortRulesByPriority()
	
	if v.config.EnableLogging {
		v.logger.Info("Updated validation rule", "rule_name", rule.Name)
	}
	
	return nil
}

// GetStats returns validator statistics
func (v *Validator) GetStats() *ValidatorStats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	// Update uptime
	v.stats.Uptime = time.Since(v.stats.StartTime)
	
	// Return a copy of stats
	stats := *v.stats
	return &stats
}

// ResetStats resets validator statistics
func (v *Validator) ResetStats() {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	v.stats = &ValidatorStats{
		StartTime: time.Now(),
	}
	
	v.logger.Info("Validator statistics reset")
}

// GetConfig returns the validator configuration
func (v *Validator) GetConfig() *ValidationConfig {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	// Return a copy of config
	config := *v.config
	return &config
}

// UpdateConfig updates the validator configuration
func (v *Validator) UpdateConfig(config *ValidationConfig) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	
	// Validate new configuration
	if err := validateValidationConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	v.config = config
	
	// Re-initialize rules and IP filters
	v.initializeRules()
	v.initializeIPFilters()
	
	v.logger.Info("Updated validator configuration", "config", config)
	
	return nil
}

// Validate validates the validator state
func (v *Validator) Validate() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	// Validate configuration
	if err := validateValidationConfig(v.config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	
	// Validate rules
	for _, rule := range v.rules {
		if rule.Name == "" {
			return fmt.Errorf("rule name cannot be empty")
		}
		if rule.Type == "" {
			return fmt.Errorf("rule type cannot be empty for rule %s", rule.Name)
		}
		if rule.Conditions == nil {
			return fmt.Errorf("rule conditions cannot be nil for rule %s", rule.Name)
		}
	}
	
	return nil
}