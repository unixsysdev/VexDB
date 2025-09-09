package search

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vexdb/vexdb/internal/config"
	"github.com/vexdb/vexdb/internal/logging"
	"github.com/vexdb/vexdb/internal/metrics"
	"github.com/vexdb/vexdb/internal/types"
)

var (
	ErrInvalidFilter      = errors.New("invalid filter")
	ErrInvalidOperator    = errors.New("invalid operator")
	ErrInvalidValue       = errors.New("invalid value")
	ErrFilterFailed       = errors.New("filter failed")
	ErrNoFilters          = errors.New("no filters specified")
	ErrUnsupportedType    = errors.New("unsupported type")
	ErrRegexCompilation   = errors.New("regex compilation failed")
	ErrLogicalOperator    = errors.New("invalid logical operator")
)

// FilterOperator represents a filter operator
type FilterOperator string

const (
	OperatorEqual        FilterOperator = "eq"
	OperatorNotEqual     FilterOperator = "ne"
	OperatorGreaterThan  FilterOperator = "gt"
	OperatorGreaterEqual FilterOperator = "ge"
	OperatorLessThan     FilterOperator = "lt"
	OperatorLessEqual    FilterOperator = "le"
	OperatorIn           FilterOperator = "in"
	OperatorNotIn        FilterOperator = "nin"
	OperatorContains     FilterOperator = "contains"
	OperatorNotContains  FilterOperator = "ncontains"
	OperatorStartsWith   FilterOperator = "startswith"
	OperatorEndsWith     FilterOperator = "endswith"
	OperatorRegex        FilterOperator = "regex"
	OperatorExists       FilterOperator = "exists"
	OperatorNotExists    FilterOperator = "nexists"
)

// LogicalOperator represents a logical operator
type LogicalOperator string

const (
	LogicalAnd LogicalOperator = "AND"
	LogicalOr  LogicalOperator = "OR"
	LogicalNot LogicalOperator = "NOT"
)

// FilterCondition represents a single filter condition
type FilterCondition struct {
	Field    string         `json:"field"`
	Operator FilterOperator `json:"operator"`
	Value    interface{}    `json:"value"`
}

// FilterGroup represents a group of filter conditions
type FilterGroup struct {
	Conditions    []*FilterCondition `json:"conditions"`
	Groups        []*FilterGroup     `json:"groups"`
	LogicalOperator LogicalOperator  `json:"logical_operator"`
}

// MetadataFilter represents a metadata filter
type MetadataFilter struct {
	Root *FilterGroup `json:"root"`
}

// FilterConfig represents the filter configuration
type FilterConfig struct {
	EnableRegex       bool           `yaml:"enable_regex" json:"enable_regex"`
	MaxRegexLength    int            `yaml:"max_regex_length" json:"max_regex_length"`
	EnableValidation  bool           `yaml:"enable_validation" json:"enable_validation"`
	MaxConditions     int            `yaml:"max_conditions" json:"max_conditions"`
	MaxDepth          int            `yaml:"max_depth" json:"max_depth"`
	EnableCaching     bool           `yaml:"enable_caching" json:"enable_caching"`
	CacheSize         int            `yaml:"cache_size" json:"cache_size"`
	CacheTTL          time.Duration  `yaml:"cache_ttl" json:"cache_ttl"`
}

// DefaultFilterConfig returns the default filter configuration
func DefaultFilterConfig() *FilterConfig {
	return &FilterConfig{
		EnableRegex:      true,
		MaxRegexLength:   1000,
		EnableValidation: true,
		MaxConditions:    100,
		MaxDepth:         10,
		EnableCaching:    true,
		CacheSize:        1000,
		CacheTTL:         5 * time.Minute,
	}
}

// FilterStats represents filter statistics
type FilterStats struct {
	TotalFilters      int64     `json:"total_filters"`
	ConditionsApplied int64     `json:"conditions_applied"`
	VectorsFiltered   int64     `json:"vectors_filtered"`
	VectorsPassed     int64     `json:"vectors_passed"`
	CacheHits         int64     `json:"cache_hits"`
	CacheMisses       int64     `json:"cache_misses"`
	AverageLatency    float64   `json:"average_latency"`
	LastFilterAt      time.Time `json:"last_filter_at"`
	FailureCount      int64     `json:"failure_count"`
}

// Filter represents a metadata filter
type Filter struct {
	config *FilterConfig
	cache  map[string]bool
	mu     sync.RWMutex
	logger logging.Logger
	metrics *metrics.StorageMetrics
}

// NewFilter creates a new metadata filter
func NewFilter(cfg *config.Config, logger logging.Logger, metrics *metrics.StorageMetrics) (*Filter, error) {
	filterConfig := DefaultFilterConfig()
	
	if cfg != nil {
		if filterCfg, ok := cfg.Get("filter"); ok {
			if cfgMap, ok := filterCfg.(map[string]interface{}); ok {
				if enableRegex, ok := cfgMap["enable_regex"].(bool); ok {
					filterConfig.EnableRegex = enableRegex
				}
				if maxRegexLength, ok := cfgMap["max_regex_length"].(int); ok {
					filterConfig.MaxRegexLength = maxRegexLength
				}
				if enableValidation, ok := cfgMap["enable_validation"].(bool); ok {
					filterConfig.EnableValidation = enableValidation
				}
				if maxConditions, ok := cfgMap["max_conditions"].(int); ok {
					filterConfig.MaxConditions = maxConditions
				}
				if maxDepth, ok := cfgMap["max_depth"].(int); ok {
					filterConfig.MaxDepth = maxDepth
				}
				if enableCaching, ok := cfgMap["enable_caching"].(bool); ok {
					filterConfig.EnableCaching = enableCaching
				}
				if cacheSize, ok := cfgMap["cache_size"].(int); ok {
					filterConfig.CacheSize = cacheSize
				}
				if cacheTTL, ok := cfgMap["cache_ttl"].(string); ok {
					if ttl, err := time.ParseDuration(cacheTTL); err == nil {
						filterConfig.CacheTTL = ttl
					}
				}
			}
		}
	}
	
	// Validate configuration
	if err := validateFilterConfig(filterConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}
	
	filter := &Filter{
		config: filterConfig,
		cache:  make(map[string]bool),
		logger: logger,
		metrics: metrics,
	}
	
	// Pre-allocate cache if enabled
	if filterConfig.EnableCaching && filterConfig.CacheSize > 0 {
		filter.cache = make(map[string]bool, filterConfig.CacheSize)
	}
	
	filter.logger.Info("Created metadata filter",
		"enable_regex", filterConfig.EnableRegex,
		"max_regex_length", filterConfig.MaxRegexLength,
		"enable_validation", filterConfig.EnableValidation,
		"max_conditions", filterConfig.MaxConditions,
		"max_depth", filterConfig.MaxDepth,
		"enable_caching", filterConfig.EnableCaching,
		"cache_size", filterConfig.CacheSize,
		"cache_ttl", filterConfig.CacheTTL)
	
	return filter, nil
}

// validateFilterConfig validates the filter configuration
func validateFilterConfig(cfg *FilterConfig) error {
	if cfg.MaxRegexLength <= 0 {
		return errors.New("max regex length must be positive")
	}
	
	if cfg.MaxConditions <= 0 {
		return errors.New("max conditions must be positive")
	}
	
	if cfg.MaxDepth <= 0 {
		return errors.New("max depth must be positive")
	}
	
	if cfg.CacheSize < 0 {
		return errors.New("cache size must be non-negative")
	}
	
	if cfg.CacheTTL <= 0 {
		return errors.New("cache TTL must be positive")
	}
	
	return nil
}

// FilterVectors filters vectors based on metadata
func (f *Filter) FilterVectors(vectors []*types.Vector, filter *MetadataFilter) ([]*types.Vector, error) {
	if f.config.EnableValidation {
		if err := filter.Validate(); err != nil {
			f.metrics.Errors.Inc("filter", "validation_failed")
			return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
	}
	
	if filter == nil || filter.Root == nil {
		return vectors, nil
	}
	
	start := time.Now()
	
	// Check cache first
	if f.config.EnableCaching {
		cacheKey := f.getCacheKey(filter, vectors)
		if result, exists := f.getFromCache(cacheKey); exists {
			f.metrics.CacheHits.Inc("filter", "cache_hit")
			return f.applyCachedResult(vectors, result), nil
		}
		f.metrics.CacheMisses.Inc("filter", "cache_miss")
	}
	
	// Apply filter
	filtered := make([]*types.Vector, 0, len(vectors))
	for _, vector := range vectors {
		passes, err := f.evaluateFilter(vector.Metadata, filter.Root)
		if err != nil {
			f.logger.Error("Failed to evaluate filter", "vector_id", vector.ID, "error", err)
			continue
		}
		
		if passes {
			filtered = append(filtered, vector)
		}
	}
	
	duration := time.Since(start)
	
	// Update metrics
	f.metrics.FilterOperations.Inc("filter", "filter_vectors")
	f.metrics.FilterLatency.Observe(duration.Seconds())
	f.metrics.VectorsFiltered.Add("filter", "vectors_filtered", int64(len(vectors)))
	f.metrics.VectorsPassed.Add("filter", "vectors_passed", int64(len(filtered)))
	
	// Update cache
	if f.config.EnableCaching {
		cacheKey := f.getCacheKey(filter, vectors)
		f.addToCache(cacheKey, filtered)
	}
	
	return filtered, nil
}

// evaluateFilter evaluates a filter group against metadata
func (f *Filter) evaluateFilter(metadata map[string]interface{}, group *FilterGroup) (bool, error) {
	if group == nil {
		return true, nil
	}
	
	// Evaluate conditions
	var conditionResults []bool
	for _, condition := range group.Conditions {
		result, err := f.evaluateCondition(metadata, condition)
		if err != nil {
			return false, err
		}
		conditionResults = append(conditionResults, result)
	}
	
	// Evaluate sub-groups
	var groupResults []bool
	for _, subGroup := range group.Groups {
		result, err := f.evaluateFilter(metadata, subGroup)
		if err != nil {
			return false, err
		}
		groupResults = append(groupResults, result)
	}
	
	// Combine results based on logical operator
	allResults := append(conditionResults, groupResults...)
	
	switch group.LogicalOperator {
	case LogicalAnd:
		return f.allTrue(allResults), nil
	case LogicalOr:
		return f.anyTrue(allResults), nil
	case LogicalNot:
		return !f.allTrue(allResults), nil
	default:
		return false, fmt.Errorf("%w: %s", ErrLogicalOperator, group.LogicalOperator)
	}
}

// evaluateCondition evaluates a single condition against metadata
func (f *Filter) evaluateCondition(metadata map[string]interface{}, condition *FilterCondition) (bool, error) {
	if metadata == nil {
		return false, nil
	}
	
	value, exists := metadata[condition.Field]
	if !exists {
		// Handle existence operators
		switch condition.Operator {
		case OperatorExists:
			return false, nil
		case OperatorNotExists:
			return true, nil
		default:
			return false, nil
		}
	}
	
	// Handle existence operators
	switch condition.Operator {
	case OperatorExists:
		return true, nil
	case OperatorNotExists:
		return false, nil
	}
	
	// Convert values to comparable types
	leftValue, err := f.convertValue(value)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidValue, err)
	}
	
	rightValue, err := f.convertValue(condition.Value)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidValue, err)
	}
	
	// Apply operator
	switch condition.Operator {
	case OperatorEqual:
		return f.equal(leftValue, rightValue), nil
	case OperatorNotEqual:
		return !f.equal(leftValue, rightValue), nil
	case OperatorGreaterThan:
		return f.greaterThan(leftValue, rightValue), nil
	case OperatorGreaterEqual:
		return f.greaterEqual(leftValue, rightValue), nil
	case OperatorLessThan:
		return f.lessThan(leftValue, rightValue), nil
	case OperatorLessEqual:
		return f.lessEqual(leftValue, rightValue), nil
	case OperatorIn:
		return f.in(leftValue, rightValue), nil
	case OperatorNotIn:
		return !f.in(leftValue, rightValue), nil
	case OperatorContains:
		return f.contains(leftValue, rightValue), nil
	case OperatorNotContains:
		return !f.contains(leftValue, rightValue), nil
	case OperatorStartsWith:
		return f.startsWith(leftValue, rightValue), nil
	case OperatorEndsWith:
		return f.endsWith(leftValue, rightValue), nil
	case OperatorRegex:
		return f.regex(leftValue, rightValue), nil
	default:
		return false, fmt.Errorf("%w: %s", ErrInvalidOperator, condition.Operator)
	}
}

// convertValue converts a value to a standard type
func (f *Filter) convertValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int, int8, int16, int32, int64:
		return toFloat64(v), nil
	case uint, uint8, uint16, uint32, uint64:
		return toFloat64(v), nil
	case float32, float64:
		return toFloat64(v), nil
	case bool:
		return v, nil
	case []interface{}:
		return v, nil
	default:
		// Try to convert to string
		return fmt.Sprintf("%v", v), nil
	}
}

// toFloat64 converts numeric types to float64
func toFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0.0
	}
}

// equal checks if two values are equal
func (f *Filter) equal(left, right interface{}) bool {
	return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
}

// greaterThan checks if left > right
func (f *Filter) greaterThan(left, right interface{}) bool {
	leftFloat, leftOk := left.(float64)
	rightFloat, rightOk := right.(float64)
	
	if leftOk && rightOk {
		return leftFloat > rightFloat
	}
	
	return fmt.Sprintf("%v", left) > fmt.Sprintf("%v", right)
}

// greaterEqual checks if left >= right
func (f *Filter) greaterEqual(left, right interface{}) bool {
	return f.greaterThan(left, right) || f.equal(left, right)
}

// lessThan checks if left < right
func (f *Filter) lessThan(left, right interface{}) bool {
	return !f.greaterEqual(left, right)
}

// lessEqual checks if left <= right
func (f *Filter) lessEqual(left, right interface{}) bool {
	return !f.greaterThan(left, right)
}

// in checks if left is in right (right should be a slice)
func (f *Filter) in(left, right interface{}) bool {
	rightSlice, ok := right.([]interface{})
	if !ok {
		return false
	}
	
	for _, item := range rightSlice {
		if f.equal(left, item) {
			return true
		}
	}
	
	return false
}

// contains checks if left contains right (for strings)
func (f *Filter) contains(left, right interface{}) bool {
	leftStr, leftOk := left.(string)
	rightStr, rightOk := right.(string)
	
	if leftOk && rightOk {
		return strings.Contains(leftStr, rightStr)
	}
	
	return false
}

// startsWith checks if left starts with right (for strings)
func (f *Filter) startsWith(left, right interface{}) bool {
	leftStr, leftOk := left.(string)
	rightStr, rightOk := right.(string)
	
	if leftOk && rightOk {
		return strings.HasPrefix(leftStr, rightStr)
	}
	
	return false
}

// endsWith checks if left ends with right (for strings)
func (f *Filter) endsWith(left, right interface{}) bool {
	leftStr, leftOk := left.(string)
	rightStr, rightOk := right.(string)
	
	if leftOk && rightOk {
		return strings.HasSuffix(leftStr, rightStr)
	}
	
	return false
}

// regex checks if left matches right regex pattern
func (f *Filter) regex(left, right interface{}) bool {
	if !f.config.EnableRegex {
		return false
	}
	
	leftStr, leftOk := left.(string)
	rightStr, rightOk := right.(string)
	
	if !leftOk || !rightOk {
		return false
	}
	
	if len(rightStr) > f.config.MaxRegexLength {
		return false
	}
	
	pattern, err := regexp.Compile(rightStr)
	if err != nil {
		f.logger.Error("Failed to compile regex", "pattern", rightStr, "error", err)
		return false
	}
	
	return pattern.MatchString(leftStr)
}

// allTrue checks if all values in a slice are true
func (f *Filter) allTrue(values []bool) bool {
	for _, v := range values {
		if !v {
			return false
		}
	}
	return true
}

// anyTrue checks if any value in a slice is true
func (f *Filter) anyTrue(values []bool) bool {
	for _, v := range values {
		if v {
			return true
		}
	}
	return false
}

// getCacheKey generates a cache key for a filter
func (f *Filter) getCacheKey(filter *MetadataFilter, vectors []*types.Vector) string {
	// Simple cache key generation
	// In a real implementation, you would use a more sophisticated approach
	key := fmt.Sprintf("%v_%d", filter, len(vectors))
	return key
}

// getFromCache retrieves results from cache
func (f *Filter) getFromCache(key string) (bool, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	result, exists := f.cache[key]
	return result, exists
}

// addToCache adds results to cache
func (f *Filter) addToCache(key string, vectors []*types.Vector) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	// Simple cache management - evict oldest if cache is full
	if len(f.cache) >= f.config.CacheSize {
		// Evict a random entry (simple approach)
		for k := range f.cache {
			delete(f.cache, k)
			break
		}
	}
	
	f.cache[key] = len(vectors) > 0
}

// applyCachedResult applies cached result to vectors
func (f *Filter) applyCachedResult(vectors []*types.Vector, result bool) []*types.Vector {
	if result {
		return vectors
	}
	return []*types.Vector{}
}

// GetStats returns filter statistics
func (f *Filter) GetStats() *FilterStats {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	return &FilterStats{
		TotalFilters:      f.metrics.FilterOperations.Get("filter", "filter_vectors"),
		ConditionsApplied: f.metrics.ConditionsApplied.Get("filter", "conditions_applied"),
		VectorsFiltered:   f.metrics.VectorsFiltered.Get("filter", "vectors_filtered"),
		VectorsPassed:     f.metrics.VectorsPassed.Get("filter", "vectors_passed"),
		CacheHits:         f.metrics.CacheHits.Get("filter", "cache_hit"),
		CacheMisses:       f.metrics.CacheMisses.Get("filter", "cache_miss"),
		AverageLatency:    f.metrics.FilterLatency.Get("filter", "filter_vectors"),
		FailureCount:      f.metrics.Errors.Get("filter", "filter_failed"),
	}
}

// GetConfig returns the filter configuration
func (f *Filter) GetConfig() *FilterConfig {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	// Return a copy of config
	config := *f.config
	return &config
}

// UpdateConfig updates the filter configuration
func (f *Filter) UpdateConfig(config *FilterConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	// Validate new configuration
	if err := validateFilterConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}
	
	// Clear cache if configuration changed significantly
	if config.EnableCaching != f.config.EnableCaching ||
		config.CacheSize != f.config.CacheSize {
		f.cache = make(map[string]bool)
		if config.EnableCaching && config.CacheSize > 0 {
			f.cache = make(map[string]bool, config.CacheSize)
		}
	}
	
	f.config = config
	
	f.logger.Info("Updated filter configuration", "config", config)
	
	return nil
}

// ClearCache clears the filter cache
func (f *Filter) ClearCache() {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	f.cache = make(map[string]bool)
	if f.config.EnableCaching && f.config.CacheSize > 0 {
		f.cache = make(map[string]bool, f.config.CacheSize)
	}
	
	f.logger.Info("Cleared filter cache")
}

// GetCacheInfo returns cache information
func (f *Filter) GetCacheInfo() map[string]interface{} {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	info := make(map[string]interface{})
	info["size"] = len(f.cache)
	info["max_size"] = f.config.CacheSize
	info["enabled"] = f.config.EnableCaching
	info["usage"] = float64(len(f.cache)) / float64(f.config.CacheSize) * 100
	
	return info
}

// GetSupportedOperators returns a list of supported filter operators
func (f *Filter) GetSupportedOperators() []FilterOperator {
	return []FilterOperator{
		OperatorEqual,
		OperatorNotEqual,
		OperatorGreaterThan,
		OperatorGreaterEqual,
		OperatorLessThan,
		OperatorLessEqual,
		OperatorIn,
		OperatorNotIn,
		OperatorContains,
		OperatorNotContains,
		OperatorStartsWith,
		OperatorEndsWith,
		OperatorRegex,
		OperatorExists,
		OperatorNotExists,
	}
}

// GetOperatorInfo returns information about a filter operator
func (f *Filter) GetOperatorInfo(operator FilterOperator) map[string]interface{} {
	info := make(map[string]interface{})
	
	switch operator {
	case OperatorEqual:
		info["name"] = "Equal"
		info["description"] = "Checks if values are equal"
		info["types"] = []string{"string", "number", "boolean"}
		info["example"] = `{"field": "name", "operator": "eq", "value": "John"}`
		
	case OperatorNotEqual:
		info["name"] = "Not Equal"
		info["description"] = "Checks if values are not equal"
		info["types"] = []string{"string", "number", "boolean"}
		info["example"] = `{"field": "name", "operator": "ne", "value": "John"}`
		
	case OperatorGreaterThan:
		info["name"] = "Greater Than"
		info["description"] = "Checks if left value is greater than right value"
		info["types"] = []string{"number"}
		info["example"] = `{"field": "age", "operator": "gt", "value": 30}`
		
	case OperatorGreaterEqual:
		info["name"] = "Greater or Equal"
		info["description"] = "Checks if left value is greater than or equal to right value"
		info["types"] = []string{"number"}
		info["example"] = `{"field": "age", "operator": "ge", "value": 30}`
		
	case OperatorLessThan:
		info["name"] = "Less Than"
		info["description"] = "Checks if left value is less than right value"
		info["types"] = []string{"number"}
		info["example"] = `{"field": "age", "operator": "lt", "value": 30}`
		
	case OperatorLessEqual:
		info["name"] = "Less or Equal"
		info["description"] = "Checks if left value is less than or equal to right value"
		info["types"] = []string{"number"}
		info["example"] = `{"field": "age", "operator": "le", "value": 30}`
		
	case OperatorIn:
		info["name"] = "In"
		info["description"] = "Checks if value is in a list of values"
		info["types"] = []string{"string", "number"}
		info["example"] = `{"field": "category", "operator": "in", "value": ["tech", "science"]}`
		
	case OperatorNotIn:
		info["name"] = "Not In"
		info["description"] = "Checks if value is not in a list of values"
		info["types"] = []string{"string", "number"}
		info["example"] = `{"field": "category", "operator": "nin", "value": ["tech", "science"]}`
		
	case OperatorContains:
		info["name"] = "Contains"
		info["description"] = "Checks if string contains substring"
		info["types"] = []string{"string"}
		info["example"] = `{"field": "description", "operator": "contains", "value": "important"}`
		
	case OperatorNotContains:
		info["name"] = "Not Contains"
		info["description"] = "Checks if string does not contain substring"
		info["types"] = []string{"string"}
		info["example"] = `{"field": "description", "operator": "ncontains", "value": "important"}`
		
	case OperatorStartsWith:
		info["name"] = "Starts With"
		info["description"] = "Checks if string starts with substring"
		info["types"] = []string{"string"}
		info["example"] = `{"field": "name", "operator": "startswith", "value": "John"}`
		
	case OperatorEndsWith:
		info["name"] = "Ends With"
		info["description"] = "Checks if string ends with substring"
		info["types"] = []string{"string"}
		info["example"] = `{"field": "name", "operator": "endswith", "value": "Doe"}`
		
	case OperatorRegex:
		info["name"] = "Regular Expression"
		info["description"] = "Checks if string matches regex pattern"
		info["types"] = []string{"string"}
		info["example"] = `{"field": "email", "operator": "regex", "value": "^[^@]+@[^@]+\\.[^@]+$"}`
		
	case OperatorExists:
		info["name"] = "Exists"
		info["description"] = "Checks if field exists in metadata"
		info["types"] = []string{"any"}
		info["example"] = `{"field": "email", "operator": "exists"}`
		
	case OperatorNotExists:
		info["name"] = "Not Exists"
		info["description"] = "Checks if field does not exist in metadata"
		info["types"] = []string{"any"}
		info["example"] = `{"field": "email", "operator": "nexists"}`
		
	default:
		info["name"] = "Unknown"
		info["description"] = "Unknown operator"
		info["types"] = []string{}
		info["example"] = ""
	}
	
	return info
}

// Validate validates the metadata filter
func (f *MetadataFilter) Validate() error {
	if f == nil || f.Root == nil {
		return ErrNoFilters
	}
	
	return f.validateGroup(f.Root, 0)
}

// validateGroup validates a filter group
func (f *MetadataFilter) validateGroup(group *FilterGroup, depth int) error {
	if depth > 10 { // Max depth
		return fmt.Errorf("%w: maximum depth exceeded", ErrInvalidFilter)
	}
	
	if group.LogicalOperator != LogicalAnd &&
		group.LogicalOperator != LogicalOr &&
		group.LogicalOperator != LogicalNot {
		return fmt.Errorf("%w: %s", ErrLogicalOperator, group.LogicalOperator)
	}
	
	for _, condition := range group.Conditions {
		if err := f.validateCondition(condition); err != nil {
			return err
		}
	}
	
	for _, subGroup := range group.Groups {
		if err := f.validateGroup(subGroup, depth+1); err != nil {
			return err
		}
	}
	
	return nil
}

// validateCondition validates a filter condition
func (f *MetadataFilter) validateCondition(condition *FilterCondition) error {
	if condition.Field == "" {
		return fmt.Errorf("%w: field is required", ErrInvalidFilter)
	}
	
	if condition.Operator == "" {
		return fmt.Errorf("%w: operator is required", ErrInvalidFilter)
	}
	
	// Check if operator is supported
	supportedOperators := []FilterOperator{
		OperatorEqual, OperatorNotEqual, OperatorGreaterThan, OperatorGreaterEqual,
		OperatorLessThan, OperatorLessEqual, OperatorIn, OperatorNotIn,
		OperatorContains, OperatorNotContains, OperatorStartsWith, OperatorEndsWith,
		OperatorRegex, OperatorExists, OperatorNotExists,
	}
	
	isSupported := false
	for _, op := range supportedOperators {
		if condition.Operator == op {
			isSupported = true
			break
		}
	}
	
	if !isSupported {
		return fmt.Errorf("%w: %s", ErrInvalidOperator, condition.Operator)
	}
	
	// Check if value is required
	if condition.Operator != OperatorExists && condition.Operator != OperatorNotExists {
		if condition.Value == nil {
			return fmt.Errorf("%w: value is required for operator %s", ErrInvalidValue, condition.Operator)
		}
	}
	
	return nil
}