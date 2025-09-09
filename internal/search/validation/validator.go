package validation

import (
    "fmt"
    "math"
    "regexp"
    "strings"

    "vexdb/internal/types"
)

// SearchRequestValidator validates search requests
type SearchRequestValidator struct {
	maxVectorDim   int
	maxK           int
	maxResults     int
	maxMetadataFilters int
}

// NewSearchRequestValidator creates a new search request validator
func NewSearchRequestValidator(maxVectorDim, maxK, maxResults, maxMetadataFilters int) *SearchRequestValidator {
	return &SearchRequestValidator{
		maxVectorDim:       maxVectorDim,
		maxK:               maxK,
		maxResults:         maxResults,
		maxMetadataFilters: maxMetadataFilters,
	}
}

// ValidateSearchRequest validates a search request
func (v *SearchRequestValidator) ValidateSearchRequest(query *types.Vector, k int, filter *types.MetadataFilter) error {
	// Validate query vector
	if err := v.validateQueryVector(query); err != nil {
		return fmt.Errorf("invalid query vector: %w", err)
	}

	// Validate k parameter
	if err := v.validateK(k); err != nil {
		return fmt.Errorf("invalid k parameter: %w", err)
	}

	// Validate metadata filter if provided
	if filter != nil {
		if err := v.validateMetadataFilter(filter); err != nil {
			return fmt.Errorf("invalid metadata filter: %w", err)
		}
	}

	return nil
}

// validateQueryVector validates the query vector
func (v *SearchRequestValidator) validateQueryVector(query *types.Vector) error {
	if query == nil {
		return fmt.Errorf("query vector cannot be nil")
	}

	if len(query.Data) == 0 {
		return fmt.Errorf("query vector cannot be empty")
	}

	if len(query.Data) > v.maxVectorDim {
		return fmt.Errorf("query vector dimension %d exceeds maximum %d", len(query.Data), v.maxVectorDim)
	}

	// Validate vector data
	for i, value := range query.Data {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("query vector contains invalid value at index %d: %f", i, value)
		}
	}

	return nil
}

// validateK validates the k parameter
func (v *SearchRequestValidator) validateK(k int) error {
	if k <= 0 {
		return fmt.Errorf("k must be positive, got %d", k)
	}

	if k > v.maxK {
		return fmt.Errorf("k %d exceeds maximum %d", k, v.maxK)
	}

	return nil
}

// validateMetadataFilter validates the metadata filter
func (v *SearchRequestValidator) validateMetadataFilter(filter *types.MetadataFilter) error {
    if filter == nil {
        return nil
    }

    // Support both typed and map-based conditions
    if len(filter.Conditions) > v.maxMetadataFilters {
        return fmt.Errorf("metadata filter has %d conditions, maximum is %d", len(filter.Conditions), v.maxMetadataFilters)
    }

    for i, raw := range filter.Conditions {
        if err := v.validateMetadataCondition(raw); err != nil {
            return fmt.Errorf("invalid metadata condition at index %d: %w", i, err)
        }
    }

    return nil
}

// validateMetadataCondition validates a single metadata condition
func (v *SearchRequestValidator) validateMetadataCondition(raw interface{}) error {
    if raw == nil {
        return fmt.Errorf("metadata condition cannot be nil")
    }

    // Normalize into a typed condition
    var key, op string
    var value string
    var values []string

    switch c := raw.(type) {
    case *types.MetadataCondition:
        key = c.Key
        op = c.Operator
        value = c.Value
        values = c.Values
    case map[string]interface{}:
        if k, ok := c["key"].(string); ok { key = k }
        if o, ok := c["operator"].(string); ok { op = o }
        if vStr, ok := c["value"].(string); ok { value = vStr }
        if vs, ok := c["values"].([]string); ok {
            values = vs
        } else if vsi, ok := c["values"].([]interface{}); ok {
            for _, it := range vsi {
                if s, ok := it.(string); ok { values = append(values, s) }
            }
        }
    default:
        return fmt.Errorf("unsupported condition type %T", raw)
    }

    // Validate key
    if strings.TrimSpace(key) == "" {
        return fmt.Errorf("metadata condition key cannot be empty")
    }
    if len(key) > 256 {
        return fmt.Errorf("metadata condition key length %d exceeds maximum 256", len(key))
    }
    keyRegex := regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
    if !keyRegex.MatchString(key) {
        return fmt.Errorf("metadata condition key contains invalid characters")
    }

    // Validate operator
    validOperators := []string{"=", "!=", ">", "<", ">=", "<=", "in", "not_in", "contains", "not_contains"}
    if !contains(validOperators, op) {
        return fmt.Errorf("metadata condition operator %s is not valid", op)
    }

    // Validate value(s)
    switch op {
    case "in", "not_in":
        if len(values) == 0 {
            return fmt.Errorf("metadata condition with operator %s requires values", op)
        }
        for _, val := range values {
            if err := v.validateMetadataValue(val); err != nil {
                return fmt.Errorf("invalid metadata value: %w", err)
            }
        }
    default:
        if value == "" {
            return fmt.Errorf("metadata condition with operator %s requires a value", op)
        }
        if err := v.validateMetadataValue(value); err != nil {
            return fmt.Errorf("invalid metadata value: %w", err)
        }
    }

    return nil
}

// validateMetadataValue validates a metadata value
func (v *SearchRequestValidator) validateMetadataValue(value string) error {
	if len(value) > 1024 {
		return fmt.Errorf("metadata value length %d exceeds maximum 1024", len(value))
	}

	// Check for potential injection attacks
	if strings.Contains(value, "<script>") || strings.Contains(value, "javascript:") {
		return fmt.Errorf("metadata value contains potentially dangerous content")
	}

	return nil
}

// ValidateMultiClusterSearchRequest validates a multi-cluster search request
func (v *SearchRequestValidator) ValidateMultiClusterSearchRequest(query *types.Vector, k int, clusterIDs []string, filter *types.MetadataFilter) error {
	// Validate basic search request
	if err := v.ValidateSearchRequest(query, k, filter); err != nil {
		return err
	}

	// Validate cluster IDs
	if len(clusterIDs) == 0 {
		return fmt.Errorf("cluster IDs cannot be empty for multi-cluster search")
	}

	if len(clusterIDs) > 100 {
		return fmt.Errorf("too many cluster IDs: %d, maximum is 100", len(clusterIDs))
	}

	// Validate each cluster ID
	for i, clusterID := range clusterIDs {
		if strings.TrimSpace(clusterID) == "" {
			return fmt.Errorf("cluster ID at index %d cannot be empty", i)
		}

		if len(clusterID) > 64 {
			return fmt.Errorf("cluster ID at index %d length %d exceeds maximum 64", i, len(clusterID))
		}

		// Validate cluster ID format (alphanumeric, underscore, hyphen)
		clusterIDRegex := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
		if !clusterIDRegex.MatchString(clusterID) {
			return fmt.Errorf("cluster ID at index %d contains invalid characters", i)
		}
	}

	return nil
}

// ValidateBatchSearchRequest validates a batch search request
func (v *SearchRequestValidator) ValidateBatchSearchRequest(queries []*types.Vector, k int, filter *types.MetadataFilter) error {
	if len(queries) == 0 {
		return fmt.Errorf("queries cannot be empty")
	}

	if len(queries) > 100 {
		return fmt.Errorf("too many queries: %d, maximum is 100", len(queries))
	}

	// Validate each query
	for i, query := range queries {
		if err := v.ValidateSearchRequest(query, k, filter); err != nil {
			return fmt.Errorf("invalid query at index %d: %w", i, err)
		}
	}

	return nil
}

// ValidateVectorID validates a vector ID
func (v *SearchRequestValidator) ValidateVectorID(vectorID string) error {
	if strings.TrimSpace(vectorID) == "" {
		return fmt.Errorf("vector ID cannot be empty")
	}

	if len(vectorID) > 128 {
		return fmt.Errorf("vector ID length %d exceeds maximum 128", len(vectorID))
	}

	// Validate vector ID format (alphanumeric, underscore, hyphen, dot)
	vectorIDRegex := regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	if !vectorIDRegex.MatchString(vectorID) {
		return fmt.Errorf("vector ID contains invalid characters")
	}

	return nil
}

// ValidateDistanceThreshold validates a distance threshold
func (v *SearchRequestValidator) ValidateDistanceThreshold(threshold float64) error {
	if threshold < 0 {
		return fmt.Errorf("distance threshold cannot be negative")
	}

	if threshold > 1e6 {
		return fmt.Errorf("distance threshold %f is too large", threshold)
	}

	return nil
}

// contains checks if a string slice contains a specific string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// ValidationError represents a validation error with detailed information
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   interface{} `json:"value,omitempty"`
}

// ValidationErrors represents multiple validation errors
type ValidationErrors struct {
	Errors []ValidationError `json:"errors"`
}

// Error implements the error interface
func (ve *ValidationErrors) Error() string {
	if len(ve.Errors) == 0 {
		return "validation failed"
	}

	var messages []string
	for _, err := range ve.Errors {
		messages = append(messages, fmt.Sprintf("%s: %s", err.Field, err.Message))
	}

	return strings.Join(messages, "; ")
}

// AddError adds a validation error
func (ve *ValidationErrors) AddError(field, message string, value interface{}) {
	ve.Errors = append(ve.Errors, ValidationError{
		Field:   field,
		Message: message,
		Value:   value,
	})
}

// HasErrors returns true if there are validation errors
func (ve *ValidationErrors) HasErrors() bool {
	return len(ve.Errors) > 0
}

// GetErrors returns all validation errors
func (ve *ValidationErrors) GetErrors() []ValidationError {
	return ve.Errors
}
