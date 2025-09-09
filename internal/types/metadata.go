package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Metadata represents structured metadata associated with vectors
type Metadata struct {
	Fields    map[string]interface{} `json:"fields" yaml:"fields"`
	Schema    *MetadataSchema        `json:"schema,omitempty" yaml:"schema,omitempty"`
	Timestamp int64                 `json:"timestamp" yaml:"timestamp"`
	Version   int32                 `json:"version" yaml:"version"`
}

// MetadataSchema defines the schema for metadata validation
type MetadataSchema struct {
	Fields map[string]MetadataField `json:"fields" yaml:"fields"`
	Strict bool                    `json:"strict" yaml:"strict"` // Strict validation mode
}

// MetadataField defines a single field in the metadata schema
type MetadataField struct {
	Type        string      `json:"type" yaml:"type"`        // "string", "int", "float", "bool", "datetime", "array", "object"
	Required    bool        `json:"required" yaml:"required"`
	Default     interface{} `json:"default,omitempty" yaml:"default,omitempty"`
	Min         interface{} `json:"min,omitempty" yaml:"min,omitempty"`
	Max         interface{} `json:"max,omitempty" yaml:"max,omitempty"`
	Enum        []string    `json:"enum,omitempty" yaml:"enum,omitempty"`
	Description string      `json:"description,omitempty" yaml:"description,omitempty"`
}

// MetadataFilter represents a filter for metadata queries
type MetadataFilter struct {
	Field    string      `json:"field" yaml:"field"`
	Operator string      `json:"operator" yaml:"operator"` // "=", "!=", ">", "<", ">=", "<=", "in", "contains", "startswith", "endswith"
	Value    interface{} `json:"value" yaml:"value"`
}

// MetadataQuery represents a complex metadata query
type MetadataQuery struct {
	Filters    []MetadataFilter `json:"filters" yaml:"filters"`
	Logic      string           `json:"logic" yaml:"logic"` // "AND", "OR"
	Limit      int              `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset     int              `json:"offset,omitempty" yaml:"offset,omitempty"`
	SortBy     string           `json:"sort_by,omitempty" yaml:"sort_by,omitempty"`
	SortOrder  string           `json:"sort_order,omitempty" yaml:"sort_order,omitempty"` // "asc", "desc"
}

// NewMetadata creates a new metadata instance
func NewMetadata() *Metadata {
	return &Metadata{
		Fields:    make(map[string]interface{}),
		Timestamp: time.Now().Unix(),
		Version:   1,
	}
}

// NewMetadataWithSchema creates a new metadata instance with schema
func NewMetadataWithSchema(schema *MetadataSchema) *Metadata {
	return &Metadata{
		Fields:    make(map[string]interface{}),
		Schema:    schema,
		Timestamp: time.Now().Unix(),
		Version:   1,
	}
}

// Set sets a metadata field with validation
func (m *Metadata) Set(key string, value interface{}) error {
	if m == nil {
		return errors.New("metadata cannot be nil")
	}
	
	if key == "" {
		return errors.New("metadata key cannot be empty")
	}
	
	// Validate against schema if present
	if m.Schema != nil {
		if err := m.validateField(key, value); err != nil {
			return err
		}
	}
	
	m.Fields[key] = value
	m.Timestamp = time.Now().Unix()
	m.Version++
	
	return nil
}

// Get gets a metadata field
func (m *Metadata) Get(key string) (interface{}, bool) {
	if m == nil || m.Fields == nil {
		return nil, false
	}
	
	value, exists := m.Fields[key]
	return value, exists
}

// GetString gets a string metadata field
func (m *Metadata) GetString(key string) (string, bool) {
	value, exists := m.Get(key)
	if !exists {
		return "", false
	}
	
	if str, ok := value.(string); ok {
		return str, true
	}
	
	return "", false
}

// GetInt gets an integer metadata field
func (m *Metadata) GetInt(key string) (int64, bool) {
	value, exists := m.Get(key)
	if !exists {
		return 0, false
	}
	
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		return 0, false
	}
}

// GetFloat gets a float metadata field
func (m *Metadata) GetFloat(key string) (float64, bool) {
	value, exists := m.Get(key)
	if !exists {
		return 0, false
	}
	
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// GetBool gets a boolean metadata field
func (m *Metadata) GetBool(key string) (bool, bool) {
	value, exists := m.Get(key)
	if !exists {
		return false, false
	}
	
	if b, ok := value.(bool); ok {
		return b, true
	}
	
	return false, false
}

// Delete deletes a metadata field
func (m *Metadata) Delete(key string) {
	if m == nil || m.Fields == nil {
		return
	}
	
	delete(m.Fields, key)
	m.Timestamp = time.Now().Unix()
	m.Version++
}

// validateField validates a field against the schema
func (m *Metadata) validateField(key string, value interface{}) error {
	if m.Schema == nil {
		return nil
	}
	
	field, exists := m.Schema.Fields[key]
	if !exists {
		if m.Schema.Strict {
			return fmt.Errorf("field '%s' is not defined in schema", key)
		}
		return nil
	}
	
	// Type validation
	if err := m.validateType(field.Type, value); err != nil {
		return fmt.Errorf("field '%s' type validation failed: %v", key, err)
	}
	
	// Range validation
	if err := m.validateRange(field, value); err != nil {
		return fmt.Errorf("field '%s' range validation failed: %v", key, err)
	}
	
	// Enum validation
	if err := m.validateEnum(field, value); err != nil {
		return fmt.Errorf("field '%s' enum validation failed: %v", key, err)
	}
	
	return nil
}

// validateType validates the type of a field value
func (m *Metadata) validateType(expectedType string, value interface{}) error {
	if value == nil {
		return nil // Nil values are allowed (will use default if required)
	}
	
	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "int":
		if !m.isInt(value) {
			return fmt.Errorf("expected int, got %T", value)
		}
	case "float":
		if !m.isFloat(value) {
			return fmt.Errorf("expected float, got %T", value)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case "datetime":
		if !m.isDateTime(value) {
			return fmt.Errorf("expected datetime, got %T", value)
		}
	case "array":
		if reflect.TypeOf(value).Kind() != reflect.Slice && reflect.TypeOf(value).Kind() != reflect.Array {
			return fmt.Errorf("expected array, got %T", value)
		}
	case "object":
		if reflect.TypeOf(value).Kind() != reflect.Map && reflect.TypeOf(value).Kind() != reflect.Struct {
			return fmt.Errorf("expected object, got %T", value)
		}
	default:
		return fmt.Errorf("unknown field type: %s", expectedType)
	}
	
	return nil
}

// validateRange validates the range of a field value
func (m *Metadata) validateRange(field MetadataField, value interface{}) error {
	if value == nil {
		return nil
	}
	
	switch field.Type {
	case "string":
		str, ok := value.(string)
		if !ok {
			return nil
		}
		
		if min, ok := field.Min.(int); ok && len(str) < min {
			return fmt.Errorf("string length %d less than minimum %d", len(str), min)
		}
		
		if max, ok := field.Max.(int); ok && len(str) > max {
			return fmt.Errorf("string length %d greater than maximum %d", len(str), max)
		}
		
	case "int", "float":
		num, err := m.toFloat64(value)
		if err != nil {
			return nil
		}
		
		if min, ok := field.Min.(float64); ok && num < min {
			return fmt.Errorf("value %f less than minimum %f", num, min)
		}
		
		if max, ok := field.Max.(float64); ok && num > max {
			return fmt.Errorf("value %f greater than maximum %f", num, max)
		}
	}
	
	return nil
}

// validateEnum validates the enum values of a field
func (m *Metadata) validateEnum(field MetadataField, value interface{}) error {
	if len(field.Enum) == 0 {
		return nil
	}
	
	valueStr := fmt.Sprintf("%v", value)
	for _, enumVal := range field.Enum {
		if enumVal == valueStr {
			return nil
		}
	}
	
	return fmt.Errorf("value '%v' not in enum values: %v", value, field.Enum)
}

// isInt checks if a value is an integer
func (m *Metadata) isInt(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

// isFloat checks if a value is a float
func (m *Metadata) isFloat(value interface{}) bool {
	switch value.(type) {
	case float32, float64:
		return true
	default:
		return m.isInt(value)
	}
}

// isDateTime checks if a value is a datetime
func (m *Metadata) isDateTime(value interface{}) bool {
	switch v := value.(type) {
	case time.Time:
		return true
	case string:
		// Try to parse as RFC3339 datetime
		_, err := time.Parse(time.RFC3339, v)
		return err == nil
	case int64:
		// Assume it's a Unix timestamp
		return true
	default:
		return false
	}
}

// toFloat64 converts a value to float64
func (m *Metadata) toFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// Matches checks if metadata matches a filter
func (m *Metadata) Matches(filter MetadataFilter) (bool, error) {
	if m == nil {
		return false, errors.New("metadata cannot be nil")
	}
	
	value, exists := m.Get(filter.Field)
	if !exists {
		return false, nil
	}
	
	return m.compareValues(value, filter.Operator, filter.Value)
}

// compareValues compares two values using the specified operator
func (m *Metadata) compareValues(value interface{}, operator string, filterValue interface{}) (bool, error) {
	switch operator {
	case "=":
		return reflect.DeepEqual(value, filterValue), nil
	case "!=":
		return !reflect.DeepEqual(value, filterValue), nil
	case ">", ">=", "<", "<=":
		return m.compareNumeric(value, operator, filterValue)
	case "in":
		return m.compareIn(value, filterValue)
	case "contains":
		return m.compareContains(value, filterValue)
	case "startswith":
		return m.compareStartsWith(value, filterValue)
	case "endswith":
		return m.compareEndsWith(value, filterValue)
	default:
		return false, fmt.Errorf("unknown operator: %s", operator)
	}
}

// compareNumeric compares numeric values
func (m *Metadata) compareNumeric(value interface{}, operator string, filterValue interface{}) (bool, error) {
	val1, err := m.toFloat64(value)
	if err != nil {
		return false, err
	}
	
	val2, err := m.toFloat64(filterValue)
	if err != nil {
		return false, err
	}
	
	switch operator {
	case ">":
		return val1 > val2, nil
	case ">=":
		return val1 >= val2, nil
	case "<":
		return val1 < val2, nil
	case "<=":
		return val1 <= val2, nil
	default:
		return false, fmt.Errorf("unknown numeric operator: %s", operator)
	}
}

// compareIn checks if value is in the filter values
func (m *Metadata) compareIn(value interface{}, filterValue interface{}) (bool, error) {
	reflectValue := reflect.ValueOf(filterValue)
	if reflectValue.Kind() != reflect.Slice && reflectValue.Kind() != reflect.Array {
		return false, fmt.Errorf("filter value for 'in' operator must be an array")
	}
	
	for i := 0; i < reflectValue.Len(); i++ {
		if reflect.DeepEqual(value, reflectValue.Index(i).Interface()) {
			return true, nil
		}
	}
	
	return false, nil
}

// compareContains checks if string value contains filter value
func (m *Metadata) compareContains(value interface{}, filterValue interface{}) (bool, error) {
	valStr, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("value for 'contains' operator must be a string")
	}
	
	filterStr, ok := filterValue.(string)
	if !ok {
		return false, fmt.Errorf("filter value for 'contains' operator must be a string")
	}
	
	return strings.Contains(valStr, filterStr), nil
}

// compareStartsWith checks if string value starts with filter value
func (m *Metadata) compareStartsWith(value interface{}, filterValue interface{}) (bool, error) {
	valStr, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("value for 'startswith' operator must be a string")
	}
	
	filterStr, ok := filterValue.(string)
	if !ok {
		return false, fmt.Errorf("filter value for 'startswith' operator must be a string")
	}
	
	return strings.HasPrefix(valStr, filterStr), nil
}

// compareEndsWith checks if string value ends with filter value
func (m *Metadata) compareEndsWith(value interface{}, filterValue interface{}) (bool, error) {
	valStr, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("value for 'endswith' operator must be a string")
	}
	
	filterStr, ok := filterValue.(string)
	if !ok {
		return false, fmt.Errorf("filter value for 'endswith' operator must be a string")
	}
	
	return strings.HasSuffix(valStr, filterStr), nil
}

// Serialize converts metadata to JSON bytes
func (m *Metadata) Serialize() ([]byte, error) {
	if m == nil {
		return nil, errors.New("cannot serialize nil metadata")
	}
	
	return json.Marshal(m)
}

// DeserializeMetadata creates metadata from JSON bytes
func DeserializeMetadata(data []byte) (*Metadata, error) {
	if len(data) == 0 {
		return NewMetadata(), nil
	}
	
	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to deserialize metadata: %v", err)
	}
	
	return &metadata, nil
}

// Clone creates a deep copy of the metadata
func (m *Metadata) Clone() (*Metadata, error) {
	if m == nil {
		return nil, errors.New("cannot clone nil metadata")
	}
	
	data, err := m.Serialize()
	if err != nil {
		return nil, err
	}
	
	return DeserializeMetadata(data)
}

// Merge merges another metadata instance into this one
func (m *Metadata) Merge(other *Metadata) error {
	if m == nil || other == nil {
		return errors.New("cannot merge nil metadata")
	}
	
	for key, value := range other.Fields {
		if err := m.Set(key, value); err != nil {
			return err
		}
	}
	
	return nil
}