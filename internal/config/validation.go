package config

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vexdb/vexdb/internal/errors"
)

// Validator defines the interface for configuration validation
type Validator interface {
	Validate(config interface{}) error
	GetSchema() map[string]interface{}
}

// ValidationRule defines a validation rule
type ValidationRule struct {
	Name        string
	Description string
	Validate    func(value interface{}) error
}

// Schema represents a configuration schema
type Schema struct {
	Type        string
	Description string
	Required    bool
	Default     interface{}
	Rules       []ValidationRule
	Properties  map[string]*Schema
	Items       *Schema // For arrays
}

// ConfigValidator implements the Validator interface
type ConfigValidator struct {
	schema map[string]*Schema
}

// NewConfigValidator creates a new configuration validator
func NewConfigValidator() *ConfigValidator {
	return &ConfigValidator{
		schema: make(map[string]*Schema),
	}
}

// AddSchema adds a schema for a configuration field
func (v *ConfigValidator) AddSchema(field string, schema *Schema) {
	v.schema[field] = schema
}

// Validate validates a configuration object
func (v *ConfigValidator) Validate(config interface{}) error {
	configValue := reflect.ValueOf(config)
	if configValue.Kind() == reflect.Ptr {
		configValue = configValue.Elem()
	}

	if configValue.Kind() != reflect.Struct {
		return errors.NewConfigInvalidError("configuration must be a struct or pointer to struct")
	}

	configType := configValue.Type()
	for i := 0; i < configValue.NumField(); i++ {
		field := configType.Field(i)
		fieldValue := configValue.Field(i)

		// Get the field name from the struct tag or use the field name
		fieldName := field.Tag.Get("yaml")
		if fieldName == "" {
			fieldName = field.Tag.Get("json")
		}
		if fieldName == "" {
			fieldName = strings.ToLower(field.Name)
		}

		schema, exists := v.schema[fieldName]
		if !exists {
			continue // Skip fields without schema
		}

		if err := v.validateField(fieldName, fieldValue, schema); err != nil {
			return err
		}
	}

	return nil
}

// validateField validates a single field
func (v *ConfigValidator) validateField(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	// Check if field is required
	if schema.Required && fieldValue.IsZero() {
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' is required", fieldName))
	}

	// Use default value if field is zero and default is provided
	if fieldValue.IsZero() && schema.Default != nil {
		defaultValue := reflect.ValueOf(schema.Default)
		if defaultValue.Type().ConvertibleTo(fieldValue.Type()) {
			fieldValue.Set(defaultValue.Convert(fieldValue.Type()))
		}
	}

	// Skip validation if field is zero and not required
	if fieldValue.IsZero() && !schema.Required {
		return nil
	}

	// Validate based on type
	switch schema.Type {
	case "string":
		return v.validateString(fieldName, fieldValue, schema)
	case "int", "int32", "int64":
		return v.validateInt(fieldName, fieldValue, schema)
	case "float", "float32", "float64":
		return v.validateFloat(fieldName, fieldValue, schema)
	case "bool":
		return v.validateBool(fieldName, fieldValue, schema)
	case "array":
		return v.validateArray(fieldName, fieldValue, schema)
	case "object":
		return v.validateObject(fieldName, fieldValue, schema)
	case "duration":
		return v.validateDuration(fieldName, fieldValue, schema)
	default:
		return errors.NewConfigInvalidError(fmt.Sprintf("unknown type '%s' for field '%s'", schema.Type, fieldName))
	}
}

// validateString validates a string field
func (v *ConfigValidator) validateString(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	if fieldValue.Kind() != reflect.String {
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a string", fieldName))
	}

	value := fieldValue.String()

	for _, rule := range schema.Rules {
		if err := rule.Validate(value); err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' validation failed: %v", fieldName, err))
		}
	}

	return nil
}

// validateInt validates an integer field
func (v *ConfigValidator) validateInt(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	var value int64

	switch fieldValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value = fieldValue.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value = int64(fieldValue.Uint())
	case reflect.Float32, reflect.Float64:
		value = int64(fieldValue.Float())
	case reflect.String:
		var err error
		value, err = strconv.ParseInt(fieldValue.String(), 10, 64)
		if err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a valid integer", fieldName))
		}
	default:
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be an integer", fieldName))
	}

	for _, rule := range schema.Rules {
		if err := rule.Validate(value); err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' validation failed: %v", fieldName, err))
		}
	}

	return nil
}

// validateFloat validates a float field
func (v *ConfigValidator) validateFloat(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	var value float64

	switch fieldValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value = float64(fieldValue.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value = float64(fieldValue.Uint())
	case reflect.Float32, reflect.Float64:
		value = fieldValue.Float()
	case reflect.String:
		var err error
		value, err = strconv.ParseFloat(fieldValue.String(), 64)
		if err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a valid float", fieldName))
		}
	default:
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a float", fieldName))
	}

	for _, rule := range schema.Rules {
		if err := rule.Validate(value); err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' validation failed: %v", fieldName, err))
		}
	}

	return nil
}

// validateBool validates a boolean field
func (v *ConfigValidator) validateBool(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	var value bool

	switch fieldValue.Kind() {
	case reflect.Bool:
		value = fieldValue.Bool()
	case reflect.String:
		var err error
		value, err = strconv.ParseBool(fieldValue.String())
		if err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a valid boolean", fieldName))
		}
	default:
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a boolean", fieldName))
	}

	for _, rule := range schema.Rules {
		if err := rule.Validate(value); err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' validation failed: %v", fieldName, err))
		}
	}

	return nil
}

// validateArray validates an array field
func (v *ConfigValidator) validateArray(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	if fieldValue.Kind() != reflect.Slice && fieldValue.Kind() != reflect.Array {
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be an array", fieldName))
	}

	if schema.Items == nil {
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' array schema missing items definition", fieldName))
	}

	for i := 0; i < fieldValue.Len(); i++ {
		itemValue := fieldValue.Index(i)
		if err := v.validateField(fmt.Sprintf("%s[%d]", fieldName, i), itemValue, schema.Items); err != nil {
			return err
		}
	}

	return nil
}

// validateObject validates an object field
func (v *ConfigValidator) validateObject(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	if fieldValue.Kind() != reflect.Struct && fieldValue.Kind() != reflect.Map {
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be an object", fieldName))
	}

	if fieldValue.Kind() == reflect.Map {
		if fieldValue.Type().Key().Kind() != reflect.String {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' map keys must be strings", fieldName))
		}

		for _, key := range fieldValue.MapKeys() {
			itemValue := fieldValue.MapIndex(key)
			propertySchema, exists := schema.Properties[key.String()]
			if !exists {
				continue // Skip unknown properties
			}

			if err := v.validateField(fmt.Sprintf("%s.%s", fieldName, key.String()), itemValue, propertySchema); err != nil {
				return err
			}
		}
	} else {
		// Handle struct
		for propName, propSchema := range schema.Properties {
			field := fieldValue.FieldByName(strings.Title(propName))
			if !field.IsValid() {
				if propSchema.Required {
					return errors.NewConfigInvalidError(fmt.Sprintf("field '%s.%s' is required", fieldName, propName))
				}
				continue
			}

			if err := v.validateField(fmt.Sprintf("%s.%s", fieldName, propName), field, propSchema); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateDuration validates a duration field
func (v *ConfigValidator) validateDuration(fieldName string, fieldValue reflect.Value, schema *Schema) error {
	var value time.Duration

	switch fieldValue.Kind() {
	case reflect.String:
		var err error
		value, err = time.ParseDuration(fieldValue.String())
		if err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a valid duration", fieldName))
		}
	case reflect.Int, reflect.Int64:
		value = time.Duration(fieldValue.Int())
	default:
		return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' must be a duration", fieldName))
	}

	for _, rule := range schema.Rules {
		if err := rule.Validate(value); err != nil {
			return errors.NewConfigInvalidError(fmt.Sprintf("field '%s' validation failed: %v", fieldName, err))
		}
	}

	return nil
}

// GetSchema returns the validation schema
func (v *ConfigValidator) GetSchema() map[string]interface{} {
	result := make(map[string]interface{})
	for field, schema := range v.schema {
		result[field] = v.schemaToMap(schema)
	}
	return result
}

// schemaToMap converts a schema to a map representation
func (v *ConfigValidator) schemaToMap(schema *Schema) map[string]interface{} {
	result := map[string]interface{}{
		"type":        schema.Type,
		"description": schema.Description,
		"required":    schema.Required,
	}

	if schema.Default != nil {
		result["default"] = schema.Default
	}

	if len(schema.Properties) > 0 {
		properties := make(map[string]interface{})
		for prop, propSchema := range schema.Properties {
			properties[prop] = v.schemaToMap(propSchema)
		}
		result["properties"] = properties
	}

	if schema.Items != nil {
		result["items"] = v.schemaToMap(schema.Items)
	}

	return result
}

// Validation rule constructors

// MinLength creates a minimum length validation rule for strings
func MinLength(min int) ValidationRule {
	return ValidationRule{
		Name:        "min_length",
		Description: fmt.Sprintf("minimum length %d", min),
		Validate: func(value interface{}) error {
			str, ok := value.(string)
			if !ok {
				return fmt.Errorf("value must be a string")
			}
			if len(str) < min {
				return fmt.Errorf("value must be at least %d characters long", min)
			}
			return nil
		},
	}
}

// MaxLength creates a maximum length validation rule for strings
func MaxLength(max int) ValidationRule {
	return ValidationRule{
		Name:        "max_length",
		Description: fmt.Sprintf("maximum length %d", max),
		Validate: func(value interface{}) error {
			str, ok := value.(string)
			if !ok {
				return fmt.Errorf("value must be a string")
			}
			if len(str) > max {
				return fmt.Errorf("value must be at most %d characters long", max)
			}
			return nil
		},
	}
}

// Pattern creates a regex pattern validation rule for strings
func Pattern(pattern string) ValidationRule {
	regex := regexp.MustCompile(pattern)
	return ValidationRule{
		Name:        "pattern",
		Description: fmt.Sprintf("must match pattern %s", pattern),
		Validate: func(value interface{}) error {
			str, ok := value.(string)
			if !ok {
				return fmt.Errorf("value must be a string")
			}
			if !regex.MatchString(str) {
				return fmt.Errorf("value must match pattern %s", pattern)
			}
			return nil
		},
	}
}

// OneOf creates a validation rule that checks if the value is one of the allowed values
func OneOf(allowedValues ...interface{}) ValidationRule {
	return ValidationRule{
		Name:        "one_of",
		Description: fmt.Sprintf("must be one of %v", allowedValues),
		Validate: func(value interface{}) error {
			for _, allowed := range allowedValues {
				if reflect.DeepEqual(value, allowed) {
					return nil
				}
			}
			return fmt.Errorf("value must be one of %v", allowedValues)
		},
	}
}

// Min creates a minimum value validation rule for numbers
func Min(min interface{}) ValidationRule {
	return ValidationRule{
		Name:        "min",
		Description: fmt.Sprintf("minimum value %v", min),
		Validate: func(value interface{}) error {
			switch v := value.(type) {
			case int:
				if minInt, ok := min.(int); ok && v < minInt {
					return fmt.Errorf("value must be at least %d", minInt)
				}
			case int64:
				if minInt64, ok := min.(int64); ok && v < minInt64 {
					return fmt.Errorf("value must be at least %d", minInt64)
				}
			case float64:
				if minFloat64, ok := min.(float64); ok && v < minFloat64 {
					return fmt.Errorf("value must be at least %f", minFloat64)
				}
			case time.Duration:
				if minDuration, ok := min.(time.Duration); ok && v < minDuration {
					return fmt.Errorf("value must be at least %v", minDuration)
				}
			default:
				return fmt.Errorf("unsupported type for min validation")
			}
			return nil
		},
	}
}

// Max creates a maximum value validation rule for numbers
func Max(max interface{}) ValidationRule {
	return ValidationRule{
		Name:        "max",
		Description: fmt.Sprintf("maximum value %v", max),
		Validate: func(value interface{}) error {
			switch v := value.(type) {
			case int:
				if maxInt, ok := max.(int); ok && v > maxInt {
					return fmt.Errorf("value must be at most %d", maxInt)
				}
			case int64:
				if maxInt64, ok := max.(int64); ok && v > maxInt64 {
					return fmt.Errorf("value must be at most %d", maxInt64)
				}
			case float64:
				if maxFloat64, ok := max.(float64); ok && v > maxFloat64 {
					return fmt.Errorf("value must be at most %f", maxFloat64)
				}
			case time.Duration:
				if maxDuration, ok := max.(time.Duration); ok && v > maxDuration {
					return fmt.Errorf("value must be at most %v", maxDuration)
				}
			default:
				return fmt.Errorf("unsupported type for max validation")
			}
			return nil
		},
	}
}

// Positive creates a validation rule that checks if the value is positive
func Positive() ValidationRule {
	return ValidationRule{
		Name:        "positive",
		Description: "must be positive",
		Validate: func(value interface{}) error {
			switch v := value.(type) {
			case int:
				if v <= 0 {
					return fmt.Errorf("value must be positive")
				}
			case int64:
				if v <= 0 {
					return fmt.Errorf("value must be positive")
				}
			case float64:
				if v <= 0 {
					return fmt.Errorf("value must be positive")
				}
			default:
				return fmt.Errorf("unsupported type for positive validation")
			}
			return nil
		},
	}
}

// NonNegative creates a validation rule that checks if the value is non-negative
func NonNegative() ValidationRule {
	return ValidationRule{
		Name:        "non_negative",
		Description: "must be non-negative",
		Validate: func(value interface{}) error {
			switch v := value.(type) {
			case int:
				if v < 0 {
					return fmt.Errorf("value must be non-negative")
				}
			case int64:
				if v < 0 {
					return fmt.Errorf("value must be non-negative")
				}
			case float64:
				if v < 0 {
					return fmt.Errorf("value must be non-negative")
				}
			default:
				return fmt.Errorf("unsupported type for non-negative validation")
			}
			return nil
		},
	}
}

// Port creates a validation rule for port numbers
func Port() ValidationRule {
	return ValidationRule{
		Name:        "port",
		Description: "must be a valid port number (1-65535)",
		Validate: func(value interface{}) error {
			var port int
			switch v := value.(type) {
			case int:
				port = v
			case int64:
				port = int(v)
			case float64:
				port = int(v)
			case string:
				var err error
				port, err = strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("port must be a number")
				}
			default:
				return fmt.Errorf("port must be a number")
			}

			if port < 1 || port > 65535 {
				return fmt.Errorf("port must be between 1 and 65535")
			}
			return nil
		},
	}
}

// URL creates a validation rule for URLs
func URL() ValidationRule {
	return ValidationRule{
		Name:        "url",
		Description: "must be a valid URL",
		Validate: func(value interface{}) error {
			str, ok := value.(string)
			if !ok {
				return fmt.Errorf("value must be a string")
			}
			// Simple URL validation - in production, use a proper URL parser
			if !strings.Contains(str, "://") {
				return fmt.Errorf("value must be a valid URL")
			}
			return nil
		},
	}
}

// Email creates a validation rule for email addresses
func Email() ValidationRule {
	// Simple email regex - in production, use a more comprehensive one
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	return ValidationRule{
		Name:        "email",
		Description: "must be a valid email address",
		Validate: func(value interface{}) error {
			str, ok := value.(string)
			if !ok {
				return fmt.Errorf("value must be a string")
			}
			if !emailRegex.MatchString(str) {
				return fmt.Errorf("value must be a valid email address")
			}
			return nil
		},
	}
}