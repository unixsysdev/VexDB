package validation

import (
	"fmt"
	"strings"

	"vexdb/internal/types"
)

// ProtocolValidator handles protocol-specific validation
type ProtocolValidator struct {
	// Protocol-specific validation rules
	httpRules      *HTTPValidationRules
	websocketRules *WebSocketValidationRules
	grpcRules      *GRPCValidationRules
	redisRules     *RedisValidationRules
}

// HTTPValidationRules contains HTTP-specific validation rules
type HTTPValidationRules struct {
	MaxHeaderSize       int
	MaxURLLength        int
	AllowedMethods      []string
	AllowedContentTypes []string
	RequiredHeaders     []string
}

// WebSocketValidationRules contains WebSocket-specific validation rules
type WebSocketValidationRules struct {
	MaxMessageSize       int64
	AllowedOrigins       []string
	RequiredSubprotocols []string
}

// GRPCValidationRules contains gRPC-specific validation rules
type GRPCValidationRules struct {
	MaxMessageSize  int
	MaxMetadataSize int
	AllowedMethods  []string
}

// RedisValidationRules contains Redis-specific validation rules
type RedisValidationRules struct {
	MaxCommandLength int
	MaxArguments     int
	AllowedCommands  []string
}

// NewProtocolValidator creates a new protocol validator
func NewProtocolValidator() *ProtocolValidator {
	return &ProtocolValidator{
		httpRules: &HTTPValidationRules{
			MaxHeaderSize:       8192,
			MaxURLLength:        2048,
			AllowedMethods:      []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowedContentTypes: []string{"application/json", "application/x-protobuf"},
			RequiredHeaders:     []string{"content-type"},
		},
		websocketRules: &WebSocketValidationRules{
			MaxMessageSize:       10 << 20, // 10MB
			AllowedOrigins:       []string{"*"},
			RequiredSubprotocols: []string{"vexdb-v1"},
		},
		grpcRules: &GRPCValidationRules{
			MaxMessageSize:  4 << 20, // 4MB
			MaxMetadataSize: 8192,
			AllowedMethods:  []string{"InsertVector", "InsertBatch", "HealthCheck"},
		},
		redisRules: &RedisValidationRules{
			MaxCommandLength: 1024,
			MaxArguments:     100,
			AllowedCommands:  []string{"PING", "VECTOR.ADD", "VECTOR.MADD", "HELLO", "COMMAND", "INFO"},
		},
	}
}

// ValidateHTTPRequest validates HTTP request data
func (v *ProtocolValidator) ValidateHTTPRequest(method, url, contentType string, headers map[string]string) error {
	// Validate method
	if !v.contains(v.httpRules.AllowedMethods, method) {
		return fmt.Errorf("method not allowed: %s", method)
	}

	// Validate URL length
	if len(url) > v.httpRules.MaxURLLength {
		return fmt.Errorf("URL too long: %d > %d", len(url), v.httpRules.MaxURLLength)
	}

	// Validate content type
	if contentType != "" && !v.contains(v.httpRules.AllowedContentTypes, contentType) {
		return fmt.Errorf("content type not allowed: %s", contentType)
	}

	// Validate required headers
	for _, header := range v.httpRules.RequiredHeaders {
		if _, exists := headers[header]; !exists {
			return fmt.Errorf("required header missing: %s", header)
		}
	}

	// Validate header size
	for key, value := range headers {
		if len(key)+len(value) > v.httpRules.MaxHeaderSize {
			return fmt.Errorf("header too large: %s", key)
		}
	}

	return nil
}

// ValidateWebSocketMessage validates WebSocket message data
func (v *ProtocolValidator) ValidateWebSocketMessage(messageType string, data []byte, origin string) error {
	// Validate message size
	if int64(len(data)) > v.websocketRules.MaxMessageSize {
		return fmt.Errorf("message too large: %d > %d", len(data), v.websocketRules.MaxMessageSize)
	}

	// Validate origin
	if !v.isOriginAllowed(origin) {
		return fmt.Errorf("origin not allowed: %s", origin)
	}

	// Validate message type based on content
	if messageType == "text" {
		return v.validateWebSocketTextMessage(data)
	}

	return nil
}

// ValidateGRPCRequest validates gRPC request data
func (v *ProtocolValidator) ValidateGRPCRequest(method string, messageSize, metadataSize int) error {
	// Validate method
	if !v.contains(v.grpcRules.AllowedMethods, method) {
		return fmt.Errorf("method not allowed: %s", method)
	}

	// Validate message size
	if messageSize > v.grpcRules.MaxMessageSize {
		return fmt.Errorf("message too large: %d > %d", messageSize, v.grpcRules.MaxMessageSize)
	}

	// Validate metadata size
	if metadataSize > v.grpcRules.MaxMetadataSize {
		return fmt.Errorf("metadata too large: %d > %d", metadataSize, v.grpcRules.MaxMetadataSize)
	}

	return nil
}

// ValidateRedisCommand validates Redis command data
func (v *ProtocolValidator) ValidateRedisCommand(command string, args []string) error {
	// Validate command length
	if len(command) > v.redisRules.MaxCommandLength {
		return fmt.Errorf("command too long: %d > %d", len(command), v.redisRules.MaxCommandLength)
	}

	// Validate command
	if !v.contains(v.redisRules.AllowedCommands, strings.ToUpper(command)) {
		return fmt.Errorf("command not allowed: %s", command)
	}

	// Validate argument count
	if len(args) > v.redisRules.MaxArguments {
		return fmt.Errorf("too many arguments: %d > %d", len(args), v.redisRules.MaxArguments)
	}

	// Validate specific commands
	switch strings.ToUpper(command) {
	case "VECTOR.ADD":
		return v.validateVectorAddCommand(args)
	case "VECTOR.MADD":
		return v.validateVectorMAddCommand(args)
	}

	return nil
}

// validateVectorAddCommand validates VECTOR.ADD command arguments
func (v *ProtocolValidator) validateVectorAddCommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("VECTOR.ADD requires at least 1 argument")
	}

	// Validate vector data format
	// This is a simplified validation - in practice, you'd parse and validate the actual vector
	if len(args[0]) == 0 {
		return fmt.Errorf("vector data cannot be empty")
	}

	return nil
}

// validateVectorMAddCommand validates VECTOR.MADD command arguments
func (v *ProtocolValidator) validateVectorMAddCommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("VECTOR.MADD requires at least 1 argument")
	}

	// Validate each vector data
	for i, arg := range args {
		if len(arg) == 0 {
			return fmt.Errorf("vector data at index %d cannot be empty", i)
		}
	}

	return nil
}

// validateWebSocketTextMessage validates WebSocket text message content
func (v *ProtocolValidator) validateWebSocketTextMessage(data []byte) error {
	// Basic JSON validation
	if !v.isValidJSON(string(data)) {
		return fmt.Errorf("invalid JSON message")
	}

	return nil
}

// isOriginAllowed checks if the origin is allowed
func (v *ProtocolValidator) isOriginAllowed(origin string) bool {
	for _, allowed := range v.websocketRules.AllowedOrigins {
		if allowed == "*" || allowed == origin {
			return true
		}
	}
	return false
}

// isValidJSON checks if a string is valid JSON
func (v *ProtocolValidator) isValidJSON(str string) bool {
	// Simple JSON validation - in practice, you'd use a proper JSON parser
	str = strings.TrimSpace(str)
	return (strings.HasPrefix(str, "{") && strings.HasSuffix(str, "}")) ||
		(strings.HasPrefix(str, "[") && strings.HasSuffix(str, "]"))
}

// contains checks if a string slice contains a specific string
func (v *ProtocolValidator) contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// ValidateVectorForProtocol validates vector data for specific protocol requirements
func (v *ProtocolValidator) ValidateVectorForProtocol(vector *types.Vector, protocol string) error {
	// Basic vector validation
	if vector == nil {
		return fmt.Errorf("vector cannot be nil")
	}

	if len(vector.Data) == 0 {
		return fmt.Errorf("vector data cannot be empty")
	}

	dim := len(vector.Data)
	if dim <= 0 {
		return fmt.Errorf("vector dimensions must be positive")
	}

	// Protocol-specific validation
	switch protocol {
	case "http":
		return v.validateVectorForHTTP(vector)
	case "websocket":
		return v.validateVectorForWebSocket(vector)
	case "grpc":
		return v.validateVectorForGRPC(vector)
	case "redis":
		return v.validateVectorForRedis(vector)
	default:
		return fmt.Errorf("unknown protocol: %s", protocol)
	}
}

// validateVectorForHTTP validates vector for HTTP protocol
func (v *ProtocolValidator) validateVectorForHTTP(vector *types.Vector) error {
	// HTTP-specific validation
	if vector.ID == "" {
		return fmt.Errorf("vector ID is required for HTTP protocol")
	}

	if vector.Metadata == nil {
		return fmt.Errorf("vector metadata is required for HTTP protocol")
	}

	return nil
}

// validateVectorForWebSocket validates vector for WebSocket protocol
func (v *ProtocolValidator) validateVectorForWebSocket(vector *types.Vector) error {
	// WebSocket-specific validation
	if vector.ID == "" {
		return fmt.Errorf("vector ID is required for WebSocket protocol")
	}

	// Check vector size for WebSocket transmission
	if len(vector.Data) > 10000 {
		return fmt.Errorf("vector dimensions too large for WebSocket: %d > 10000", len(vector.Data))
	}

	return nil
}

// validateVectorForGRPC validates vector for gRPC protocol
func (v *ProtocolValidator) validateVectorForGRPC(vector *types.Vector) error {
	// gRPC-specific validation
	if vector.ID == "" {
		return fmt.Errorf("vector ID is required for gRPC protocol")
	}

	// Check
	// Check vector size for gRPC transmission
	if len(vector.Data) > 50000 {
		return fmt.Errorf("vector dimensions too large for gRPC: %d > 50000", len(vector.Data))
	}

	return nil
}

// validateVectorForRedis validates vector for Redis protocol
func (v *ProtocolValidator) validateVectorForRedis(vector *types.Vector) error {
	// Redis-specific validation
	if vector.ID == "" {
		return fmt.Errorf("vector ID is required for Redis protocol")
	}

	// Check vector size for Redis transmission
	if len(vector.Data) > 1000 {
		return fmt.Errorf("vector dimensions too large for Redis: %d > 1000", len(vector.Data))
	}

	return nil
}
