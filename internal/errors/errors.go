package errors

import (
	"errors"
	"fmt"
	"time"
)

// VexError represents a custom error type for VexDB
type VexError struct {
	Code      ErrorCode
	Message   string
	Details   map[string]interface{}
	Timestamp time.Time
	Cause     error
	Stack     string
}

// ErrorCode represents the type of error
type ErrorCode int

const (
	// General errors
	ErrorCodeUnknown ErrorCode = iota
	ErrorCodeInvalidArgument
	ErrorCodeNotFound
	ErrorCodeAlreadyExists
	ErrorCodePermissionDenied
	ErrorCodeUnauthenticated
	ErrorCodeResourceExhausted
	ErrorCodeFailedPrecondition
	ErrorCodeAborted
	ErrorCodeOutOfRange
	ErrorCodeUnimplemented
	ErrorCodeInternal
	ErrorCodeUnavailable
	ErrorCodeDeadlineExceeded
	
	// Vector-specific errors
	ErrorCodeInvalidVector
	ErrorCodeVectorNotFound
	ErrorCodeVectorDimensionMismatch
	ErrorCodeVectorTooLarge
	ErrorCodeVectorCorrupted
	
	// Cluster-specific errors
	ErrorCodeClusterNotFound
	ErrorCodeClusterNotReady
	ErrorCodeClusterFull
	ErrorCodeClusterInconsistent
	ErrorCodeClusterRebalanceFailed
	
	// Node-specific errors
	ErrorCodeNodeNotFound
	ErrorCodeNodeUnhealthy
	ErrorCodeNodeOverloaded
	ErrorCodeNodeDisconnected
	ErrorCodeNodeRecoveryFailed
	
	// Storage-specific errors
	ErrorCodeStorageFull
	ErrorCodeStorageCorrupted
	ErrorCodeStorageUnavailable
	ErrorCodeStorageWriteFailed
	ErrorCodeStorageReadFailed
	ErrorCodeStorageCompactionFailed
	
	// Search-specific errors
	ErrorCodeSearchFailed
	ErrorCodeQueryInvalid
	ErrorCodeQueryTimeout
	ErrorCodeQueryCancelled
	ErrorCodeSearchIndexCorrupted
	ErrorCodeSearchNotAvailable
	
	// Network-specific errors
	ErrorCodeNetworkUnavailable
	ErrorCodeConnectionFailed
	ErrorCodeConnectionTimeout
	ErrorCodeConnectionRefused
	ErrorCodeNetworkPartition
	
	// Configuration-specific errors
	ErrorCodeConfigInvalid
	ErrorCodeConfigNotFound
	ErrorCodeConfigParseFailed
	ErrorCodeConfigValidationFailed
	ErrorCodeConfigUpdateFailed
	
	// Protocol-specific errors
	ErrorCodeProtocolError
	ErrorCodeProtocolNotSupported
	ErrorCodeProtocolVersionMismatch
	ErrorCodeProtocolMessageInvalid
	ErrorCodeProtocolMessageTooLarge
	
	// Replication-specific errors
	ErrorCodeReplicationFailed
	ErrorCodeReplicationTimeout
	ErrorCodeReplicationInconsistent
	ErrorCodeReplicationQuorumNotReached
	ErrorCodeReplicationConflict
	
	// Metrics-specific errors
	ErrorCodeMetricsCollectionFailed
	ErrorCodeMetricsExportFailed
	ErrorCodeMetricsInvalid
	ErrorCodeMetricsTimeout
	
	// Health-specific errors
	ErrorCodeHealthCheckFailed
	ErrorCodeHealthCheckTimeout
	ErrorCodeHealthCheckInvalid
	ErrorCodeHealthCheckUnavailable
)

// Error returns the error message
func (e *VexError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.Code.String(), e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code.String(), e.Message)
}

// Unwrap returns the underlying error
func (e *VexError) Unwrap() error {
	return e.Cause
}

// Is checks if the error matches the target error
func (e *VexError) Is(target error) bool {
	var other *VexError
	if errors.As(target, &other) {
		return e.Code == other.Code
	}
	return false
}

// String returns the string representation of the error code
func (c ErrorCode) String() string {
	switch c {
	case ErrorCodeUnknown:
		return "UNKNOWN"
	case ErrorCodeInvalidArgument:
		return "INVALID_ARGUMENT"
	case ErrorCodeNotFound:
		return "NOT_FOUND"
	case ErrorCodeAlreadyExists:
		return "ALREADY_EXISTS"
	case ErrorCodePermissionDenied:
		return "PERMISSION_DENIED"
	case ErrorCodeUnauthenticated:
		return "UNAUTHENTICATED"
	case ErrorCodeResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case ErrorCodeFailedPrecondition:
		return "FAILED_PRECONDITION"
	case ErrorCodeAborted:
		return "ABORTED"
	case ErrorCodeOutOfRange:
		return "OUT_OF_RANGE"
	case ErrorCodeUnimplemented:
		return "UNIMPLEMENTED"
	case ErrorCodeInternal:
		return "INTERNAL"
	case ErrorCodeUnavailable:
		return "UNAVAILABLE"
	case ErrorCodeDeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case ErrorCodeInvalidVector:
		return "INVALID_VECTOR"
	case ErrorCodeVectorNotFound:
		return "VECTOR_NOT_FOUND"
	case ErrorCodeVectorDimensionMismatch:
		return "VECTOR_DIMENSION_MISMATCH"
	case ErrorCodeVectorTooLarge:
		return "VECTOR_TOO_LARGE"
	case ErrorCodeVectorCorrupted:
		return "VECTOR_CORRUPTED"
	case ErrorCodeClusterNotFound:
		return "CLUSTER_NOT_FOUND"
	case ErrorCodeClusterNotReady:
		return "CLUSTER_NOT_READY"
	case ErrorCodeClusterFull:
		return "CLUSTER_FULL"
	case ErrorCodeClusterInconsistent:
		return "CLUSTER_INCONSISTENT"
	case ErrorCodeClusterRebalanceFailed:
		return "CLUSTER_REBALANCE_FAILED"
	case ErrorCodeNodeNotFound:
		return "NODE_NOT_FOUND"
	case ErrorCodeNodeUnhealthy:
		return "NODE_UNHEALTHY"
	case ErrorCodeNodeOverloaded:
		return "NODE_OVERLOADED"
	case ErrorCodeNodeDisconnected:
		return "NODE_DISCONNECTED"
	case ErrorCodeNodeRecoveryFailed:
		return "NODE_RECOVERY_FAILED"
	case ErrorCodeStorageFull:
		return "STORAGE_FULL"
	case ErrorCodeStorageCorrupted:
		return "STORAGE_CORRUPTED"
	case ErrorCodeStorageUnavailable:
		return "STORAGE_UNAVAILABLE"
	case ErrorCodeStorageWriteFailed:
		return "STORAGE_WRITE_FAILED"
	case ErrorCodeStorageReadFailed:
		return "STORAGE_READ_FAILED"
	case ErrorCodeStorageCompactionFailed:
		return "STORAGE_COMPACTION_FAILED"
	case ErrorCodeSearchFailed:
		return "SEARCH_FAILED"
	case ErrorCodeQueryInvalid:
		return "QUERY_INVALID"
	case ErrorCodeQueryTimeout:
		return "QUERY_TIMEOUT"
	case ErrorCodeQueryCancelled:
		return "QUERY_CANCELLED"
	case ErrorCodeSearchIndexCorrupted:
		return "SEARCH_INDEX_CORRUPTED"
	case ErrorCodeSearchNotAvailable:
		return "SEARCH_NOT_AVAILABLE"
	case ErrorCodeNetworkUnavailable:
		return "NETWORK_UNAVAILABLE"
	case ErrorCodeConnectionFailed:
		return "CONNECTION_FAILED"
	case ErrorCodeConnectionTimeout:
		return "CONNECTION_TIMEOUT"
	case ErrorCodeConnectionRefused:
		return "CONNECTION_REFUSED"
	case ErrorCodeNetworkPartition:
		return "NETWORK_PARTITION"
	case ErrorCodeConfigInvalid:
		return "CONFIG_INVALID"
	case ErrorCodeConfigNotFound:
		return "CONFIG_NOT_FOUND"
	case ErrorCodeConfigParseFailed:
		return "CONFIG_PARSE_FAILED"
	case ErrorCodeConfigValidationFailed:
		return "CONFIG_VALIDATION_FAILED"
	case ErrorCodeConfigUpdateFailed:
		return "CONFIG_UPDATE_FAILED"
	case ErrorCodeProtocolError:
		return "PROTOCOL_ERROR"
	case ErrorCodeProtocolNotSupported:
		return "PROTOCOL_NOT_SUPPORTED"
	case ErrorCodeProtocolVersionMismatch:
		return "PROTOCOL_VERSION_MISMATCH"
	case ErrorCodeProtocolMessageInvalid:
		return "PROTOCOL_MESSAGE_INVALID"
	case ErrorCodeProtocolMessageTooLarge:
		return "PROTOCOL_MESSAGE_TOO_LARGE"
	case ErrorCodeReplicationFailed:
		return "REPLICATION_FAILED"
	case ErrorCodeReplicationTimeout:
		return "REPLICATION_TIMEOUT"
	case ErrorCodeReplicationInconsistent:
		return "REPLICATION_INCONSISTENT"
	case ErrorCodeReplicationQuorumNotReached:
		return "REPLICATION_QUORUM_NOT_REACHED"
	case ErrorCodeReplicationConflict:
		return "REPLICATION_CONFLICT"
	case ErrorCodeMetricsCollectionFailed:
		return "METRICS_COLLECTION_FAILED"
	case ErrorCodeMetricsExportFailed:
		return "METRICS_EXPORT_FAILED"
	case ErrorCodeMetricsInvalid:
		return "METRICS_INVALID"
	case ErrorCodeMetricsTimeout:
		return "METRICS_TIMEOUT"
	case ErrorCodeHealthCheckFailed:
		return "HEALTH_CHECK_FAILED"
	case ErrorCodeHealthCheckTimeout:
		return "HEALTH_CHECK_TIMEOUT"
	case ErrorCodeHealthCheckInvalid:
		return "HEALTH_CHECK_INVALID"
	case ErrorCodeHealthCheckUnavailable:
		return "HEALTH_CHECK_UNAVAILABLE"
	default:
		return "UNKNOWN"
	}
}

// New creates a new VexError
func New(code ErrorCode, message string) *VexError {
	return &VexError{
		Code:      code,
		Message:   message,
		Details:   make(map[string]interface{}),
		Timestamp: time.Now(),
	}
}

// Wrap wraps an existing error with additional context
func Wrap(err error, code ErrorCode, message string) *VexError {
	return &VexError{
		Code:      code,
		Message:   message,
		Details:   make(map[string]interface{}),
		Timestamp: time.Now(),
		Cause:     err,
	}
}

// WithDetails adds details to the error
func (e *VexError) WithDetails(details map[string]interface{}) *VexError {
	for k, v := range details {
		e.Details[k] = v
	}
	return e
}

// WithDetail adds a single detail to the error
func (e *VexError) WithDetail(key string, value interface{}) *VexError {
	e.Details[key] = value
	return e
}

// WithStack adds stack trace information
func (e *VexError) WithStack(stack string) *VexError {
	e.Stack = stack
	return e
}

// IsErrorCode checks if the error is a VexError with the specific error code
func IsErrorCode(err error, code ErrorCode) bool {
	var vexErr *VexError
	return errors.As(err, &vexErr) && vexErr.Code == code
}

// GetErrorCode returns the error code from a VexError
func GetErrorCode(err error) ErrorCode {
	var vexErr *VexError
	if errors.As(err, &vexErr) {
		return vexErr.Code
	}
	return ErrorCodeUnknown
}

// GetErrorDetails returns the details from a VexError
func GetErrorDetails(err error) map[string]interface{} {
	var vexErr *VexError
	if errors.As(err, &vexErr) {
		return vexErr.Details
	}
	return nil
}

// Predefined error constructors

// Vector errors
func NewInvalidVectorError(message string) *VexError {
	return New(ErrorCodeInvalidVector, message)
}

func NewVectorNotFoundError(vectorID string) *VexError {
	return New(ErrorCodeVectorNotFound, fmt.Sprintf("vector not found: %s", vectorID)).
		WithDetail("vector_id", vectorID)
}

func NewVectorDimensionMismatchError(expected, actual int) *VexError {
	return New(ErrorCodeVectorDimensionMismatch, fmt.Sprintf("vector dimension mismatch: expected %d, got %d", expected, actual)).
		WithDetail("expected_dimension", expected).
		WithDetail("actual_dimension", actual)
}

func NewVectorTooLargeError(size int64, maxSize int64) *VexError {
	return New(ErrorCodeVectorTooLarge, fmt.Sprintf("vector too large: size %d exceeds max size %d", size, maxSize)).
		WithDetail("size", size).
		WithDetail("max_size", maxSize)
}

// Cluster errors
func NewClusterNotFoundError(clusterID string) *VexError {
	return New(ErrorCodeClusterNotFound, fmt.Sprintf("cluster not found: %s", clusterID)).
		WithDetail("cluster_id", clusterID)
}

func NewClusterNotReadyError(clusterID string) *VexError {
	return New(ErrorCodeClusterNotReady, fmt.Sprintf("cluster not ready: %s", clusterID)).
		WithDetail("cluster_id", clusterID)
}

func NewClusterFullError(clusterID string) *VexError {
	return New(ErrorCodeClusterFull, fmt.Sprintf("cluster is full: %s", clusterID)).
		WithDetail("cluster_id", clusterID)
}

// Node errors
func NewNodeNotFoundError(nodeID string) *VexError {
	return New(ErrorCodeNodeNotFound, fmt.Sprintf("node not found: %s", nodeID)).
		WithDetail("node_id", nodeID)
}

func NewNodeUnhealthyError(nodeID string) *VexError {
	return New(ErrorCodeNodeUnhealthy, fmt.Sprintf("node is unhealthy: %s", nodeID)).
		WithDetail("node_id", nodeID)
}

func NewNodeOverloadedError(nodeID string) *VexError {
	return New(ErrorCodeNodeOverloaded, fmt.Sprintf("node is overloaded: %s", nodeID)).
		WithDetail("node_id", nodeID)
}

// Storage errors
func NewStorageFullError() *VexError {
	return New(ErrorCodeStorageFull, "storage is full")
}

func NewStorageCorruptedError(path string) *VexError {
	return New(ErrorCodeStorageCorrupted, fmt.Sprintf("storage corrupted: %s", path)).
		WithDetail("path", path)
}

func NewStorageUnavailableError() *VexError {
	return New(ErrorCodeStorageUnavailable, "storage is unavailable")
}

// Search errors
func NewSearchFailedError(message string) *VexError {
	return New(ErrorCodeSearchFailed, message)
}

func NewQueryInvalidError(message string) *VexError {
	return New(ErrorCodeQueryInvalid, message)
}

func NewQueryTimeoutError(queryID string) *VexError {
	return New(ErrorCodeQueryTimeout, fmt.Sprintf("query timeout: %s", queryID)).
		WithDetail("query_id", queryID)
}

// Network errors
func NewNetworkUnavailableError() *VexError {
	return New(ErrorCodeNetworkUnavailable, "network is unavailable")
}

func NewConnectionFailedError(address string) *VexError {
	return New(ErrorCodeConnectionFailed, fmt.Sprintf("connection failed: %s", address)).
		WithDetail("address", address)
}

func NewConnectionTimeoutError(address string) *VexError {
	return New(ErrorCodeConnectionTimeout, fmt.Sprintf("connection timeout: %s", address)).
		WithDetail("address", address)
}

// Configuration errors
func NewConfigInvalidError(message string) *VexError {
	return New(ErrorCodeConfigInvalid, message)
}

func NewConfigNotFoundError(path string) *VexError {
	return New(ErrorCodeConfigNotFound, fmt.Sprintf("config not found: %s", path)).
		WithDetail("path", path)
}

func NewConfigParseFailedError(path string, err error) *VexError {
	return Wrap(err, ErrorCodeConfigParseFailed, fmt.Sprintf("config parse failed: %s", path)).
		WithDetail("path", path)
}

// Replication errors
func NewReplicationFailedError(message string) *VexError {
	return New(ErrorCodeReplicationFailed, message)
}

func NewReplicationTimeoutError() *VexError {
	return New(ErrorCodeReplicationTimeout, "replication timeout")
}

func NewReplicationQuorumNotReachedError(required, actual int) *VexError {
	return New(ErrorCodeReplicationQuorumNotReached, fmt.Sprintf("replication quorum not reached: required %d, got %d", required, actual)).
		WithDetail("required", required).
		WithDetail("actual", actual)
}

// Health check errors
func NewHealthCheckFailedError(service string) *VexError {
	return New(ErrorCodeHealthCheckFailed, fmt.Sprintf("health check failed: %s", service)).
		WithDetail("service", service)
}

func NewHealthCheckTimeoutError(service string) *VexError {
	return New(ErrorCodeHealthCheckTimeout, fmt.Sprintf("health check timeout: %s", service)).
		WithDetail("service", service)
}

// NewInvalidArgumentError creates a new invalid argument error
func NewInvalidArgumentError(message string) *VexError {
	return New(ErrorCodeInvalidArgument, message)
}