package types

import "errors"

// Storage-related errors
var (
	ErrStorageNotStarted = errors.New("storage not started")
	ErrSegmentClosed     = errors.New("segment closed")
	ErrInvalidSegmentFormat = errors.New("invalid segment format")
	ErrUnsupportedSegmentVersion = errors.New("unsupported segment version")
	ErrSearchFailed     = errors.New("search failed")
)

// Buffer-related errors
var (
	ErrBufferFull       = errors.New("buffer full")
	ErrBufferClosed     = errors.New("buffer closed")
	ErrBufferNotStarted = errors.New("buffer not started")
)

// Segment-related errors
var (
	ErrSegmentNotFound  = errors.New("segment not found")
	ErrSegmentExists    = errors.New("segment already exists")
	ErrSegmentCorrupted = errors.New("segment corrupted")
)

// Vector-related errors
var (
	ErrVectorNotFound   = errors.New("vector not found")
	ErrVectorExists     = errors.New("vector already exists")
	ErrVectorInvalid    = errors.New("vector invalid")
	ErrVectorTooLarge   = errors.New("vector too large")
)

// Cluster-related errors
var (
	ErrClusterNotFound  = errors.New("cluster not found")
	ErrClusterExists    = errors.New("cluster already exists")
	ErrClusterFull      = errors.New("cluster full")
	ErrNodeNotFound     = errors.New("node not found")
	ErrNodeExists       = errors.New("node already exists")
	ErrNodeUnhealthy    = errors.New("node unhealthy")
)

// Configuration-related errors
var (
	ErrConfigInvalid    = errors.New("configuration invalid")
	ErrConfigNotFound   = errors.New("configuration not found")
	ErrConfigExists     = errors.New("configuration already exists")
)

// Query-related errors
var (
	ErrQueryInvalid     = errors.New("query invalid")
	ErrQueryTimeout     = errors.New("query timeout")
	ErrQueryCancelled   = errors.New("query cancelled")
	ErrQueryFailed      = errors.New("query failed")
)

// Metadata-related errors
var (
	ErrMetadataInvalid  = errors.New("metadata invalid")
	ErrMetadataNotFound = errors.New("metadata not found")
	ErrMetadataExists   = errors.New("metadata already exists")
)

// Compression-related errors
var (
	ErrCompressionFailed = errors.New("compression failed")
	ErrDecompressionFailed = errors.New("decompression failed")
	ErrCompressionNotSupported = errors.New("compression not supported")
)

// Hashing-related errors
var (
	ErrHashFailed      = errors.New("hash failed")
	ErrHashInvalid     = errors.New("hash invalid")
	ErrHashNotFound    = errors.New("hash not found")
)

// Range-related errors
var (
	ErrRangeInvalid    = errors.New("range invalid")
	ErrRangeNotFound   = errors.New("range not found")
	ErrRangeExists     = errors.New("range already exists")
	ErrRangeOverlap    = errors.New("range overlap")
)

// Search-related errors
var (
	ErrSearchInvalid   = errors.New("search invalid")
	ErrSearchNotFound  = errors.New("search not found")
	ErrSearchExists    = errors.New("search already exists")
	ErrSearchTimeout   = errors.New("search timeout")
	ErrSearchCancelled = errors.New("search cancelled")
)

// Service-related errors
var (
	ErrServiceNotStarted = errors.New("service not started")
	ErrServiceStopped    = errors.New("service stopped")
	ErrServiceFailed     = errors.New("service failed")
	ErrServiceUnavailable = errors.New("service unavailable")
)

// Network-related errors
var (
	ErrNetworkTimeout   = errors.New("network timeout")
	ErrNetworkUnavailable = errors.New("network unavailable")
	ErrConnectionFailed = errors.New("connection failed")
	ErrConnectionClosed = errors.New("connection closed")
)

// Authentication-related errors
var (
	ErrAuthFailed      = errors.New("authentication failed")
	ErrAuthInvalid     = errors.New("authentication invalid")
	ErrAuthExpired     = errors.New("authentication expired")
	ErrAuthNotFound    = errors.New("authentication not found")
)

// Authorization-related errors
var (
	ErrUnauthorized    = errors.New("unauthorized")
	ErrForbidden       = errors.New("forbidden")
	ErrAccessDenied    = errors.New("access denied")
	ErrPermissionDenied = errors.New("permission denied")
)

// Rate limiting-related errors
var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
	ErrQuotaExceeded    = errors.New("quota exceeded")
	ErrThrottled        = errors.New("throttled")
)

// Validation-related errors
var (
	ErrValidationFailed = errors.New("validation failed")
	ErrInvalidInput     = errors.New("invalid input")
	ErrRequiredField    = errors.New("required field missing")
	ErrInvalidFormat    = errors.New("invalid format")
	ErrInvalidValue     = errors.New("invalid value")
	ErrInvalidLength    = errors.New("invalid length")
	ErrInvalidRange     = errors.New("invalid range")
	ErrInvalidType      = errors.New("invalid type")
)

// System-related errors
var (
	ErrSystemError      = errors.New("system error")
	ErrInternalError    = errors.New("internal error")
	ErrNotImplemented   = errors.New("not implemented")
	ErrNotSupported     = errors.New("not supported")
	ErrTimeout          = errors.New("timeout")
	ErrCancelled        = errors.New("cancelled")
	ErrBusy             = errors.New("busy")
	ErrUnavailable      = errors.New("unavailable")
	ErrFailed           = errors.New("failed")
	ErrUnknown          = errors.New("unknown error")
)