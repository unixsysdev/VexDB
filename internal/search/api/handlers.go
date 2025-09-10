package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"vxdb/internal/metrics"
	"vxdb/internal/search/config"
	"vxdb/internal/search/response"
	"vxdb/internal/search/validation"
	"vxdb/internal/service"
	"vxdb/internal/types"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// SearchHandler handles HTTP search requests
type SearchHandler struct {
	config    *config.SearchServiceConfig
	logger    *zap.Logger
	metrics   *metrics.Collector
	validator *validation.SearchRequestValidator
	formatter *response.Formatter
	service   service.SearchService
}

// NewSearchHandler creates a new search handler
func NewSearchHandler(cfg *config.SearchServiceConfig, logger *zap.Logger, metrics *metrics.Collector, service service.SearchService) *SearchHandler {
	validator := validation.NewSearchRequestValidator(
		10000, // maxVectorDim
		cfg.Engine.MaxResults,
		cfg.Engine.MaxResults,
		100, // maxMetadataFilters
	)

	formatter := response.NewFormatter(&response.ResponseConfig{
		IncludeQueryVector: cfg.API.Response.IncludeQueryVector,
		IncludeMetadata:    cfg.API.Response.IncludeMetadata,
		IncludeTimestamps:  cfg.API.Response.IncludeTimestamps,
		PrettyPrint:        cfg.API.Response.PrettyPrint,
		MaxResults:         cfg.Engine.MaxResults,
		ResponseTimeout:    cfg.Server.WriteTimeout,
	})

	return &SearchHandler{
		config:    cfg,
		logger:    logger,
		metrics:   metrics,
		validator: validator,
		formatter: formatter,
		service:   service,
	}
}

// RegisterRoutes registers all search API routes
func (h *SearchHandler) RegisterRoutes(router *mux.Router) {
	// Search endpoints
	router.HandleFunc("/search", h.handleSearch).Methods("POST")
	router.HandleFunc("/search/similarity", h.handleSimilaritySearch).Methods("POST")
	router.HandleFunc("/search/batch", h.handleBatchSearch).Methods("POST")
	router.HandleFunc("/search/multi-cluster", h.handleMultiClusterSearch).Methods("POST")

	// Vector operations
	router.HandleFunc("/vectors/{id}", h.handleGetVector).Methods("GET")
	router.HandleFunc("/vectors/{id}", h.handleDeleteVector).Methods("DELETE")

	// Cluster operations
	router.HandleFunc("/clusters", h.handleListClusters).Methods("GET")
	router.HandleFunc("/clusters/{id}", h.handleGetCluster).Methods("GET")
	router.HandleFunc("/clusters/{id}/search", h.handleClusterSearch).Methods("POST")

	// Health and metrics
	router.HandleFunc("/health", h.handleHealthCheck).Methods("GET")
	router.HandleFunc("/health/detailed", h.handleDetailedHealthCheck).Methods("GET")
	router.HandleFunc("/metrics", h.handleMetrics).Methods("GET")

	// Configuration
	router.HandleFunc("/config", h.handleGetConfig).Methods("GET")
	router.HandleFunc("/config", h.handleUpdateConfig).Methods("PUT")
}

// handleSearch handles basic similarity search requests
func (h *SearchHandler) handleSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	startTime := time.Now()
	defer func() {
		h.recordMetrics("search", time.Since(startTime), nil)
	}()

	// Parse request
	var request SearchRequest
	if err := h.parseRequest(r, &request); err != nil {
		h.handleError(w, r, err, queryID, "INVALID_REQUEST")
		return
	}

	// Validate request
	if err := h.validateSearchRequest(&request); err != nil {
		h.handleValidationError(w, r, err, queryID)
		return
	}

	// Convert to internal types
	queryVector := &types.Vector{
		ID:       request.VectorID,
		Data:     toFloat32(request.Vector),
		Metadata: toMetadata(request.Metadata),
	}

	// Set default k if not provided
	k := request.K
	if k <= 0 {
		k = h.config.Engine.DefaultK
	}

	// Perform search
	results, err := h.service.Search(ctx, queryVector, k, request.MetadataFilter)
	if err != nil {
		h.handleError(w, r, err, queryID, "SEARCH_ERROR")
		return
	}

	// Format response
	stats := &response.SearchStats{
		TotalResults:    len(results),
		ReturnedResults: len(results),
		ExecutionTime:   time.Since(startTime),
	}

	response := h.formatter.FormatSearchResponse(results, queryVector, k, queryID, stats)
	h.sendJSONResponse(w, http.StatusOK, response)
}

// handleSimilaritySearch handles similarity search requests with advanced options
func (h *SearchHandler) handleSimilaritySearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	startTime := time.Now()
	defer func() {
		h.recordMetrics("similarity_search", time.Since(startTime), nil)
	}()

	// Parse request
	var request SimilaritySearchRequest
	if err := h.parseRequest(r, &request); err != nil {
		h.handleError(w, r, err, queryID, "INVALID_REQUEST")
		return
	}

	// Validate request
	if err := h.validateSimilaritySearchRequest(&request); err != nil {
		h.handleValidationError(w, r, err, queryID)
		return
	}

	// Convert to internal types
	queryVector := &types.Vector{
		ID:       request.VectorID,
		Data:     toFloat32(request.Vector),
		Metadata: toMetadata(request.Metadata),
	}

	// Build metadata filter if provided
	var filter *types.MetadataFilter
	if len(request.Filters) > 0 {
		filter = &types.MetadataFilter{Conditions: make([]interface{}, 0, len(request.Filters))}
		for _, f := range request.Filters {
			cond := &types.MetadataCondition{Key: f.Key, Operator: f.Operator, Value: f.Value, Values: f.Values}
			filter.Conditions = append(filter.Conditions, cond)
		}
	}

	// Set default k if not provided
	k := request.K
	if k <= 0 {
		k = h.config.Engine.DefaultK
	}

	// Perform search
	results, err := h.service.Search(ctx, queryVector, k, request.MetadataFilter)
	if err != nil {
		h.handleError(w, r, err, queryID, "SEARCH_ERROR")
		return
	}

	// Apply distance threshold if provided
	if request.DistanceThreshold > 0 {
		results = h.filterByDistance(results, request.DistanceThreshold)
	}

	// Format response
	stats := &response.SearchStats{
		TotalResults:    len(results),
		ReturnedResults: len(results),
		ExecutionTime:   time.Since(startTime),
	}

	response := h.formatter.FormatSearchResponse(results, queryVector, k, queryID, stats)
	h.sendJSONResponse(w, http.StatusOK, response)
}

// handleBatchSearch handles batch search requests
func (h *SearchHandler) handleBatchSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	startTime := time.Now()
	defer func() {
		h.recordMetrics("batch_search", time.Since(startTime), nil)
	}()

	// Parse request
	var request BatchSearchRequest
	if err := h.parseRequest(r, &request); err != nil {
		h.handleError(w, r, err, queryID, "INVALID_REQUEST")
		return
	}

	// Validate request
	if err := h.validateBatchSearchRequest(&request); err != nil {
		h.handleValidationError(w, r, err, queryID)
		return
	}

	// Process each query in the batch
	var allResults [][]*types.SearchResult
	var queryIDs []string
	var stats []*response.SearchStats

	for i, query := range request.Queries {
		queryVector := &types.Vector{
			ID:       query.VectorID,
			Data:     toFloat32(query.Vector),
			Metadata: toMetadata(query.Metadata),
		}

		k := query.K
		if k <= 0 {
			k = h.config.Engine.DefaultK
		}

		queryStart := time.Now()
		results, err := h.service.Search(ctx, queryVector, k, request.MetadataFilter)
		if err != nil {
			h.logger.Error("Batch search query failed", zap.Int("index", i), zap.Error(err))
			continue
		}

		allResults = append(allResults, results)
		queryIDs = append(queryIDs, fmt.Sprintf("%s_%d", queryID, i))

		stats = append(stats, &response.SearchStats{
			TotalResults:    len(results),
			ReturnedResults: len(results),
			ExecutionTime:   time.Since(queryStart),
		})
	}

	// Format batch response
	batchResponse := h.formatter.FormatBatchResponse(allResults, nil, request.K, queryIDs, stats)
	h.sendJSONResponse(w, http.StatusOK, batchResponse)
}

// handleMultiClusterSearch handles multi-cluster search requests
func (h *SearchHandler) handleMultiClusterSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	startTime := time.Now()
	defer func() {
		h.recordMetrics("multi_cluster_search", time.Since(startTime), nil)
	}()

	// Parse request
	var request MultiClusterSearchRequest
	if err := h.parseRequest(r, &request); err != nil {
		h.handleError(w, r, err, queryID, "INVALID_REQUEST")
		return
	}

	// Validate request
	if err := h.validateMultiClusterSearchRequest(&request); err != nil {
		h.handleValidationError(w, r, err, queryID)
		return
	}

	// Convert to internal types
	queryVector := &types.Vector{
		ID:       request.VectorID,
		Data:     toFloat32(request.Vector),
		Metadata: toMetadata(request.Metadata),
	}

	// Set default k if not provided
	k := request.K
	if k <= 0 {
		k = h.config.Engine.DefaultK
	}

	// Perform multi-cluster search
	results, err := h.service.MultiClusterSearch(ctx, queryVector, k, request.ClusterIDs, request.MetadataFilter)
	if err != nil {
		h.handleError(w, r, err, queryID, "SEARCH_ERROR")
		return
	}

	// Format response
	stats := &response.SearchStats{
		TotalResults:    len(results),
		ReturnedResults: len(results),
		ExecutionTime:   time.Since(startTime),
		ClustersQueried: len(request.ClusterIDs),
	}

	response := h.formatter.FormatSearchResponse(results, queryVector, k, queryID, stats)
	h.sendJSONResponse(w, http.StatusOK, response)
}

// handleGetVector handles get vector requests
func (h *SearchHandler) handleGetVector(w http.ResponseWriter, r *http.Request) {
	queryID := generateQueryID()
	vars := mux.Vars(r)
	vectorID := vars["id"]

	// Validate vector ID
	if err := h.validator.ValidateVectorID(vectorID); err != nil {
		h.handleValidationError(w, r, err, queryID)
		return
	}

	// For now, return not implemented
	h.handleError(w, r, fmt.Errorf("get vector not implemented"), queryID, "NOT_IMPLEMENTED")
}

// handleDeleteVector handles delete vector requests
func (h *SearchHandler) handleDeleteVector(w http.ResponseWriter, r *http.Request) {
	queryID := generateQueryID()
	vars := mux.Vars(r)
	vectorID := vars["id"]

	// Validate vector ID
	if err := h.validator.ValidateVectorID(vectorID); err != nil {
		h.handleValidationError(w, r, err, queryID)
		return
	}

	// For now, return not implemented
	h.handleError(w, r, fmt.Errorf("delete vector not implemented"), queryID, "NOT_IMPLEMENTED")
}

// handleListClusters handles list clusters requests
func (h *SearchHandler) handleListClusters(w http.ResponseWriter, r *http.Request) {
	queryID := generateQueryID()

	// For now, return not implemented
	h.handleError(w, r, fmt.Errorf("list clusters not implemented"), queryID, "NOT_IMPLEMENTED")
}

// handleGetCluster handles get cluster requests
func (h *SearchHandler) handleGetCluster(w http.ResponseWriter, r *http.Request) {
	queryID := generateQueryID()
	_ = mux.Vars(r)["id"]

	// For now, return not implemented
	h.handleError(w, r, fmt.Errorf("get cluster not implemented"), queryID, "NOT_IMPLEMENTED")
}

// handleClusterSearch handles cluster-specific search requests
func (h *SearchHandler) handleClusterSearch(w http.ResponseWriter, r *http.Request) {
	queryID := generateQueryID()
	_ = mux.Vars(r)["id"]

	// For now, return not implemented
	h.handleError(w, r, fmt.Errorf("cluster search not implemented"), queryID, "NOT_IMPLEMENTED")
}

// handleHealthCheck handles health check requests
func (h *SearchHandler) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	health, err := h.service.HealthCheck(ctx, false)
	if err != nil {
		h.handleError(w, r, err, queryID, "HEALTH_CHECK_ERROR")
		return
	}

	response := h.formatter.FormatHealthResponse(health.Status == "healthy", health.Status, map[string]interface{}{
		"message":   health.Message,
		"details":   health.Details,
		"timestamp": health.Timestamp,
	})
	h.sendJSONResponse(w, http.StatusOK, response)
}

// handleDetailedHealthCheck handles detailed health check requests
func (h *SearchHandler) handleDetailedHealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	health, err := h.service.HealthCheck(ctx, true)
	if err != nil {
		h.handleError(w, r, err, queryID, "HEALTH_CHECK_ERROR")
		return
	}

	response := h.formatter.FormatHealthResponse(health.Status == "healthy", health.Status, map[string]interface{}{
		"message":   health.Message,
		"details":   health.Details,
		"timestamp": health.Timestamp,
		"checks":    health.Checks,
	})
	h.sendJSONResponse(w, http.StatusOK, response)
}

// handleMetrics handles metrics requests
func (h *SearchHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	// Get metric type from query parameters
	metricType := r.URL.Query().Get("type")
	if metricType == "" {
		metricType = "system"
	}

	mvals, err := h.service.GetMetrics(ctx, metricType, "", "")
	if err != nil {
		h.handleError(w, r, err, queryID, "METRICS_ERROR")
		return
	}

	// Convert to map[string]interface{}
	imap := make(map[string]interface{}, len(mvals))
	for k, v := range mvals {
		imap[k] = v
	}
	response := h.formatter.FormatMetricsResponse(imap)
	h.sendJSONResponse(w, http.StatusOK, response)
}

// handleGetConfig handles get configuration requests
func (h *SearchHandler) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	config, err := h.service.GetConfig(ctx)
	if err != nil {
		h.handleError(w, r, err, queryID, "CONFIG_ERROR")
		return
	}

	h.sendJSONResponse(w, http.StatusOK, config)
}

// handleUpdateConfig handles update configuration requests
func (h *SearchHandler) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queryID := generateQueryID()

	var updates map[string]string
	if err := h.parseRequest(r, &updates); err != nil {
		h.handleError(w, r, err, queryID, "INVALID_REQUEST")
		return
	}

	if err := h.service.UpdateConfig(ctx, updates); err != nil {
		h.handleError(w, r, err, queryID, "CONFIG_UPDATE_ERROR")
		return
	}

	h.sendJSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Configuration updated successfully",
	})
}

// Request types

type SearchRequest struct {
	Vector         []float64         `json:"vector"`
	VectorID       string            `json:"vector_id,omitempty"`
	K              int               `json:"k,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	MetadataFilter map[string]string `json:"metadata_filter,omitempty"`
}

type SimilaritySearchRequest struct {
	Vector            []float64         `json:"vector"`
	VectorID          string            `json:"vector_id,omitempty"`
	K                 int               `json:"k,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	MetadataFilter    map[string]string `json:"metadata_filter,omitempty"`
	Filters           []MetadataFilter  `json:"filters,omitempty"`
	DistanceThreshold float64           `json:"distance_threshold,omitempty"`
}

type MetadataFilter struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Value    string   `json:"value,omitempty"`
	Values   []string `json:"values,omitempty"`
}

type BatchSearchRequest struct {
	Queries        []SearchRequest   `json:"queries"`
	MetadataFilter map[string]string `json:"metadata_filter,omitempty"`
	K              int               `json:"k,omitempty"`
}

type MultiClusterSearchRequest struct {
	Vector         []float64         `json:"vector"`
	VectorID       string            `json:"vector_id,omitempty"`
	K              int               `json:"k,omitempty"`
	ClusterIDs     []string          `json:"cluster_ids"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	MetadataFilter map[string]string `json:"metadata_filter,omitempty"`
}

// Helper methods

func (h *SearchHandler) parseRequest(r *http.Request, dest interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	return nil
}

func (h *SearchHandler) validateSearchRequest(request *SearchRequest) error {
	if len(request.Vector) == 0 {
		return fmt.Errorf("vector cannot be empty")
	}

	if request.K < 0 {
		return fmt.Errorf("k must be non-negative")
	}

	if request.K > h.config.Engine.MaxResults {
		return fmt.Errorf("k exceeds maximum allowed value of %d", h.config.Engine.MaxResults)
	}

	return nil
}

func (h *SearchHandler) validateSimilaritySearchRequest(request *SimilaritySearchRequest) error {
	if err := h.validateSearchRequest(&SearchRequest{
		Vector:   request.Vector,
		VectorID: request.VectorID,
		K:        request.K,
		Metadata: request.Metadata,
	}); err != nil {
		return err
	}

	if request.DistanceThreshold < 0 {
		return fmt.Errorf("distance threshold cannot be negative")
	}

	return nil
}

func (h *SearchHandler) validateBatchSearchRequest(request *BatchSearchRequest) error {
	if len(request.Queries) == 0 {
		return fmt.Errorf("queries cannot be empty")
	}

	if len(request.Queries) > 100 {
		return fmt.Errorf("too many queries: %d, maximum is 100", len(request.Queries))
	}

	for i, query := range request.Queries {
		if err := h.validateSearchRequest(&query); err != nil {
			return fmt.Errorf("invalid query at index %d: %w", i, err)
		}
	}

	return nil
}

func (h *SearchHandler) validateMultiClusterSearchRequest(request *MultiClusterSearchRequest) error {
	if err := h.validateSearchRequest(&SearchRequest{
		Vector:   request.Vector,
		VectorID: request.VectorID,
		K:        request.K,
		Metadata: request.Metadata,
	}); err != nil {
		return err
	}

	if len(request.ClusterIDs) == 0 {
		return fmt.Errorf("cluster IDs cannot be empty")
	}

	return nil
}

func (h *SearchHandler) filterByDistance(results []*types.SearchResult, threshold float64) []*types.SearchResult {
	filtered := make([]*types.SearchResult, 0, len(results))
	for _, result := range results {
		if result.Distance <= threshold {
			filtered = append(filtered, result)
		}
	}
	return filtered
}

func (h *SearchHandler) handleError(w http.ResponseWriter, r *http.Request, err error, queryID string, code string) {
	h.logger.Error("Request failed", zap.String("query_id", queryID), zap.Error(err))

	errorResponse := h.formatter.FormatErrorResponse(err, queryID, code, nil)
	h.sendJSONResponse(w, h.getHTTPStatusCode(code), errorResponse)
}

func (h *SearchHandler) handleValidationError(w http.ResponseWriter, r *http.Request, err error, queryID string) {
	h.logger.Warn("Validation failed", zap.String("query_id", queryID), zap.Error(err))

	// Convert error to validation errors
	validationErrors := []response.ValidationError{
		{
			Field:   "request",
			Message: err.Error(),
		},
	}

	errorResponse := h.formatter.FormatValidationErrorResponse(validationErrors, queryID)
	h.sendJSONResponse(w, http.StatusBadRequest, errorResponse)
}

func (h *SearchHandler) sendJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	jsonData, err := h.formatter.ToJSON(data)
	if err != nil {
		h.logger.Error("Failed to marshal JSON response", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(jsonData); err != nil {
		h.logger.Error("Failed to write response", zap.Error(err))
	}
}

func (h *SearchHandler) getHTTPStatusCode(errorCode string) int {
	switch errorCode {
	case "INVALID_REQUEST", "VALIDATION_ERROR":
		return http.StatusBadRequest
	case "NOT_FOUND":
		return http.StatusNotFound
	case "UNAUTHORIZED":
		return http.StatusUnauthorized
	case "RATE_LIMIT_EXCEEDED":
		return http.StatusTooManyRequests
	case "NOT_IMPLEMENTED":
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

func (h *SearchHandler) recordMetrics(operation string, duration time.Duration, err error) {
	h.metrics.RecordHistogram("search_request_duration", duration.Seconds(), map[string]string{
		"operation": operation,
		"status":    getStatusLabel(err),
	})
}

func generateQueryID() string {
	return fmt.Sprintf("query_%d", time.Now().UnixNano())
}

func getStatusLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

// toFloat32 converts []float64 to []float32 safely
func toFloat32(in []float64) []float32 {
	if len(in) == 0 {
		return nil
	}
	out := make([]float32, len(in))
	for i, v := range in {
		out[i] = float32(v)
	}
	return out
}

// toMetadata converts map[string]string to map[string]interface{}
func toMetadata(in map[string]string) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
