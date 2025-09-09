package validation

import (
	"math"
	"testing"

	"vexdb/internal/types"
)

func TestNewSearchRequestValidator(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	if validator == nil {
		t.Fatal("Expected validator to be created, got nil")
	}
	
	if validator.maxVectorDim != 1000 {
		t.Errorf("Expected maxVectorDim to be 1000, got %d", validator.maxVectorDim)
	}
	
	if validator.maxK != 100 {
		t.Errorf("Expected maxK to be 100, got %d", validator.maxK)
	}
	
	if validator.maxResults != 1000 {
		t.Errorf("Expected maxResults to be 1000, got %d", validator.maxResults)
	}
	
	if validator.maxMetadataFilters != 50 {
		t.Errorf("Expected maxMetadataFilters to be 50, got %d", validator.maxMetadataFilters)
	}
}

func TestValidateSearchRequest_ValidRequest(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	query := &types.Vector{
		ID:   "test-vector",
		Data: []float32{1.0, 2.0, 3.0, 4.0, 5.0},
	}
	
	err := validator.ValidateSearchRequest(query, 10, nil)
	
	if err != nil {
		t.Errorf("Expected no error for valid request, got: %v", err)
	}
}

func TestValidateSearchRequest_NilQuery(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.ValidateSearchRequest(nil, 10, nil)
	
	if err == nil {
		t.Error("Expected error for nil query, got nil")
	}
	
	if err.Error() != "query vector cannot be nil" {
		t.Errorf("Expected 'query vector cannot be nil', got: %v", err)
	}
}

func TestValidateSearchRequest_EmptyVector(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	query := &types.Vector{
		ID:   "test-vector",
		Data: []float32{},
	}
	
	err := validator.ValidateSearchRequest(query, 10, nil)
	
	if err == nil {
		t.Error("Expected error for empty vector, got nil")
	}
	
	if err.Error() != "query vector cannot be empty" {
		t.Errorf("Expected 'query vector cannot be empty', got: %v", err)
	}
}

func TestValidateSearchRequest_VectorTooLarge(t *testing.T) {
	validator := NewSearchRequestValidator(5, 100, 1000, 50)
	
	query := &types.Vector{
		ID:   "test-vector",
		Data: []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}, // 6 dimensions, max is 5
	}
	
	err := validator.ValidateSearchRequest(query, 10, nil)
	
	if err == nil {
		t.Error("Expected error for vector too large, got nil")
	}
	
	expectedMsg := "query vector dimension 6 exceeds maximum 5"
	if err.Error() != expectedMsg {
		t.Errorf("Expected '%s', got: %v", expectedMsg, err)
	}
}

func TestValidateSearchRequest_InvalidVectorValues(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	// Test with NaN
	query := &types.Vector{
		ID:   "test-vector",
		Data: []float32{1.0, float32(math.NaN()), 3.0},
	}
	
	err := validator.ValidateSearchRequest(query, 10, nil)
	
	if err == nil {
		t.Error("Expected error for NaN value, got nil")
	}
	
	// Test with Inf
	query = &types.Vector{
		ID:   "test-vector",
		Data: []float32{1.0, float32(math.Inf(1)), 3.0},
	}
	
	err = validator.ValidateSearchRequest(query, 10, nil)
	
	if err == nil {
		t.Error("Expected error for Inf value, got nil")
	}
}

func TestValidateK_ValidK(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.validateK(50)
	
	if err != nil {
		t.Errorf("Expected no error for valid k, got: %v", err)
	}
}

func TestValidateK_ZeroK(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.validateK(0)
	
	if err == nil {
		t.Error("Expected error for zero k, got nil")
	}
	
	if err.Error() != "k must be positive, got 0" {
		t.Errorf("Expected 'k must be positive, got 0', got: %v", err)
	}
}

func TestValidateK_NegativeK(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.validateK(-5)
	
	if err == nil {
		t.Error("Expected error for negative k, got nil")
	}
	
	if err.Error() != "k must be positive, got -5" {
		t.Errorf("Expected 'k must be positive, got -5', got: %v", err)
	}
}

func TestValidateK_KTooLarge(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 50, 1000, 50)
	
	err := validator.validateK(100)
	
	if err == nil {
		t.Error("Expected error for k too large, got nil")
	}
	
	expectedMsg := "k 100 exceeds maximum 50"
	if err.Error() != expectedMsg {
		t.Errorf("Expected '%s', got: %v", expectedMsg, err)
	}
}

func TestValidateVectorID_ValidID(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.ValidateVectorID("valid-vector-id_123")
	
	if err != nil {
		t.Errorf("Expected no error for valid vector ID, got: %v", err)
	}
}

func TestValidateVectorID_EmptyID(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.ValidateVectorID("")
	
	if err == nil {
		t.Error("Expected error for empty vector ID, got nil")
	}
	
	if err.Error() != "vector ID cannot be empty" {
		t.Errorf("Expected 'vector ID cannot be empty', got: %v", err)
	}
}

func TestValidateVectorID_IDTooLong(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	// Create a very long ID (more than 128 characters)
	longID := "a"
	for i := 0; i < 130; i++ {
		longID += "a"
	}
	
	err := validator.ValidateVectorID(longID)
	
	if err == nil {
		t.Error("Expected error for vector ID too long, got nil")
	}
}

func TestValidateDistanceThreshold_ValidThreshold(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.ValidateDistanceThreshold(0.5)
	
	if err != nil {
		t.Errorf("Expected no error for valid threshold, got: %v", err)
	}
}

func TestValidateDistanceThreshold_NegativeThreshold(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.ValidateDistanceThreshold(-0.1)
	
	if err == nil {
		t.Error("Expected error for negative threshold, got nil")
	}
	
	if err.Error() != "distance threshold cannot be negative" {
		t.Errorf("Expected 'distance threshold cannot be negative', got: %v", err)
	}
}

func TestValidateDistanceThreshold_ThresholdTooLarge(t *testing.T) {
	validator := NewSearchRequestValidator(1000, 100, 1000, 50)
	
	err := validator.ValidateDistanceThreshold(1e7) // Too large
	
	if err == nil {
		t.Error("Expected error for threshold too large, got nil")
	}
}

func TestValidationErrors(t *testing.T) {
	errors := &ValidationErrors{}
	
	if errors.HasErrors() {
		t.Error("Expected HasErrors to return false for empty errors")
	}
	
	errors.AddError("field1", "error message", "value1")
	errors.AddError("field2", "another error", "value2")
	
	if !errors.HasErrors() {
		t.Error("Expected HasErrors to return true after adding errors")
	}
	
	if len(errors.GetErrors()) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(errors.GetErrors()))
	}
	
	errorStr := errors.Error()
	if errorStr == "" {
		t.Error("Expected non-empty error string")
	}
	
	if !contains(errorStr, "field1") || !contains(errorStr, "field2") {
		t.Error("Expected error string to contain field names")
	}
}

// Helper function
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}