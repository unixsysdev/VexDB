package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Vector represents a high-dimensional vector with metadata
type Vector struct {
	ID        string                 `json:"id" yaml:"id"`
	Data      []float32              `json:"data" yaml:"data"`
	Metadata  map[string]interface{} `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	ClusterID uint32                `json:"cluster_id" yaml:"cluster_id"`
	Timestamp int64                 `json:"timestamp" yaml:"timestamp"`
}

// VectorConfig defines configuration for vector operations
type VectorConfig struct {
	Dimensions    int     `json:"dimensions" yaml:"dimensions"`
	MaxDimensions int     `json:"max_dimensions" yaml:"max_dimensions"`
	MinDimensions int     `json:"min_dimensions" yaml:"min_dimensions"`
	Normalize     bool    `json:"normalize" yaml:"normalize"`
	DistanceType  string  `json:"distance_type" yaml:"distance_type"` // "cosine", "euclidean", "dotproduct"
}

// NewVector creates a new vector with validation
func NewVector(id string, data []float32, metadata map[string]interface{}) (*Vector, error) {
	if id == "" {
		return nil, errors.New("vector ID cannot be empty")
	}
	
	if len(data) == 0 {
		return nil, errors.New("vector data cannot be empty")
	}
	
	if len(data) > 10000 { // Reasonable upper limit
		return nil, fmt.Errorf("vector dimensions %d exceeds maximum allowed", len(data))
	}
	
	// Validate vector data
	for i, val := range data {
		if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
			return nil, fmt.Errorf("vector data contains invalid value at index %d: %f", i, val)
		}
	}
	
	vector := &Vector{
		ID:        id,
		Data:      make([]float32, len(data)),
		Metadata:  metadata,
		Timestamp: 0, // Will be set by the system
	}
	
	// Copy data to avoid external modification
	copy(vector.Data, data)
	
	return vector, nil
}

// Validate validates the vector data
func (v *Vector) Validate(config *VectorConfig) error {
	if v == nil {
		return errors.New("vector cannot be nil")
	}
	
	if len(v.Data) == 0 {
		return errors.New("vector data cannot be empty")
	}
	
	if config != nil {
		if len(v.Data) < config.MinDimensions {
			return fmt.Errorf("vector dimensions %d below minimum required %d", len(v.Data), config.MinDimensions)
		}
		
		if len(v.Data) > config.MaxDimensions {
			return fmt.Errorf("vector dimensions %d exceeds maximum allowed %d", len(v.Data), config.MaxDimensions)
		}
	}
	
	// Validate vector data
	for i, val := range v.Data {
		if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
			return fmt.Errorf("vector data contains invalid value at index %d: %f", i, val)
		}
	}
	
	return nil
}

// Normalize normalizes the vector to unit length
func (v *Vector) Normalize() error {
	if len(v.Data) == 0 {
		return errors.New("cannot normalize empty vector")
	}
	
	var norm float64
	for _, val := range v.Data {
		norm += float64(val) * float64(val)
	}
	
	norm = math.Sqrt(norm)
	if norm == 0 {
		return errors.New("cannot normalize zero vector")
	}
	
	for i := range v.Data {
		v.Data[i] = float32(float64(v.Data[i]) / norm)
	}
	
	return nil
}

// Distance calculates distance between two vectors
func (v *Vector) Distance(other *Vector, distanceType string) (float64, error) {
	if len(v.Data) != len(other.Data) {
		return 0, fmt.Errorf("vector dimensions mismatch: %d vs %d", len(v.Data), len(other.Data))
	}
	
	switch distanceType {
	case "cosine":
		return v.cosineDistance(other), nil
	case "euclidean":
		return v.euclideanDistance(other), nil
	case "dotproduct":
		return v.dotProduct(other), nil
	default:
		return 0, fmt.Errorf("unsupported distance type: %s", distanceType)
	}
}

// cosineDistance calculates cosine distance between two vectors
func (v *Vector) cosineDistance(other *Vector) float64 {
	dotProduct := v.dotProduct(other)
	norm1 := v.magnitude()
	norm2 := other.magnitude()
	
	if norm1 == 0 || norm2 == 0 {
		return 1.0 // Maximum distance for zero vectors
	}
	
	return 1.0 - (dotProduct / (norm1 * norm2))
}

// euclideanDistance calculates Euclidean distance between two vectors
func (v *Vector) euclideanDistance(other *Vector) float64 {
	var sum float64
	for i := range v.Data {
		diff := float64(v.Data[i]) - float64(other.Data[i])
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

// dotProduct calculates dot product between two vectors
func (v *Vector) dotProduct(other *Vector) float64 {
	var sum float64
	for i := range v.Data {
		sum += float64(v.Data[i]) * float64(other.Data[i])
	}
	return sum
}

// magnitude calculates the magnitude (L2 norm) of the vector
func (v *Vector) magnitude() float64 {
	var sum float64
	for _, val := range v.Data {
		sum += float64(val) * float64(val)
	}
	return math.Sqrt(sum)
}

// Size returns the size of vector in bytes for serialization
func (v *Vector) Size() int {
	// ID (4 bytes len + len(ID)) + Data (4 bytes len + len(Data)*4) + 
	// Metadata (variable) + ClusterID (4) + Timestamp (8)
	return 4 + len(v.ID) + 4 + len(v.Data)*4 + v.metadataSize() + 4 + 8
}

// metadataSize returns the size of metadata in bytes (simplified)
func (v *Vector) metadataSize() int {
	if v.Metadata == nil {
		return 4 // 4 bytes for length
	}
	// Simplified calculation - in practice, this would be more complex
	return 4 + len(v.Metadata)*16 // Approximate
}

// Serialize converts vector to byte array for storage
func (v *Vector) Serialize() ([]byte, error) {
	if v == nil {
		return nil, errors.New("cannot serialize nil vector")
	}
	
	// Calculate total size
	size := v.Size()
	buf := make([]byte, 0, size)
	
	// Serialize ID
	idBytes := []byte(v.ID)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(idBytes)))
	buf = append(buf, idBytes...)
	
	// Serialize Data
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(v.Data)))
	for _, val := range v.Data {
		buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(val))
	}
	
	// Serialize Metadata (simplified)
	if v.Metadata == nil {
		buf = binary.BigEndian.AppendUint32(buf, 0)
	} else {
		// In practice, this would serialize metadata properly
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v.Metadata)))
		// Placeholder for metadata serialization
	}
	
	// Serialize ClusterID and Timestamp
	buf = binary.BigEndian.AppendUint32(buf, v.ClusterID)
	buf = binary.BigEndian.AppendUint64(buf, uint64(v.Timestamp))
	
	return buf, nil
}

// Deserialize creates vector from byte array
func Deserialize(data []byte) (*Vector, error) {
	if len(data) < 12 { // Minimum size check
		return nil, errors.New("insufficient data for vector deserialization")
	}
	
	vector := &Vector{
		Metadata: make(map[string]interface{}),
	}
	
	offset := 0
	
	// Deserialize ID
	idLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(idLen) > len(data) {
		return nil, errors.New("invalid ID length in vector data")
	}
	vector.ID = string(data[offset : offset+int(idLen)])
	offset += int(idLen)
	
	// Deserialize Data
	dataLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(dataLen)*4 > len(data) {
		return nil, errors.New("invalid data length in vector data")
	}
	vector.Data = make([]float32, dataLen)
	for i := 0; i < int(dataLen); i++ {
		bits := binary.BigEndian.Uint32(data[offset : offset+4])
		vector.Data[i] = math.Float32frombits(bits)
		offset += 4
	}
	
	// Deserialize Metadata (simplified)
	metadataLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	// Skip metadata for now (would implement proper deserialization in practice)
	offset += int(metadataLen) * 16 // Approximate
	
	// Deserialize ClusterID and Timestamp
	if offset+12 > len(data) {
		return nil, errors.New("insufficient data for cluster ID and timestamp")
	}
	vector.ClusterID = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	vector.Timestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	
	return vector, nil
}