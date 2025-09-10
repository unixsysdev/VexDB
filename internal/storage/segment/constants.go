package segment

import (
	"fmt"
	"vxdb/internal/types"
)

// Segment magic number and version
const (
	SegmentMagic   = 0x56455844 // "VXD" in hex
	SegmentVersion = 1
)

// SegmentHeader represents the header of a segment file
type SegmentHeader struct {
	Magic      uint32          `json:"magic" yaml:"magic"`
	Version    uint32          `json:"version" yaml:"version"`
	ClusterID  uint32          `json:"cluster_id" yaml:"cluster_id"`
	SegmentID  string          `json:"segment_id" yaml:"segment_id"`
	Status     string          `json:"status" yaml:"status"`
	VectorSize uint32          `json:"vector_size" yaml:"vector_size"`
	CreatedAt  types.Timestamp `json:"created_at" yaml:"created_at"`
	UpdatedAt  types.Timestamp `json:"updated_at" yaml:"updated_at"`
}

// SegmentStatus represents the status of a segment
type SegmentStatus string

const (
	SegmentStatusActive     SegmentStatus = "active"
	SegmentStatusSealed     SegmentStatus = "sealed"
	SegmentStatusCompacting SegmentStatus = "compacting"
	SegmentStatusDeleted    SegmentStatus = "deleted"
)

// SegmentInfo represents information about a segment
type SegmentInfo struct {
	ID          string          `json:"id" yaml:"id"`
	ClusterID   uint32          `json:"cluster_id" yaml:"cluster_id"`
	Status      SegmentStatus   `json:"status" yaml:"status"`
	VectorCount uint64          `json:"vector_count" yaml:"vector_count"`
	CreatedAt   types.Timestamp `json:"created_at" yaml:"created_at"`
	UpdatedAt   types.Timestamp `json:"updated_at" yaml:"updated_at"`
}

// ClusterIDString returns a string representation of the cluster ID
func (si *SegmentInfo) ClusterIDString() string {
	return fmt.Sprintf("%d", si.ClusterID)
}

// IsValid checks if the segment info is valid
func (si *SegmentInfo) IsValid() bool {
	if si.ID == "" {
		return false
	}
	if si.ClusterID == 0 {
		return false
	}
	if si.Status == "" {
		return false
	}
	if si.CreatedAt == 0 {
		return false
	}
	if si.UpdatedAt == 0 {
		return false
	}
	return true
}

// IsActive checks if the segment is active
func (si *SegmentInfo) IsActive() bool {
	return si.Status == SegmentStatusActive
}

// IsSealed checks if the segment is sealed
func (si *SegmentInfo) IsSealed() bool {
	return si.Status == SegmentStatusSealed
}

// IsCompacting checks if the segment is compacting
func (si *SegmentInfo) IsCompacting() bool {
	return si.Status == SegmentStatusCompacting
}

// IsDeleted checks if the segment is deleted
func (si *SegmentInfo) IsDeleted() bool {
	return si.Status == SegmentStatusDeleted
}

// NewSegmentInfo creates a new segment info
func NewSegmentInfo(id string, clusterID uint32) *SegmentInfo {
	now := types.NowTimestamp()
	return &SegmentInfo{
		ID:          id,
		ClusterID:   clusterID,
		Status:      SegmentStatusActive,
		VectorCount: 0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// IsValidSegmentStatus checks if the segment status is valid
func IsValidSegmentStatus(status string) bool {
	switch SegmentStatus(status) {
	case SegmentStatusActive, SegmentStatusSealed, SegmentStatusCompacting, SegmentStatusDeleted:
		return true
	default:
		return false
	}
}

// SegmentStatusFromString converts a string to a segment status
func SegmentStatusFromString(status string) SegmentStatus {
	switch status {
	case "active":
		return SegmentStatusActive
	case "sealed":
		return SegmentStatusSealed
	case "compacting":
		return SegmentStatusCompacting
	case "deleted":
		return SegmentStatusDeleted
	default:
		return SegmentStatusActive
	}
}

// String returns the string representation of the segment status
func (ss SegmentStatus) String() string {
	return string(ss)
}
