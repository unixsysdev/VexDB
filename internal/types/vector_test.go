package types

import (
	"math"
	"testing"
)

func TestNewVector(t *testing.T) {
	input := []float32{1, 2, 3}
	v, err := NewVector("id", input, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ensure data is copied
	input[0] = 10
	if v.Data[0] != 1 {
		t.Fatalf("expected vector to copy data slice, got %v", v.Data[0])
	}
	if _, err := NewVector("", []float32{1}, nil); err == nil {
		t.Fatal("expected error for empty id")
	}
	if _, err := NewVector("id", []float32{}, nil); err == nil {
		t.Fatal("expected error for empty data")
	}
	if _, err := NewVector("id", []float32{float32(math.NaN())}, nil); err == nil {
		t.Fatal("expected error for NaN value")
	}
}

func TestValidateWithConfig(t *testing.T) {
	v, _ := NewVector("id", []float32{1, 2, 3}, nil)
	cfg := &VectorConfig{MinDimensions: 2, MaxDimensions: 4}
	if err := v.ValidateWithConfig(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cfg.MinDimensions = 5
	if err := v.ValidateWithConfig(cfg); err == nil {
		t.Fatal("expected error for too few dimensions")
	}
	cfg = &VectorConfig{MinDimensions: 1, MaxDimensions: 2}
	if err := v.ValidateWithConfig(cfg); err == nil {
		t.Fatal("expected error for too many dimensions")
	}
	v.Data[1] = float32(math.NaN())
	if err := v.ValidateWithConfig(nil); err == nil {
		t.Fatal("expected error for NaN data")
	}
}

func TestNormalize(t *testing.T) {
	v, _ := NewVector("id", []float32{3, 4}, nil)
	if err := v.Normalize(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if math.Abs(float64(v.Data[0])-0.6) > 1e-6 || math.Abs(float64(v.Data[1])-0.8) > 1e-6 {
		t.Fatalf("unexpected normalization result: %v", v.Data)
	}
	zero, _ := NewVector("id2", []float32{0, 0}, nil)
	if err := zero.Normalize(); err == nil {
		t.Fatal("expected error for zero vector")
	}
}

func TestDistance(t *testing.T) {
	a, _ := NewVector("a", []float32{1, 2}, nil)
	b, _ := NewVector("b", []float32{4, 6}, nil)
	d, err := a.Distance(b, "euclidean")
	if err != nil || math.Abs(d-5) > 1e-6 {
		t.Fatalf("unexpected euclidean distance: %v %v", d, err)
	}
	d, err = a.Distance(b, "dotproduct")
	if err != nil || math.Abs(d-(1*4+2*6)) > 1e-6 {
		t.Fatalf("unexpected dot product: %v %v", d, err)
	}
	// cosine similarity of vectors [1,2] and [4,6]
	d, err = a.Distance(b, "cosine")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cos := (1*4 + 2*6) / (math.Sqrt(1*1+2*2) * math.Sqrt(4*4+6*6))
	expected := 1 - cos
	if math.Abs(d-expected) > 1e-6 {
		t.Fatalf("unexpected cosine distance: %v", d)
	}
	c, _ := NewVector("c", []float32{1}, nil)
	if _, err := a.Distance(c, "euclidean"); err == nil {
		t.Fatal("expected dimension mismatch error")
	}
}

func TestSerializeDeserialize(t *testing.T) {
	v, _ := NewVector("id", []float32{1, 2}, nil)
	v.ClusterID = 5
	v.Timestamp = 42
	data, err := v.Serialize()
	if err != nil {
		t.Fatalf("serialize error: %v", err)
	}
	v2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("deserialize error: %v", err)
	}
	if v2.ID != v.ID || v2.ClusterID != v.ClusterID || v2.Timestamp != v.Timestamp {
		t.Fatalf("round trip mismatch: %+v vs %+v", v2, v)
	}
	if len(v2.Data) != len(v.Data) || v2.Data[0] != v.Data[0] || v2.Data[1] != v.Data[1] {
		t.Fatalf("data mismatch: %v vs %v", v2.Data, v.Data)
	}
}
