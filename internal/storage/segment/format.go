package segment

import (
    "encoding/binary"
    "errors"
    "fmt"
    "hash/crc32"
    "io"
    "encoding/json"
    "math"
    "time"

	"vexdb/internal/types"
)

const (
    // FileSegmentMagic aliases the global segment magic to avoid redeclarations
    FileSegmentMagic = SegmentMagic
    // FileSegmentVersion aliases the global segment version
    FileSegmentVersion = SegmentVersion
    
    // HeaderSize is the size of the segment header in bytes
    HeaderSize = 64
	
	// FooterSize is the size of the segment footer in bytes
	FooterSize = 32
	
	// IndexEntrySize is the size of each index entry in bytes
	IndexEntrySize = 32
	
	// DefaultMaxVectorsPerSegment is the default maximum number of vectors per segment
	DefaultMaxVectorsPerSegment = 10000
	
	// DefaultCompressionLevel is the default compression level
	DefaultCompressionLevel = 1
)

var (
    ErrInvalidMagic      = errors.New("invalid segment magic number")
    ErrInvalidVersion    = errors.New("unsupported segment version")
    ErrCorruptedData     = errors.New("segment data is corrupted")
    ErrSegmentFull       = errors.New("segment is full")
    FileErrVectorNotFound    = errors.New("vector not found")
    FileErrInvalidOffset     = errors.New("invalid vector offset")
    ErrChecksumMismatch  = errors.New("checksum mismatch")
)

// Header represents the segment file header
type FileHeader struct {
    Magic        uint32
    Version      uint32
    CreatedAt    int64
	ModifiedAt   int64
	VectorCount  uint32
	VectorDim    uint32
	ClusterID    uint32
	MaxVectors   uint32
	Flags        uint32
	Reserved     [32]byte
	Checksum     uint32
}

// Footer represents the segment file footer
type FileFooter struct {
    IndexOffset  uint64
    IndexSize    uint64
    DataOffset   uint64
	DataSize     uint64
	MetadataSize uint32
	Flags        uint32
	Checksum     uint32
}

// IndexEntry represents an entry in the segment index
type FileIndexEntry struct {
    VectorID     uint64
    Offset       uint64
    Size         uint32
	ClusterID    uint32
	Checksum     uint32
	Reserved     [12]byte
}

// VectorHeader represents the header for each vector in the segment
type FileVectorHeader struct {
    VectorID     uint64
    Timestamp    int64
    MetadataSize uint32
	VectorSize   uint32
	Flags        uint32
	Checksum     uint32
}

// Segment represents a segment file
type FileSegment struct {
    header       *FileHeader
    footer       *FileFooter
    index        []*FileIndexEntry
    vectors      map[uint64]*types.Vector
    metadata     map[uint64][]byte
    path         string
	maxVectors   int
	compression  int
	isReadOnly   bool
	dirty        bool
}

// NewSegment creates a new segment
func NewFileSegment(path string, vectorDim uint32, clusterID uint32, maxVectors int) *FileSegment {
	now := time.Now().Unix()
	
    return &FileSegment{
        header: &FileHeader{
            Magic:       FileSegmentMagic,
            Version:     FileSegmentVersion,
			CreatedAt:   now,
			ModifiedAt:  now,
			VectorDim:   vectorDim,
			ClusterID:   clusterID,
			MaxVectors:  uint32(maxVectors),
		},
        footer:      &FileFooter{},
        index:       make([]*FileIndexEntry, 0, maxVectors),
        vectors:     make(map[uint64]*types.Vector),
        metadata:    make(map[uint64][]byte),
		path:        path,
		maxVectors:  maxVectors,
		compression: DefaultCompressionLevel,
		isReadOnly:  false,
		dirty:       true,
	}
}

// LoadSegment loads an existing segment from file
func LoadFileSegment(path string) (*FileSegment, error) {
	// This would be implemented to read from disk
	// For now, return a new segment
    return NewFileSegment(path, 128, 0, DefaultMaxVectorsPerSegment), nil
}

// AddVector adds a vector to the segment
func (s *FileSegment) AddVector(vector *types.Vector) error {
    if s.isReadOnly {
        return ErrSegmentFull
    }
	
	if len(s.vectors) >= s.maxVectors {
		return ErrSegmentFull
	}
	
    key := idToU64(vector.ID)
    if _, exists := s.vectors[key]; exists {
        return fmt.Errorf("vector with ID '%s' already exists", vector.ID)
    }
	
    s.vectors[key] = vector
    if vector.Metadata != nil {
        s.metadata[key] = encodeMetadata(vector.Metadata)
    }
	
	s.header.VectorCount++
	s.header.ModifiedAt = time.Now().Unix()
	s.dirty = true
	
	return nil
}

// GetVector retrieves a vector from the segment
func (s *FileSegment) GetVectorByKey(key uint64) (*types.Vector, error) {
    vector, exists := s.vectors[key]
    if !exists {
        return nil, FileErrVectorNotFound
    }
    return vector, nil
}

// RemoveVector removes a vector from the segment
func (s *FileSegment) RemoveVector(vectorID uint64) error {
	if s.isReadOnly {
		return ErrSegmentFull
	}
	
    if _, exists := s.vectors[vectorID]; !exists {
        return ErrVectorNotFound
    }
	
	delete(s.vectors, vectorID)
	delete(s.metadata, vectorID)
	
	// Remove from index
	for i, entry := range s.index {
		if entry.VectorID == vectorID {
			s.index = append(s.index[:i], s.index[i+1:]...)
			break
		}
	}
	
	s.header.VectorCount--
	s.header.ModifiedAt = time.Now().Unix()
	s.dirty = true
	
	return nil
}

// GetVectors returns all vectors in the segment
func (s *FileSegment) GetVectors() []*types.Vector {
	vectors := make([]*types.Vector, 0, len(s.vectors))
	for _, vector := range s.vectors {
		vectors = append(vectors, vector)
	}
	return vectors
}

// GetVectorCount returns the number of vectors in the segment
func (s *FileSegment) GetVectorCount() int {
	return len(s.vectors)
}

// IsFull returns true if the segment is full
func (s *FileSegment) IsFull() bool {
	return len(s.vectors) >= s.maxVectors
}

// SetReadOnly sets the segment to read-only mode
func (s *FileSegment) SetReadOnly() {
	s.isReadOnly = true
}

// IsReadOnly returns true if the segment is read-only
func (s *FileSegment) IsReadOnly() bool {
	return s.isReadOnly
}

// IsDirty returns true if the segment has unsaved changes
func (s *FileSegment) IsDirty() bool {
	return s.dirty
}

// Serialize serializes the segment to bytes
func (s *FileSegment) Serialize() ([]byte, error) {
	// Create buffer
	buf := make([]byte, 0)
	
	// Serialize header
	headerBytes, err := s.serializeHeader()
	if err != nil {
		return nil, err
	}
	buf = append(buf, headerBytes...)
	
	// Serialize data
	dataBytes, err := s.serializeData()
	if err != nil {
		return nil, err
	}
	buf = append(buf, dataBytes...)
	
	// Serialize index
	indexBytes, err := s.serializeIndex()
	if err != nil {
		return nil, err
	}
	buf = append(buf, indexBytes...)
	
	// Serialize footer
	footerBytes, err := s.serializeFooter()
	if err != nil {
		return nil, err
	}
	buf = append(buf, footerBytes...)
	
	return buf, nil
}

// Deserialize deserializes a segment from bytes
func (s *FileSegment) Deserialize(data []byte) error {
	if len(data) < HeaderSize+FooterSize {
		return ErrCorruptedData
	}
	
	// Deserialize header
	if err := s.deserializeHeader(data[:HeaderSize]); err != nil {
		return err
	}
	
	// Deserialize footer
	footerStart := len(data) - FooterSize
	if err := s.deserializeFooter(data[footerStart:]); err != nil {
		return err
	}
	
	// Deserialize data
	dataStart := HeaderSize
	dataEnd := int(s.footer.DataOffset + s.footer.DataSize)
	if dataEnd > len(data) {
		return ErrCorruptedData
	}
	if err := s.deserializeData(data[dataStart:dataEnd]); err != nil {
		return err
	}
	
	// Deserialize index
	indexStart := int(s.footer.IndexOffset)
	indexEnd := indexStart + int(s.footer.IndexSize)
	if indexEnd > len(data) {
		return ErrCorruptedData
	}
	if err := s.deserializeIndex(data[indexStart:indexEnd]); err != nil {
		return err
	}
	
	s.dirty = false
	return nil
}

// serializeHeader serializes the segment header
func (s *FileSegment) serializeHeader() ([]byte, error) {
	buf := make([]byte, HeaderSize)
	
	binary.LittleEndian.PutUint32(buf[0:4], s.header.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], s.header.Version)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(s.header.CreatedAt))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(s.header.ModifiedAt))
	binary.LittleEndian.PutUint32(buf[24:28], s.header.VectorCount)
	binary.LittleEndian.PutUint32(buf[28:32], s.header.VectorDim)
	binary.LittleEndian.PutUint32(buf[32:36], s.header.ClusterID)
	binary.LittleEndian.PutUint32(buf[36:40], s.header.MaxVectors)
	binary.LittleEndian.PutUint32(buf[40:44], s.header.Flags)
	
	// Calculate checksum
	checksum := crc32.ChecksumIEEE(buf[:44])
	binary.LittleEndian.PutUint32(buf[60:64], checksum)
	
	return buf, nil
}

// deserializeHeader deserializes the segment header
func (s *FileSegment) deserializeHeader(data []byte) error {
	if len(data) != HeaderSize {
		return ErrCorruptedData
	}
	
	s.header.Magic = binary.LittleEndian.Uint32(data[0:4])
	s.header.Version = binary.LittleEndian.Uint32(data[4:8])
	s.header.CreatedAt = int64(binary.LittleEndian.Uint64(data[8:16]))
	s.header.ModifiedAt = int64(binary.LittleEndian.Uint64(data[16:24]))
	s.header.VectorCount = binary.LittleEndian.Uint32(data[24:28])
	s.header.VectorDim = binary.LittleEndian.Uint32(data[28:32])
	s.header.ClusterID = binary.LittleEndian.Uint32(data[32:36])
	s.header.MaxVectors = binary.LittleEndian.Uint32(data[36:40])
	s.header.Flags = binary.LittleEndian.Uint32(data[40:44])
	
	// Verify checksum
	checksum := crc32.ChecksumIEEE(data[:44])
	if checksum != binary.LittleEndian.Uint32(data[60:64]) {
		return ErrChecksumMismatch
	}
	
	// Verify magic and version
	if s.header.Magic != SegmentMagic {
		return ErrInvalidMagic
	}
	if s.header.Version != SegmentVersion {
		return ErrInvalidVersion
	}
	
	return nil
}

// serializeData serializes the segment data
func (s *FileSegment) serializeData() ([]byte, error) {
	buf := make([]byte, 0)
	
	for _, vector := range s.vectors {
		// Serialize vector header
            vectorHeader := &FileVectorHeader{
                VectorID:     idToU64(vector.ID),
                Timestamp:    vector.Timestamp,
                VectorSize:   uint32(len(vector.Data) * 4), // float32 = 4 bytes
                Flags:        0,
            }
		
            key := idToU64(vector.ID)
            if metadata, exists := s.metadata[key]; exists {
                vectorHeader.MetadataSize = uint32(len(metadata))
            }
		
		headerBytes := make([]byte, 24)
		binary.LittleEndian.PutUint64(headerBytes[0:8], vectorHeader.VectorID)
		binary.LittleEndian.PutUint64(headerBytes[8:16], uint64(vectorHeader.Timestamp))
		binary.LittleEndian.PutUint32(headerBytes[16:20], vectorHeader.MetadataSize)
		binary.LittleEndian.PutUint32(headerBytes[20:24], vectorHeader.VectorSize)
		
		// Calculate checksum
		checksum := crc32.ChecksumIEEE(headerBytes[:20])
		binary.LittleEndian.PutUint32(headerBytes[20:24], checksum)
		
		buf = append(buf, headerBytes...)
		
		// Serialize metadata
            if vectorHeader.MetadataSize > 0 {
                buf = append(buf, s.metadata[key]...)
            }
		
		// Serialize vector data
		vectorBytes := make([]byte, len(vector.Data)*4)
		for i, val := range vector.Data {
			binary.LittleEndian.PutUint32(vectorBytes[i*4:(i+1)*4], math.Float32bits(val))
		}
		buf = append(buf, vectorBytes...)
	}
	
	return buf, nil
}

// deserializeData deserializes the segment data
func (s *FileSegment) deserializeData(data []byte) error {
	offset := 0
	
	for offset < len(data) {
		if offset+24 > len(data) {
			return ErrCorruptedData
		}
		
		// Read vector header
        vectorHeader := &FileVectorHeader{}
		vectorHeader.VectorID = binary.LittleEndian.Uint64(data[offset:offset+8])
		vectorHeader.Timestamp = int64(binary.LittleEndian.Uint64(data[offset+8:offset+16]))
		vectorHeader.MetadataSize = binary.LittleEndian.Uint32(data[offset+16:offset+20])
		vectorHeader.VectorSize = binary.LittleEndian.Uint32(data[offset+20:offset+24])
		
		// Verify checksum
		checksum := crc32.ChecksumIEEE(data[offset:offset+20])
		if checksum != binary.LittleEndian.Uint32(data[offset+20:offset+24]) {
			return ErrChecksumMismatch
		}
		
		offset += 24
		
		// Read metadata
		var metadata []byte
		if vectorHeader.MetadataSize > 0 {
			if offset+int(vectorHeader.MetadataSize) > len(data) {
				return ErrCorruptedData
			}
			metadata = data[offset : offset+int(vectorHeader.MetadataSize)]
			offset += int(vectorHeader.MetadataSize)
		}
		
		// Read vector data
		if offset+int(vectorHeader.VectorSize) > len(data) {
			return ErrCorruptedData
		}
		vectorData := make([]float32, vectorHeader.VectorSize/4)
		for i := 0; i < len(vectorData); i++ {
			vectorData[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset+i*4 : offset+(i+1)*4]))
		}
		offset += int(vectorHeader.VectorSize)
		
		// Create vector
        vector := &types.Vector{
            ID:        fmt.Sprintf("%d", vectorHeader.VectorID),
            Data:      vectorData,
            Timestamp: vectorHeader.Timestamp,
        }
		
            if len(metadata) > 0 {
                var md map[string]interface{}
                if err := json.Unmarshal(metadata, &md); err == nil {
                    vector.Metadata = md
                }
            }
		
        key := vectorHeader.VectorID
        s.vectors[key] = vector
        if len(metadata) > 0 {
            s.metadata[key] = metadata
        }
	}
	
	return nil
}

// serializeIndex serializes the segment index
func (s *FileSegment) serializeIndex() ([]byte, error) {
	buf := make([]byte, len(s.index)*IndexEntrySize)
	
    for i, entry := range s.index {
		offset := i * IndexEntrySize
		binary.LittleEndian.PutUint64(buf[offset:offset+8], entry.VectorID)
		binary.LittleEndian.PutUint64(buf[offset+8:offset+16], entry.Offset)
		binary.LittleEndian.PutUint32(buf[offset+16:offset+20], entry.Size)
		binary.LittleEndian.PutUint32(buf[offset+20:offset+24], entry.ClusterID)
		binary.LittleEndian.PutUint32(buf[offset+24:offset+28], entry.Checksum)
	}
	
	return buf, nil
}

// deserializeIndex deserializes the segment index
func (s *FileSegment) deserializeIndex(data []byte) error {
	if len(data)%IndexEntrySize != 0 {
		return ErrCorruptedData
	}
	
    s.index = make([]*FileIndexEntry, 0, len(data)/IndexEntrySize)
	
	for i := 0; i < len(data); i += IndexEntrySize {
        entry := &FileIndexEntry{}
		entry.VectorID = binary.LittleEndian.Uint64(data[i:i+8])
		entry.Offset = binary.LittleEndian.Uint64(data[i+8:i+16])
		entry.Size = binary.LittleEndian.Uint32(data[i+16:i+20])
		entry.ClusterID = binary.LittleEndian.Uint32(data[i+20:i+24])
		entry.Checksum = binary.LittleEndian.Uint32(data[i+24:i+28])
		
        s.index = append(s.index, entry)
	}
	
	return nil
}

// serializeFooter serializes the segment footer
func (s *FileSegment) serializeFooter() ([]byte, error) {
	buf := make([]byte, FooterSize)
	
	binary.LittleEndian.PutUint64(buf[0:8], s.footer.IndexOffset)
	binary.LittleEndian.PutUint64(buf[8:16], s.footer.IndexSize)
	binary.LittleEndian.PutUint64(buf[16:24], s.footer.DataOffset)
	binary.LittleEndian.PutUint64(buf[24:32], s.footer.DataSize)
	binary.LittleEndian.PutUint32(buf[32:36], s.footer.MetadataSize)
	binary.LittleEndian.PutUint32(buf[36:40], s.footer.Flags)
	
	// Calculate checksum
	checksum := crc32.ChecksumIEEE(buf[:36])
	binary.LittleEndian.PutUint32(buf[28:32], checksum)
	
	return buf, nil
}

// deserializeFooter deserializes the segment footer
func (s *FileSegment) deserializeFooter(data []byte) error {
	if len(data) != FooterSize {
		return ErrCorruptedData
	}
	
	s.footer.IndexOffset = binary.LittleEndian.Uint64(data[0:8])
	s.footer.IndexSize = binary.LittleEndian.Uint64(data[8:16])
	s.footer.DataOffset = binary.LittleEndian.Uint64(data[16:24])
	s.footer.DataSize = binary.LittleEndian.Uint64(data[24:32])
	s.footer.MetadataSize = binary.LittleEndian.Uint32(data[32:36])
	s.footer.Flags = binary.LittleEndian.Uint32(data[36:40])
	
	// Verify checksum
	checksum := crc32.ChecksumIEEE(data[:36])
	if checksum != binary.LittleEndian.Uint32(data[28:32]) {
		return ErrChecksumMismatch
	}
	
	return nil
}

// WriteTo writes the segment to an io.Writer
func (s *FileSegment) WriteTo(w io.Writer) error {
	data, err := s.Serialize()
	if err != nil {
		return err
	}
	
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	
	s.dirty = false
	return nil
}

// ReadFrom reads the segment from an io.Reader
func (s *FileSegment) ReadFrom(r io.Reader) error {
	// Read the entire segment data
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	
	return s.Deserialize(data)
}

// GetSize returns the size of the segment in bytes
func (s *FileSegment) GetSize() int {
	size := HeaderSize + FooterSize
	
	// Calculate data size
    for id, vector := range s.vectors {
        size += 24 // Vector header size
        if metadata, exists := s.metadata[id]; exists {
            size += len(metadata)
        }
        size += len(vector.Data) * 4 // float32 = 4 bytes
    }
	
	// Calculate index size
	size += len(s.index) * IndexEntrySize
	
	return size
}

// GetClusterID returns the cluster ID of the segment
func (s *FileSegment) GetClusterID() uint32 {
	return s.header.ClusterID
}

// GetVectorDim returns the vector dimension of the segment
func (s *FileSegment) GetVectorDim() uint32 {
	return s.header.VectorDim
}

// GetCreatedAt returns the creation time of the segment
func (s *FileSegment) GetCreatedAt() time.Time {
	return time.Unix(s.header.CreatedAt, 0)
}

// GetModifiedAt returns the last modification time of the segment
func (s *FileSegment) GetModifiedAt() time.Time {
	return time.Unix(s.header.ModifiedAt, 0)
}

// Compact compacts the segment by removing deleted vectors
func (s *FileSegment) Compact() error {
	if s.isReadOnly {
		return ErrSegmentFull
	}
	
	// Create new vectors and metadata maps
	newVectors := make(map[uint64]*types.Vector)
	newMetadata := make(map[uint64][]byte)
	
	// Copy existing vectors
	for id, vector := range s.vectors {
		newVectors[id] = vector
		if metadata, exists := s.metadata[id]; exists {
			newMetadata[id] = metadata
		}
	}
	
	s.vectors = newVectors
	s.metadata = newMetadata
	s.header.VectorCount = uint32(len(s.vectors))
	s.header.ModifiedAt = time.Now().Unix()
	s.dirty = true
	
	return nil
}

// Validate validates the segment integrity
func (s *FileSegment) Validate() error {
	// Validate header
	if s.header.Magic != SegmentMagic {
		return ErrInvalidMagic
	}
	if s.header.Version != SegmentVersion {
		return ErrInvalidVersion
	}
	
	// Validate vector count
	if uint32(len(s.vectors)) != s.header.VectorCount {
		return ErrCorruptedData
	}
	
    // Validate vectors
    for _, vector := range s.vectors {
        if len(vector.Data) != int(s.header.VectorDim) {
            return ErrCorruptedData
        }
    }
	
	return nil
}
