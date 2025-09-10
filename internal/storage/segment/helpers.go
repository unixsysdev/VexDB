package segment

import (
	"encoding/json"
	"hash/fnv"
)

// idToU64 provides a stable uint64 key for a string ID
func idToU64(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}

// encodeMetadata serializes metadata map to JSON bytes (best-effort)
func encodeMetadata(m map[string]interface{}) []byte {
	if m == nil {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}
