package fulltext

import "encoding/json"
import "fmt"

var ErrFormatVersionMismatch = fmt.Errorf("fulltext_format_version_mismatch")

// Serialize serializes to JSON
func (idx *Index) Serialize() ([]byte, error) {
	return json.Marshal(idx.private)
}

// Deserialize deserializes from JSON
func (idx *Index) Deserialize(data []byte) error {
	err := json.Unmarshal(data, &(idx.private))
	if err != nil {
		return err
	}
	for _, p := range idx.private {
		if p.Version == 0 || p.Version > 2 {
			return ErrFormatVersionMismatch
		}
	}
	return nil
}
