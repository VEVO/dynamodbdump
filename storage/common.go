package storage

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"

	"github.com/segmentio/ksuid"
)

// ManifestEntry represents an entry in the backup manifest
type ManifestEntry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

// Manifest represents the backup manifest
type Manifest struct {
	Name    string          `json:"name"`
	Version int             `json:"version"`
	Entries []ManifestEntry `json:"entries"`
}

// FileInput is used as input for the functions that require a file definition,
// be it a s3 file or a file on disk.
// Bucket will be used only for S3-related functions.
// Path will be the Key in the context of S3 functions
type FileInput struct {
	Bucket, Path *string
}

// genNewFileName returns a UUID used by the datapipelines
func genNewFileName() string {
	uuID := hex.EncodeToString(ksuid.New().Payload())
	return fmt.Sprintf("%s-%s-%s-%s-%s", uuID[:8], uuID[8:12], uuID[12:16], uuID[16:20], uuID[20:])
}

// Close logs errors on io.Closer if any should happen during a Close call
func Close(r io.Closer) {
	if err := r.Close(); err != nil {
		log.Printf("[ERROR] while closing %+v: %s", r, err)
	}
}
