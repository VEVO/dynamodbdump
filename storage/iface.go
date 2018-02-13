package storage

// It will hold the structures and interfaces related to the storage of the
// backups of the dynamodbdump application

import (
	"bytes"
	"io"
	"sync"
	"time"
)

// BackupIface is the interface that each storage backends implement
type BackupIface interface {
	LoadManifest(*FileInput) error
	GetFile(*FileInput) error
	Exists(input *FileInput) (bool, error)
	Flush(input *FileInput, data []byte) error
	Scan(*io.ReadCloser) error
	WriteToDB(string, int64, time.Duration, *sync.WaitGroup) error
	DumpBuffer(*FileInput, *bytes.Buffer)
	Write(*FileInput, int, *sync.WaitGroup)
}
