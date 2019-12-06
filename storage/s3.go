package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

// S3Backup is the structure that implements the backup storage for the S3 backend
type S3Backup struct {
	BackupIface
	manifest Manifest
	client   s3iface.S3API
	uploader s3manageriface.UploaderAPI
	DataPipe chan map[string]*dynamodb.AttributeValue
}

// NewS3Backup initlialiaes the s3 client and returns a pointer to a S3Backup struct
func NewS3Backup(sess client.ConfigProvider) *S3Backup {
	return &S3Backup{client: s3.New(sess), uploader: s3manager.NewUploader(sess)}
}

// LoadManifest downloads the given manifest file and load it in the
// Manifest attribute of the struct
func (h *S3Backup) LoadManifest(input *FileInput) error {
	doc, err := h.GetFile(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.Printf("[ERROR] Unable to find a manifest flag in the provided folder. Are you sure the backup was successful?\nAborting...\n")
		}
		log.Printf("[ERROR] Unable to retrieve the manifest flag information: %s\nAborting...\n", err)
		return err
	}
	defer Close(*doc)
	buff := bytes.NewBuffer(nil)
	if _, err := io.Copy(buff, *doc); err != nil {
		return err
	}

	return json.Unmarshal(buff.Bytes(), &h.manifest)
}

// GetFile downloads a file from s3 to memory (as the files are small by
// default - just a few Mb).
func (h *S3Backup) GetFile(input *FileInput) (*io.ReadCloser, error) {
	s3Input := &s3.GetObjectInput{
		Bucket: input.Bucket,
		Key:    input.Path,
	}

	results, err := h.client.GetObject(s3Input)
	if err != nil {
		return nil, err
	}
	return &results.Body, nil
}

// Exists checks that a given path in s3 exists as a file
func (h *S3Backup) Exists(input *FileInput) (bool, error) {
	s3Input := &s3.HeadObjectInput{
		Bucket: input.Bucket,
		Key:    input.Path,
	}

	_, err := h.client.HeadObject(s3Input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Flush writes the content of a bytes array to the given s3 path
func (h *S3Backup) Flush(input *FileInput, data []byte) error {
	upParams := &s3manager.UploadInput{
		Bucket:               input.Bucket,
		Key:                  input.Path,
		Body:                 bytes.NewReader(data),
		StorageClass:         aws.String("STANDARD_IA"),
		ServerSideEncryption: aws.String("AES256"),
		// Expire: ...,
		// Tagging:
	}
	// Set file name and content before upload
	log.Printf("Writing file: s3://%s/%s\n", *upParams.Bucket, *upParams.Key)
	_, err := h.uploader.Upload(upParams)
	return err
}

// Scan reads the data from a backup line by line, serializes it and
// sends it to the struct's channel
func (h *S3Backup) Scan(dataReader *io.ReadCloser) error {
	defer Close(*dataReader)
	scanner := bufio.NewScanner(*dataReader)
	for scanner.Scan() {
		res := map[string]*dynamodb.AttributeValue{}
		data := scanner.Bytes()
		if err := json.Unmarshal(data[:], &res); err != nil {
			log.Printf("[Error] unmashaling %v: %s", data, err)
		} else {
			h.DataPipe <- res
		}
	}
	return scanner.Err()
}

// WriteToDB pulls the s3 files from S3Backup.manifest and import them
// inside the given table using the given batch size (and wait period between
// each batch)
func (h *S3Backup) WriteToDB(tableName string, batchSize int64, waitPeriod time.Duration, wg *sync.WaitGroup) error {
	wg.Add(1)
	for _, entry := range h.manifest.Entries {
		u, _ := url.Parse(entry.URL)
		if u.Scheme == "s3" {
			data, err := h.GetFile(&FileInput{Bucket: &u.Host, Path: &u.Path})
			if err != nil {
				return err
			}
			if err = h.Scan(data); err != nil {
				return err
			}
		}

	}
	close(h.DataPipe)
	return nil
}

// DumpBuffer dumps the content of the given buffer to a new randomly generated
// file name in the given s3 path in the given bucket and resets the said buffer
func (h *S3Backup) DumpBuffer(input *FileInput, buff *bytes.Buffer) {
	*input.Path = fmt.Sprintf("%s/%s", *input.Path, genNewFileName())
	if err := h.Flush(input, buff.Bytes()); err != nil {
		log.Printf("[ERROR] while writing the file %s: %s", *input.Path, err)
	}
	h.manifest.Entries = append(h.manifest.Entries, ManifestEntry{URL: fmt.Sprintf("s3://%s/%s", *input.Bucket, *input.Path), Mandatory: true})
	buff.Reset()
}

// Write reads from the given channel and sends the data the given bucket
// in files of s3BufferSize max size
func (h *S3Backup) Write(input *FileInput, s3BufferSize int, wg *sync.WaitGroup) {
	defer wg.Done()
	// buff is the buffer where the data will be stored while before being sent to s3
	var buff bytes.Buffer
	h.manifest = Manifest{Version: 3, Name: "DynamoDB-export"}
	s3Folder := *input.Path

	for elem := range h.DataPipe {
		data, err := MarshalDynamoAttributeMap(elem)
		if err != nil {
			log.Fatalf("[ERROR] while converting to json: %v\nError: %s\n", elem, err)
		}

		// before overflowing the buffer, dump to s3 and empty it
		if buff.Len()+len(data) >= s3BufferSize && buff.Len() > 0 {
			h.DumpBuffer(input, &buff)
		}
		// add the data to the buffer
		buff.Write(data)
		buff.WriteString("\n")
	}

	// Upload the rest of the buffer
	h.DumpBuffer(input, &buff)
	// Wrap up the manifest of the backup files
	manifestData, err := json.Marshal(h.manifest)
	if err != nil {
		log.Fatalf("[ERROR] while marshaling the manifest: %v\nError: %s\n", h.manifest, err)
	}
	m := FileInput{Bucket: input.Bucket, Path: aws.String(fmt.Sprintf("%s/manifest", s3Folder))}
	if err = h.Flush(&m, manifestData); err != nil {
		log.Printf("[ERROR] while writing the manifest file: %s", err)
	}
	// Signal the success of the backup
	s := FileInput{Bucket: input.Bucket, Path: aws.String(fmt.Sprintf("%s/_SUCCESS", s3Folder))}
	if err = h.Flush(&s, []byte{}); err != nil {
		log.Printf("[ERROR] while writing the _SUCCESS file: %s", err)
	}
}
