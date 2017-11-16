package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/segmentio/ksuid"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3ManifestEntry represents an entry in the backup manifest stored in the s3 folder of the backup
type S3ManifestEntry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

// S3Manifest represents the backup manifest stored in the s3 folder of the backup
type S3Manifest struct {
	Name    string            `json:"name"`
	Version int               `json:"version"`
	Entries []S3ManifestEntry `json:"entries"`
}

// genNewFileName returns a UUID used by the datapipelines
func genNewFileName() string {
	uuID := hex.EncodeToString(ksuid.New().Payload())
	return fmt.Sprintf("%s-%s-%s-%s-%s", uuID[:8], uuID[8:12], uuID[12:16], uuID[16:20], uuID[20:])
}

// LoadManifestFromS3 downloads the given manifest file and load it in the
// ManifestS3 attribute of the struct
func (h *AwsHelper) LoadManifestFromS3(bucketName, manifestPath string) error {
	doc, err := h.GetFromS3(bucketName, manifestPath)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.Fatalf("[ERROR] Unable to find a manifest flag in the provided folder. Are you sure the backup was successful?\nAborting...\n")
		}
		log.Fatalf("[ERROR] Unable to retrieve the manifest flag information: %s\nAborting...\n", err)
	}
	defer (*doc).Close()
	buff := bytes.NewBuffer(nil)
	if _, err := io.Copy(buff, *doc); err != nil {
		return err
	}

	return json.Unmarshal(buff.Bytes(), &h.ManifestS3)
}

// GetFromS3 download a file from s3 to memory (as the files are small by
// default - just a few Mb).
func (h *AwsHelper) GetFromS3(bucketName, s3Path string) (*io.ReadCloser, error) {
	svc := s3.New(h.AwsSession)
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Path),
	}

	results, err := svc.GetObject(input)
	if err != nil {
		return nil, err
	}
	return &results.Body, nil
}

// ExistsInS3 checks that a given path in s3 exists as a file
func (h *AwsHelper) ExistsInS3(bucketName, s3Path string) (bool, error) {
	svc := s3.New(h.AwsSession)
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Path),
	}

	_, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// UploadToS3 writes the content of a bytes array to the given s3 path
func (h *AwsHelper) UploadToS3(bucketName, s3Key string, data []byte) {
	uploader := s3manager.NewUploader(h.AwsSession)

	upParams := &s3manager.UploadInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(s3Key),
		Body:                 bytes.NewReader(data),
		StorageClass:         aws.String("STANDARD_IA"),
		ServerSideEncryption: aws.String("AES256"),
		// Expire: ...,
		// Tagging:
	}
	// Set file name and content before upload
	log.Printf("Writing file: s3://%s/%s\n", *upParams.Bucket, *upParams.Key)
	_, err := uploader.Upload(upParams)
	if err != nil {
		log.Fatalf("[ERROR] while uploading to s3: %s", err)
	}
}

// ReaderToChannel reads the data from a backup line by line, serializes it and
// sends it to the struct's channel
func (h *AwsHelper) ReaderToChannel(dataReader *io.ReadCloser) error {
	defer (*dataReader).Close()
	scanner := bufio.NewScanner(*dataReader)
	for scanner.Scan() {
		res := map[string]*dynamodb.AttributeValue{}
		data := scanner.Bytes()
		json.Unmarshal(data[:], &res)
		h.DataPipe <- res
	}
	return scanner.Err()
}

// S3ToDynamo pulls the s3 files from AwsHelper.ManifestS3 and import them
// inside the given table using the given batch size (and wait period between
// each batch)
func (h *AwsHelper) S3ToDynamo(tableName string, batchSize int64, waitPeriod time.Duration) error {
	var err error
	go h.ChannelToTable(tableName, batchSize, waitPeriod)
	h.Wg.Add(1)
	for _, entry := range h.ManifestS3.Entries {
		u, _ := url.Parse(entry.URL)
		if u.Scheme == "s3" {
			data, err := h.GetFromS3(u.Host, u.Path)
			if err != nil {
				return err
			}
			if err = h.ReaderToChannel(data); err != nil {
				break
			}
		}

	}
	close(h.DataPipe)
	h.Wg.Wait()
	return err
}

// DumpBuffer dumps the content of the given buffer to a new randomly generated
// file name in the given s3 path in the given bucket and resets the said buffer
func (h *AwsHelper) DumpBuffer(bucketName, s3Folder string, buff *bytes.Buffer) {
	filePath := fmt.Sprintf("%s/%s", s3Folder, genNewFileName())
	h.UploadToS3(bucketName, filePath, buff.Bytes())
	h.ManifestS3.Entries = append(h.ManifestS3.Entries, S3ManifestEntry{URL: fmt.Sprintf("s3://%s/%s", bucketName, filePath), Mandatory: true})
	buff.Reset()
}

// ChannelToS3 reads from the given channel and sends the data the given bucket
// in files of s3BufferSize max size
func (h *AwsHelper) ChannelToS3(bucketName, s3Folder string, s3BufferSize int) {
	defer h.Wg.Done()
	// buff is the buffer where the data will be stored while before being sent to s3
	var buff bytes.Buffer
	h.ManifestS3 = S3Manifest{Version: 3, Name: "DynamoDB-export"}

	for elem := range h.DataPipe {
		data, err := MarshalDynamoAttributeMap(elem)
		if err != nil {
			log.Fatalf("[ERROR] while converting to json: %v\nError: %s\n", elem, err)
		}

		// before overflowing the buffer, dump to s3 and empty it
		if buff.Len()+len(data) >= s3BufferSize && buff.Len() > 0 {
			h.DumpBuffer(bucketName, s3Folder, &buff)
		}
		// add the data to the buffer
		buff.Write(data)
		buff.WriteString("\n")
	}

	// Upload the rest of the buffer
	h.DumpBuffer(bucketName, s3Folder, &buff)
	// Signal the success of the backup
	h.UploadToS3(bucketName, fmt.Sprintf("%s/_SUCCESS", s3Folder), []byte{})
	// Wrap up the manifest of the backup files
	manifestData, err := json.Marshal(h.ManifestS3)
	if err != nil {
		log.Fatalf("[ERROR] while doing a marshal on the manifest: %v\nError: %s\n", h.ManifestS3, err)
	}
	h.UploadToS3(bucketName, fmt.Sprintf("%s/manifest", s3Folder), manifestData)
}
