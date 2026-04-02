package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/VEVO/dynamodbdump/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/gobike/envflag"
)

var (
	dynamoSvc dynamodbiface.DynamoDBAPI
	c         chan map[string]*dynamodb.AttributeValue
)

// backupTable manages the consumer from a given DynamoDB table and a producer
// to a given s3 bucket
func backupTable(tableName string, batchSize int64, waitPeriod time.Duration, bucket, prefix string, addDate bool, store storage.BackupIface) {
	var wg sync.WaitGroup
	if addDate {
		t := time.Now().UTC()
		prefix += "/" + t.Format("2006-01-02-15-04-05")
	}

	wg.Add(1)
	go store.Write(&storage.FileInput{Bucket: aws.String(bucket), Path: aws.String(prefix)}, 10*1024*1024, &wg)

	err := TableToChannel(dynamoSvc, tableName, batchSize, waitPeriod, c)
	if err != nil {
		log.Fatal(err.Error())
	}
	wg.Wait()
}

func restoreTable(bucket, prefix, tableName string, batchSize int64, waitPeriod time.Duration, appendToTable bool, store storage.BackupIface) {
	var wg sync.WaitGroup
	// Check if the table exists and has data in it. If so, abort
	itemsCount, err := CheckTableEmpty(dynamoSvc, tableName)
	if err != nil {
		log.Fatalf("[ERROR] Unable to retrieve the target table informations: %s\nAborting...\n", err)
	}
	switch {
	case itemsCount > 0 && !appendToTable:
		log.Fatalf("[ERROR] The target table is not empty. Aborting...\n")
	case itemsCount == -1:
		log.Fatalf("[ERROR] The target table does not exists. Aborting...\n")
	case itemsCount < -1:
		log.Fatalf("[ERROR] The target table is not in ACTIVE state, so not writable. Aborting...\n")
	}

	// Check if a file "_SUCCESS" is present in the directory
	if exists, err := store.Exists(&storage.FileInput{Bucket: aws.String(bucket), Path: aws.String(fmt.Sprintf("%s/_SUCCESS", prefix))}); !exists {
		switch {
		case err != nil:
			log.Fatalf("[ERROR] Unable to retrieve the _SUCCESS flag information: %s\nAborting...\n", err)
		case !exists:
			log.Fatalf("[ERROR] Unable to find a _SUCCESS flag in the provided folder. Are you sure the backup was successful?\nAborting...\n")
		}
	}

	// Pull the manifest from s3 and load it to memory
	err = store.LoadManifest(&storage.FileInput{Bucket: aws.String(bucket), Path: aws.String(fmt.Sprintf("%s/manifest", prefix))})
	if err != nil {
		log.Fatalf("[ERROR] Unable to load the manifest flag information: %s\nAborting...\n", err)
	}

	// For each file in the manifest pull the file, decode each line and add them to a batch and push them into the table (batch size, then wait and continue)
	go ChannelToTable(dynamoSvc, tableName, batchSize, waitPeriod, c, &wg)
	err = store.WriteToDB(tableName, batchSize, waitPeriod, &wg)
	if err != nil {
		log.Fatalf("[ERROR] Unable to import the full s3 backup to Dynamo: %s\nAborting...\n", err)
	}
	wg.Wait()
}

func main() {
	var (
		s3DateSuffix, appendRestore           bool
		batchSize, waitTime                   int64
		action, tableName, s3Bucket, s3Folder string
	)

	flag.StringVar(&action, "action", "backup", "Action to perform. Only accept 'backup' or 'restore'. Environment variable: ACTION")
	flag.StringVar(&tableName, "dynamo-table", "", "Name of the Dynamo table to backup from or to restore in. Environment variable: DYNAMO_TABLE")
	flag.StringVar(&s3Bucket, "s3-bucket", "", "Name of the s3 bucket where to put the backup or where to restore from. Environment variable: S3_BUCKET")
	flag.StringVar(&s3Folder, "s3-folder", "", "Path inside the s3 bucket where to put or grab (for restore) the backup. Environment variable: S3_FOLDER")
	flag.BoolVar(&s3DateSuffix, "s3-date-folder", false, "Adds an autogenenated suffix folder named using the UTC date in the format YYYY-mm-dd-HH24-MI-SS to the provided S3 folder. Environment variable: S3_DATE_FOLDER")
	flag.Int64Var(&batchSize, "batch-size", 1000, "Max number of records to read from the dynamo table at once or to write in case of a restore. Environment variable: BATCH_SIZE")
	flag.Int64Var(&waitTime, "wait-ms", 100, "Number of milliseconds to wait between batches. If a ProvisionedThroughputExceededException is encountered, the script will wait twice that amount of time before retrying. Environment variable: WAIT_MS")
	flag.BoolVar(&appendRestore, "restore-append", false, "Appends the rows to a non-empty table when restoring instead of aborting. Environment variable: RESTORE_APPEND")
	envflag.Parse()

	// For now we only backup to s3 but this can easily evolve in the future
	awsSess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	bkpStorage := storage.NewS3Backup(awsSess)
	dynamoSvc = dynamodb.New(awsSess)
	c = make(chan map[string]*dynamodb.AttributeValue)
	bkpStorage.DataPipe = c

	switch action {
	case "backup":
		backupTable(tableName, batchSize, time.Duration(waitTime)*time.Millisecond, s3Bucket, s3Folder, s3DateSuffix, bkpStorage)
	case "restore":
		restoreTable(s3Bucket, s3Folder, tableName, batchSize, time.Duration(waitTime)*time.Millisecond, appendRestore, bkpStorage)
	default:
		log.Fatalf("[ERROR] Unknown action given. See help for available actions.")
	}
	log.Println("All done!")
}
