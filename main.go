package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/VEVO/dynamodbdump/storage"
	"github.com/gobike/envflag"
)

// backupTable manages the consumer from a given DynamoDB table and a producer
// to a given s3 bucket
func backupTable(tableName string, batchSize int64, waitPeriod time.Duration, bucket, prefix string, addDate bool) {
	if addDate {
		t := time.Now().UTC()
		prefix += "/" + t.Format("2006-01-02-15-04-05")
	}

	proc := NewAwsHelper()
	go proc.ChannelToS3(bucket, prefix, 10*1024*1024)

	err := proc.TableToChannel(tableName, batchSize, waitPeriod)
	if err != nil {
		log.Fatal(err.Error())
	}
	proc.Wg.Wait()
}

func restoreTable(bucket, prefix, tableName string, batchSize int64, waitPeriod time.Duration, appendToTable bool) {
	proc := NewAwsHelper()

	// Check if the table exists and has data in it. If so, abort
	itemsCount, err := proc.CheckTableEmpty(tableName)
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
	if exists, err := proc.ExistsInS3(bucket, fmt.Sprintf("%s/_SUCCESS", prefix)); !exists {
		switch {
		case err != nil:
			log.Fatalf("[ERROR] Unable to retrieve the _SUCCESS flag information: %s\nAborting...\n", err)
		case !exists:
			log.Fatalf("[ERROR] Unable to find a _SUCCESS flag in the provided folder. Are you sure the backup was successful?\nAborting...\n")
		}
	}

	// Pull the manifest from s3 and load it to memory
	err = proc.LoadManifestFromS3(bucket, fmt.Sprintf("%s/manifest", prefix))
	if err != nil {
		log.Fatalf("[ERROR] Unable to load the manifest flag information: %s\nAborting...\n", err)
	}

	// For each file in the manifest pull the file, decode each line and add them to a batch and push them into the table (batch size, then wait and continue)
	go proc.ChannelToTable(tableName, batchSize, waitPeriod)
	err = proc.S3ToDynamo(tableName, batchSize, waitPeriod)
	if err != nil {
		log.Fatalf("[ERROR] Unable to import the full s3 backup to Dynamo: %s\nAborting...\n", err)
	}
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

	switch action {
	case "backup":
		backupTable(tableName, batchSize, time.Duration(waitTime)*time.Millisecond, s3Bucket, s3Folder, s3DateSuffix)
	case "restore":
		restoreTable(s3Bucket, s3Folder, tableName, batchSize, time.Duration(waitTime)*time.Millisecond, appendRestore)
	default:
		log.Fatalf("[ERROR] Unknown action given. See help for available actions.")
	}
	log.Println("All done!")
}
