package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// dynamoErrorCheck checks for the error output and waits the given waitError in
// case of ProvisionedThroughputExceededException
func dynamoErrorCheck(err error, waitError time.Duration) error {
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Printf("[WARNING] ProvisionedThroughputExceededException encountered, will wait %d before retrying: %s", waitError, aerr.Error())
				// This one is recoverable, just wait the waitTime before retrying
				time.Sleep(waitError)
			default:
				return aerr
			}
		} else {
			return err
		}
	}
	return nil
}

// AwsHelper supports a set of helpers around DynamoDB and s3
type AwsHelper struct {
	AwsSession client.ConfigProvider
	DynamoSvc  dynamodbiface.DynamoDBAPI
	Wg         sync.WaitGroup
	DataPipe   chan map[string]*dynamodb.AttributeValue
	ManifestS3 S3Manifest
}

// NewAwsHelper creates a new AwsHelper, initializing an AWS session and a few
// objects like a channel or a DynamoDB client
func NewAwsHelper() *AwsHelper {
	awsSess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	dataPipe := make(chan map[string]*dynamodb.AttributeValue)
	dynamoSvc := dynamodb.New(awsSess)
	return &AwsHelper{AwsSession: awsSess, DataPipe: dataPipe, DynamoSvc: dynamoSvc}
}

// TableToChannel scans an entire DynamoDB table, putting all the output records to a
// given channel and increment a given waitgroup
func (h *AwsHelper) TableToChannel(tableName string, batchSize int64, waitPeriod time.Duration) error {
	h.Wg.Add(1)

	var errChk error
	stopScan := false
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	// var results []map[string]*dynamodb.AttributeValue
	// Looping to recover on errors
	for !stopScan {
		params := &dynamodb.ScanInput{
			TableName:              aws.String(tableName),
			ReturnConsumedCapacity: aws.String("TOTAL"),
		}

		// Limit only accepts an int64 >= 1
		if batchSize > 0 {
			params.Limit = aws.Int64(batchSize)
		}
		// This is how we recover on basic errors
		if lastEvaluatedKey != nil {
			params.ExclusiveStartKey = lastEvaluatedKey
		}

		err := h.DynamoSvc.ScanPages(params,
			func(page *dynamodb.ScanOutput, lastPage bool) bool {
				log.Printf("Items: %d, Capacity consumed: %f", *page.Count, *page.ConsumedCapacity.CapacityUnits)
				for _, res := range page.Items {
					h.DataPipe <- res
				}
				time.Sleep(waitPeriod)
				stopScan = lastPage
				return !lastPage
			})

		// Error handling
		if errChk = dynamoErrorCheck(err, waitPeriod*2); errChk != nil {
			break
		}
	}
	close(h.DataPipe)
	return errChk
}

// CheckTableEmpty checks if the table exists and is empty. Returns -1 if does
// not exist, returns the number of items if exists and -10 in case of an error.
// If the table status is not "ACTIVE", the returned number will be -2 for
// "CREATING", -4 for "UPDATING", -8 for "DELETING"
func (h *AwsHelper) CheckTableEmpty(tbl string) (int64, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tbl),
	}

	result, err := h.DynamoSvc.DescribeTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return -1, nil
		}
		return -10, err
	}

	switch *result.Table.TableStatus {
	case "ACTIVE":
		return *result.Table.ItemCount, nil
	case "CREATING":
		return -2, nil
	case "UPDATING":
		return -4, nil
	case "DELETING":
		return -8, nil
	default:
		return -10, fmt.Errorf("Unable to determine the target table status. Please try again")
	}
}

// channelToWriteRequests polls from the channel and create an array of
// WriteRequests to be passed to a BatchWriteItem. If (globalIndex - batchSize)
// is less than 25 this will be the batch size. Else it'll be 25
//
// Note that the following criteria will be rejected by the AWS SDK:
// * Any individual item in a batch exceeds 400 KB.
// * The total request size exceeds 16 MB.
func (h *AwsHelper) channelToWriteRequests(batchSize, globalIndex int64) []*dynamodb.WriteRequest {
	idx := 0
	dataReq := []*dynamodb.WriteRequest{}
	for elem := range h.DataPipe {
		req := dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: elem}}
		dataReq = append(dataReq, &req)
		idx++
		// A BatchWriteItem should not have more than 25 WriteRequests
		if idx >= 25 || batchSize <= (globalIndex+int64(idx)) {
			break
		}
	}
	return dataReq
}

// batchToTable sends a BatchWriteItem to Dynamo
func (h *AwsHelper) batchToTable(wRequest map[string][]*dynamodb.WriteRequest, waitRetry time.Duration) {
	input := &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: aws.String("TOTAL"),
		RequestItems:           wRequest,
	}
	result, err := h.DynamoSvc.BatchWriteItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Printf("[WARNING] ProvisionedThroughputExceededException encountered. Waiting %d before retrying...\n", waitRetry)
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				log.Println("[WARNING] An item collection is too large. This exception is only returned for tables that have one or more local secondary indexes. Skip collection.")
			default:
				log.Fatalf("[ERROR] unrecoverable error during batch write: %s\n", aerr.Error())
			}
		} else {
			log.Fatalf("[ERROR] unrecoverable error during batch write: %s\n", aerr.Error())
		}
	}

	log.Printf("Unprocessed items: %d, Capacity consumed: %f\n", len(result.UnprocessedItems), *(result.ConsumedCapacity[0].CapacityUnits))
	if len(result.UnprocessedItems) > 0 {
		time.Sleep(waitRetry)
		h.batchToTable(result.UnprocessedItems, waitRetry)
	}
}

// ChannelToTable puts the data from the channel into the given Dynamo table
func (h *AwsHelper) ChannelToTable(tableName string, batchSize int64, waitPeriod time.Duration) {
	var currentIdx int64
	currentIdx = 0
	for {
		dataReq := h.channelToWriteRequests(batchSize, currentIdx)
		reqSize := len(dataReq)
		if reqSize == 0 {
			break // Leaves if the queue is closed and no items were found
		}
		log.Printf("Sending %d items\n", reqSize)
		h.batchToTable(map[string][]*dynamodb.WriteRequest{tableName: dataReq}, waitPeriod*2)
		currentIdx += int64(reqSize)
		if currentIdx >= batchSize {
			time.Sleep(waitPeriod)
			currentIdx = 0
		}
	}
	h.Wg.Done()
}
