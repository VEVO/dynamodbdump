package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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

// TableToChannel scans an entire DynamoDB table, putting all the output records to a
// given channel and increment a given waitgroup
func TableToChannel(svc dynamodbiface.DynamoDBAPI, tableName string, batchSize int64, waitPeriod time.Duration, dataPipe chan map[string]*dynamodb.AttributeValue) error {
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

		err := svc.ScanPages(params,
			func(page *dynamodb.ScanOutput, lastPage bool) bool {
				log.Printf("Items: %d, Capacity consumed: %f", *page.Count, *page.ConsumedCapacity.CapacityUnits)
				for _, res := range page.Items {
					dataPipe <- res
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
	close(dataPipe)
	return errChk
}

// CheckTableEmpty checks if the table exists and is empty. Returns -1 if does
// not exist, returns the number of items if exists and -10 in case of an error.
// If the table status is not "ACTIVE", the returned number will be -2 for
// "CREATING", -4 for "UPDATING", -8 for "DELETING"
func CheckTableEmpty(svc dynamodbiface.DynamoDBAPI, tbl string) (int64, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tbl),
	}

	result, err := svc.DescribeTable(input)
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
func channelToWriteRequests(batchSize, globalIndex int64, dataPipe chan map[string]*dynamodb.AttributeValue) []*dynamodb.WriteRequest {
	idx := 0
	dataReq := []*dynamodb.WriteRequest{}
	for elem := range dataPipe {
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
func batchToTable(svc dynamodbiface.DynamoDBAPI, wRequest map[string][]*dynamodb.WriteRequest, waitRetry time.Duration) {
	input := &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: aws.String("TOTAL"),
		RequestItems:           wRequest,
	}
	result, err := svc.BatchWriteItem(input)
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
		batchToTable(svc, result.UnprocessedItems, waitRetry)
	}
}

// ChannelToTable puts the data from the channel into the given Dynamo table
func ChannelToTable(svc dynamodbiface.DynamoDBAPI, tableName string, batchSize int64, waitPeriod time.Duration, dataPipe chan map[string]*dynamodb.AttributeValue, wg *sync.WaitGroup) {
	var currentIdx int64
	currentIdx = 0
	for {
		dataReq := channelToWriteRequests(batchSize, currentIdx, dataPipe)
		reqSize := len(dataReq)
		if reqSize == 0 {
			break // Leaves if the queue is closed and no items were found
		}
		log.Printf("Sending %d items\n", reqSize)
		batchToTable(svc, map[string][]*dynamodb.WriteRequest{tableName: dataReq}, waitPeriod*2)
		currentIdx += int64(reqSize)
		if currentIdx >= batchSize {
			time.Sleep(waitPeriod)
			currentIdx = 0
		}
	}
	wg.Done()
}
