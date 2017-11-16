package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"reflect"
	"testing"
	"time"
)

// dataSet represents the sets of data used for the tests in Dynamo
var dataSet = []map[string]*dynamodb.AttributeValue{
	{"artist": {S: aws.String("Aerosmith")}, "songs": {SS: []*string{aws.String("Cryin'"), aws.String("Walk this way"), aws.String("Crazy"), aws.String("Rag Doll")}}},
	{"artist": {S: aws.String("Queen")}, "songs": {SS: []*string{aws.String("Under pressure"), aws.String("Somebody to love"), aws.String("Fat bottom girls")}}},
	{"artist": {S: aws.String("Metallica")}, "songs": {SS: []*string{aws.String("Enter Sandman"), aws.String("Master of Puppets")}}},
}

// struct to mock the Dynamo calls
type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamoDBClient) ScanPages(params *dynamodb.ScanInput, pager func(*dynamodb.ScanOutput, bool) bool) error {
	dsSize := int64(len(dataSet))
	dataOut := dynamodb.ScanOutput{
		ConsumedCapacity: &dynamodb.ConsumedCapacity{CapacityUnits: aws.Float64(23), TableName: params.TableName},
		Count:            aws.Int64(dsSize),
		Items:            dataSet,
	}
	pager(&dataOut, true)
	return nil
}

func TestTableToChannel(t *testing.T) {
	h := AwsHelper{}

	h.DynamoSvc = &mockDynamoDBClient{}
	h.DataPipe = make(chan map[string]*dynamodb.AttributeValue)

	// Consumer
	go func() {
		// Checks that all the elements of the channel are part of the dataSet
		idx := 0
		for elem := range h.DataPipe {
			if !reflect.DeepEqual(elem, dataSet[idx]) {
				t.Fatalf("Element %d in the channel mismatch. Expecting: %v\nGot: %v\n", idx, dataSet[idx], elem)
			}
			idx++
		}
		// Checks that all the elements of the dataSet have been parsed
		if idx != len(dataSet) {
			t.Fatalf("Size of the dataSet is %d, only got %d elements from the channel\n", len(dataSet), idx)
		}
		h.Wg.Done()
	}()

	h.TableToChannel("myTable", 10, time.Duration(42)*time.Millisecond)
}

func TestDynamoErrorCheck(t *testing.T) {
	errorTest := []struct{ inputErr, expectedOut error }{
		{inputErr: nil, expectedOut: nil},
		{inputErr: fmt.Errorf("Random error"), expectedOut: fmt.Errorf("Random error")},
		{inputErr: awserr.New(dynamodb.ErrCodeInternalServerError, "Bla bla", nil), expectedOut: awserr.New(dynamodb.ErrCodeInternalServerError, "Bla bla", nil)},
		{inputErr: awserr.New(dynamodb.ErrCodeProvisionedThroughputExceededException, "Bla bla", nil), expectedOut: nil},
	}
	for _, item := range errorTest {
		returnedOut := dynamoErrorCheck(item.inputErr, 1)
		if !reflect.DeepEqual(returnedOut, item.expectedOut) {
			t.Fatalf("Input %v should return %v. Got: %v\n", item.inputErr, item.expectedOut, returnedOut)
		}
	}
}
