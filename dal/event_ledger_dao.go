package dal

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	dynamo_tables "github.com/bezalel-media-core/v2/dal/tables/v1"

	"log"
)

var svc = dynamodb.New(aws_configuration.GetAwsSession())

func CreateLedger(item dynamo_tables.Ledger) error {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		log.Fatalf("Got error marshalling new movie item: %s", err)
	}
	tableName := dynamo_configuration.TABLE_EVENT_LEDGER

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = svc.PutItem(input)
	if err != nil {
		log.Fatalf("Got error calling PutItem: %s", err)
	}

	return err
}

func AppendLedgerScriptEvents() error {
	// TODO: Conditional updates
	return nil
}

func AppendLedgerMediaEvents() error {
	// TODO: Conditional updates
	return nil
}

func AppendLedgerPublishEvents() error {
	// TODO: Conditional updates
	return nil
}

func GetLedger(ledgerId string) (dynamo_tables.Ledger, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerId),
			},
		},
	})
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	item := dynamo_tables.Ledger{}
	if result.Item == nil {
		return item, errors.New("not found")
	}
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		return item, err
	}
	return item, err
}
