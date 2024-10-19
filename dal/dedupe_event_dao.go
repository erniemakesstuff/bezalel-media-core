package dal

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"

	"log"
)

type HashEntry struct {
	EventHash string
	TTL       int64
}

func CreateHashEntry(rawContentHash string) error {
	const threeDaysTTL = 259200000
	entry := HashEntry{
		EventHash: rawContentHash,
		TTL:       time.Now().UnixMilli() + threeDaysTTL,
	}
	av, err := dynamodbattribute.MarshalMap(entry)
	if err != nil {
		log.Printf("got error marshalling hash entry: %s", err)
		return err
	}
	tableName := dynamo_configuration.TABLE_DEDUPE_EVENTS

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = svc.PutItem(input)
	if err != nil {
		log.Printf("got error calling PutItem hash entry: %s", err)
		return err
	}

	return err
}

func GetHashEntry(rawContentHash string) (HashEntry, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_DEDUPE_EVENTS),
		Key: map[string]*dynamodb.AttributeValue{
			"EventHash": {
				S: aws.String(rawContentHash),
			},
		},
	})

	resultItem := HashEntry{}
	if err != nil {
		log.Printf("got error calling GetItem hash entry: %s", err)
		return resultItem, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &resultItem)
	if err != nil {
		log.Printf("error unmarshalling hash entry: %s", err)
		return resultItem, err
	}

	return resultItem, err
}
