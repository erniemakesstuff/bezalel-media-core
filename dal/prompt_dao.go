package dal

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"

	"log"
)

func CreatePrompt(item tables.Prompt) error {

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		log.Printf("got error marshalling prompt item: %s", err)
		return err
	}
	tableName := dynamo_configuration.TABLE_PROMPT

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = svc.PutItem(input)
	if err != nil {
		log.Printf("got error calling PutItem item: %s", err)
		return err
	}

	return err
}

func GetPrompt(promptId string) (tables.Prompt, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_PROMPT),
		Key: map[string]*dynamodb.AttributeValue{
			"PromptID": {
				S: aws.String(promptId),
			},
		},
	})

	resultItem := tables.Prompt{}
	if err != nil {
		log.Printf("got error calling GetItem prompt item: %s", err)
		return resultItem, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &resultItem)
	if err != nil {
		log.Printf("error unmarshalling prompt item: %s", err)
		return resultItem, err
	}

	return resultItem, err
}
