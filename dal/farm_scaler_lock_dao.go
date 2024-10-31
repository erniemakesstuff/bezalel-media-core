package dal

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"

	"log"
)

type FarmScalerLockEntry struct {
	SystemID        string
	ProcessID       string
	ExpiryTimeMilli int64
	Version         int64
}

const SYSTEM_RENDER_FARM = "RenderFarmScaler"

func InitRenderFarmEntry() error {
	existingLock, err := GetLockEntry(SYSTEM_RENDER_FARM)
	if err != nil {
		log.Printf("error checking initial render farm lock entry: %s", err)
		return err
	}

	if len(existingLock.SystemID) != 0 {
		log.Printf("render farm lock entry already exists")
		return nil
	}

	entry := FarmScalerLockEntry{
		SystemID:        SYSTEM_RENDER_FARM,
		ProcessID:       "",
		ExpiryTimeMilli: 0,
		Version:         0,
	}
	av, err := dynamodbattribute.MarshalMap(entry)
	if err != nil {
		log.Printf("got error marshalling farm lock entry: %s", err)
		return err
	}
	tableName := dynamo_configuration.TABLE_FARM_SCALER_LOCK

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = svc.PutItem(input)
	if err != nil {
		log.Printf("got error calling PutItem farm lock entry: %s", err)
		return err
	}

	return err
}

func GetLockEntry(systemId string) (FarmScalerLockEntry, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_FARM_SCALER_LOCK),
		Key: map[string]*dynamodb.AttributeValue{
			"SystemID": {
				S: aws.String(systemId),
			},
		},
	})

	resultItem := FarmScalerLockEntry{}
	if err != nil {
		log.Printf("got error calling GetItem farm lock entry: %s", err)
		return resultItem, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &resultItem)
	if err != nil {
		log.Printf("error unmarshalling farm lock entry: %s", err)
		return resultItem, err
	}

	return resultItem, err
}

func TakeSystemLockOwnership(systemId string, processId string) (bool, error) {
	existingValue, err := GetLockEntry(systemId)
	if err != nil {
		log.Printf("failed to get existing lock entry: %s", err)
		return false, err
	}

	if !canTakeLock(existingValue, processId) {
		log.Printf("cannot take system lock system %s process %s", systemId, processId)
		return false, nil
	}

	// Check to see that no one updated before us.
	oldVersionNumber := existingValue.Version
	newVersionNumber := oldVersionNumber + 1
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"SystemID": {
				S: aws.String(systemId),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: aws.String(processId),
			},
			":v": {
				N: aws.String(strconv.FormatInt(newVersionNumber, 10)),
			},
			":ov": {
				N: aws.String(strconv.FormatInt(oldVersionNumber, 10)),
			},
		},
		TableName:           aws.String(dynamo_configuration.TABLE_FARM_SCALER_LOCK),
		ReturnValues:        aws.String("NONE"),
		UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :r, %s = :v", "ProcessID", "Version")),
		ConditionExpression: aws.String(fmt.Sprintf("%s = :ov", "Version")),
	}

	_, err = svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateLedgerEvents: %s", err)
		return false, err
	}
	return true, err
}

func canTakeLock(lockEntry FarmScalerLockEntry, processId string) bool {
	now := time.Now().UnixMilli()
	if lockEntry.ExpiryTimeMilli < now {
		return true
	}
	return lockEntry.ProcessID == processId
}
