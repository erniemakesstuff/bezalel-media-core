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

type DaemonLockEntry struct {
	SystemID             string
	ProcessID            string
	ExpiryTimeEpochMilli int64
	Version              int64
}

const SYSTEM_RENDER_FARM = "RenderFarmScaler"
const SYSTEM_HEARTBEAT_MONITOR = "HeartbeatMonitor"

func InitDaemonEntry(systemId string) error {
	existingLock, err := GetLockEntry(systemId)
	if err != nil {
		log.Printf("error checking initial render farm lock entry: %s", err)
		return err
	}

	if len(existingLock.SystemID) != 0 {
		// No need to reinitialize. happy path.
		log.Printf("daemon lock entry already exists for system %s", systemId)
		return nil
	}

	entry := DaemonLockEntry{
		SystemID:             systemId,
		ProcessID:            "",
		ExpiryTimeEpochMilli: 0,
		Version:              0,
	}
	av, err := dynamodbattribute.MarshalMap(entry)
	if err != nil {
		log.Printf("got error marshalling farm lock entry: %s", err)
		return err
	}
	tableName := dynamo_configuration.SYSTEM_DAEMON

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

func GetLockEntry(systemId string) (DaemonLockEntry, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.SYSTEM_DAEMON),
		Key: map[string]*dynamodb.AttributeValue{
			"SystemID": {
				S: aws.String(systemId),
			},
		},
	})

	resultItem := DaemonLockEntry{}
	if err != nil {
		log.Printf("got error calling GetItem daemon lock entry: %s", err)
		return resultItem, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &resultItem)
	if err != nil {
		log.Printf("error unmarshalling daemon lock entry: %s", err)
		return resultItem, err
	}

	return resultItem, err
}

func TakeSystemLockOwnership(systemId string, processId string, expiryTimeMilli int64) (bool, error) {
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
	expiryTime := time.Now().UnixMilli() + expiryTimeMilli
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
			":e": {
				N: aws.String(strconv.FormatInt(expiryTime, 10)),
			},
		},
		TableName:           aws.String(dynamo_configuration.SYSTEM_DAEMON),
		ReturnValues:        aws.String("NONE"),
		UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :r, %s = :v, %s = :e", "ProcessID", "Version", "ExpiryTimeEpochMilli")),
		ConditionExpression: aws.String(fmt.Sprintf("%s = :ov", "Version")),
	}

	_, err = svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateLedgerEvents: %s", err)
		return false, err
	}
	return true, err
}

func canTakeLock(lockEntry DaemonLockEntry, processId string) bool {
	now := time.Now().UnixMilli()
	if lockEntry.ExpiryTimeEpochMilli < now {
		return true
	}
	return lockEntry.ProcessID == processId
}
