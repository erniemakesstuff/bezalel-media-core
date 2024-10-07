package dal

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"

	"log"
)

func CreatePublisherAccount(item tables.AccountPublisher) error {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		log.Printf("got error marshalling ledger item: %s", err)
		return err
	}
	tableName := dynamo_configuration.TABLE_ACCOUNTS

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

func GetPublisherAccount(accountId string, publisherProfileId string) (tables.AccountPublisher, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		Key: map[string]*dynamodb.AttributeValue{
			"AccountID": {
				S: aws.String(accountId),
			},
			"PublisherProfileID": {
				S: aws.String(publisherProfileId),
			},
		},
	})

	resultItem := tables.AccountPublisher{}
	if err != nil {
		log.Printf("got error calling GetItem accountPublisher item: %s", err)
		return resultItem, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &resultItem)
	if err != nil {
		log.Printf("error unmarshalling accountPublisher item: %s", err)
		return resultItem, err
	}

	return resultItem, err
}

func DeletePublisherAccount(accountId string, publisherProfileId string) error {
	_, err := svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		Key: map[string]*dynamodb.AttributeValue{
			"AccountID": {
				S: aws.String(accountId),
			},
			"PublisherProfileID": {
				S: aws.String(publisherProfileId),
			},
		},
	})
	return err
}

func ReleaseAssignment(accountId string, publisherProfileId string, processId string) error {
	return releaseAssignment(accountId, publisherProfileId, processId, "AssignmentLockID", "AssignmentLockTTL")
}

func ReleasePublishLock(accountId string, publisherProfileId string, processId string) error {
	return releaseAssignment(accountId, publisherProfileId, processId, "PublishLockID", "PublishLockTTL")
}

func releaseAssignment(accountId string, publisherProfileId string, oldLockId string, lockIdField string, lockTtlField string) error {
	const releaseLockId = ""
	const releaseTime = 0
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"AccountID": {
				S: aws.String(accountId),
			},
			"PublisherProfileID": {
				S: aws.String(publisherProfileId),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: aws.String(releaseLockId),
			},
			":v": {
				N: aws.String(strconv.FormatInt(releaseTime, 10)),
			},
			":ov": {
				N: aws.String(oldLockId),
			},
		},
		TableName:           aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		ReturnValues:        aws.String("NONE"),
		UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :r, %s = :v", lockIdField, lockTtlField)),
		ConditionExpression: aws.String(fmt.Sprintf("%s = :ov", lockIdField)),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling UpdateItem to release lock: %s", err)
		return err
	}
	return nil
}

func AssignPublisherProfile(processId string, distributionChannelName string, publisherLanguage string) (tables.AccountPublisher, error) {
	lpk := ""
	lsk := ""
	var err error
	resultItem := tables.AccountPublisher{}
	for {
		resultItem, lpk, lsk, err = queryActivePublisherProfile(distributionChannelName, lpk, lsk, publisherLanguage)
		if err != nil {
			log.Printf("failed to query account publisher profile table: %s", err)
			return tables.AccountPublisher{}, err
		} else if lpk == "" && lsk == "" {
			break
		}
	}

	if resultItem.AccountID == "" {
		return resultItem, errors.New("no active account publisher profiles found")
	}

	err = takeAssignmentLock(resultItem.AccountID, resultItem.PublisherProfileID, processId)
	if err != nil {
		log.Printf("error assigning publisher profile: %s", err)
		return resultItem, err
	}
	return resultItem, nil
}

func queryActivePublisherProfile(distributionChannelName string, lastPagekeyPK string,
	lastPageKeySK string, publisherLanguage string) (tables.AccountPublisher, string, string, error) {
	const maxRecordsPerQuery = 200
	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		IndexName:              aws.String(dynamo_configuration.PUBLISHER_PROFILE_GSI_NAME),
		KeyConditionExpression: aws.String("ChannelName = :c"),
		ScanIndexForward:       aws.Bool(true), // ASCending last-publish time.
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":c": {
				S: aws.String(distributionChannelName),
			},
			":e": {
				S: aws.String("Expired"),
			},
			":n": {
				N: aws.String(strconv.FormatInt(time.Now().UnixMilli(), 10)),
			},
			":l": {
				S: aws.String(publisherLanguage),
			},
		},
		FilterExpression: aws.String("NOT contains(AccountSubscriptionStatus, :e) AND AssignmentLockTTL < :n AND PublisherLanguage = :l"),
		Limit:            aws.Int64(maxRecordsPerQuery),
	}
	if lastPagekeyPK != "" {
		queryInput.SetExclusiveStartKey(map[string]*dynamodb.AttributeValue{
			"ChannelName": {
				S: aws.String(lastPagekeyPK),
			},
			"LastPublishAtEpochMilli": {
				N: aws.String(lastPageKeySK),
			},
		})
	}
	queryOutput, err := svc.Query(queryInput)
	if err != nil {
		log.Printf("unalbe to query account publisher GSI: %s", err)
		return tables.AccountPublisher{}, "", "", err
	}
	const pk = "ChannelName"
	const sk = "LastPublishAtEpochMilli"
	pagePk := ""
	pageSk := ""
	if _, ok := queryOutput.LastEvaluatedKey[pk]; ok {
		pagePk = *queryOutput.LastEvaluatedKey[pk].S
	}
	if _, ok := queryOutput.LastEvaluatedKey[sk]; ok {
		pageSk = *queryOutput.LastEvaluatedKey[sk].S
	}
	if len(queryOutput.Items) == 0 {
		log.Printf("no records found in page for account publisher GSI, returning w/ pagination if set")
		return tables.AccountPublisher{}, pagePk, pageSk, nil
	}

	resultItem := tables.AccountPublisher{}
	err = dynamodbattribute.UnmarshalMap(queryOutput.Items[0], &resultItem)
	if err != nil {
		log.Printf("error unmarshalling accountPublisher item: %s", err)
		return resultItem, "", "", err
	}
	return resultItem, "", "", nil
}

func takeAssignmentLock(accountId string, publisherProfileId string, processId string) error {
	account, err := GetPublisherAccount(accountId, publisherProfileId)
	if err != nil {
		log.Printf("error getting publisher account: %s", err)
		return err
	}

	if !canTakeAssignmentLock(processId, account) {
		return fmt.Errorf("unable to take assignment lock. accountId: %s publisherProfileId: %s processId: %s",
			accountId, publisherProfileId, processId)
	}
	err = takeLock(processId, account, "AssignmentLockID", "AssignmentLockTTL", account.AssignmentLockID)
	return err
}

func TakePublishLock(accountId string, publisherProfileId string, processId string) error {
	account, err := GetPublisherAccount(accountId, publisherProfileId)
	if err != nil {
		log.Printf("error getting publisher account: %s", err)
		return err
	}

	if !canTakePublishLock(processId, account) {
		return fmt.Errorf("unable to take publish lock. accountId: %s publisherProfileId: %s processId: %s",
			accountId, publisherProfileId, processId)
	}
	err = takeLock(processId, account, "PublishLockID", "PublishLockTTL", account.PublishLockID)
	return err
}

func canTakeAssignmentLock(processId string, account tables.AccountPublisher) bool {
	if account.AssignmentLockID == processId {
		return true
	}
	if account.AssignmentLockID == "" {
		return true
	}

	lockExpiry := account.AssignmentLockTTL
	epochNow := time.Now().UnixMilli()
	return epochNow > lockExpiry
}

func canTakePublishLock(processId string, account tables.AccountPublisher) bool {
	if account.PublishLockID == processId {
		return true
	}
	if account.PublishLockID == "" {
		return true
	}

	lockExpiry := account.PublishLockTTL
	epochNow := time.Now().UnixMilli()
	return epochNow > lockExpiry
}

func takeLock(processId string, account tables.AccountPublisher, lockIdField string, lockTtlField string, oldLockId string) error {
	const ninetyMinutes = 5400000 // TODO: Replace w/ env config
	expiryTime := time.Now().UnixMilli() + ninetyMinutes
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"AccountID": {
				S: aws.String(account.AccountID),
			},
			"PublisherProfileID": {
				S: aws.String(account.PublisherProfileID),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: aws.String(processId),
			},
			":v": {
				N: aws.String(strconv.FormatInt(expiryTime, 10)),
			},
			":ov": {
				S: aws.String(oldLockId), // old assignment lock
			},
			":n": {
				S: aws.String("NULL"),
			},
		},
		TableName:           aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		ReturnValues:        aws.String("NONE"),
		UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :r, %s = :v", lockIdField, lockTtlField)),
		ConditionExpression: aws.String(fmt.Sprintf("%s = :ov OR attribute_type(%s, :n)", lockIdField, lockIdField)),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling UpdateItem: %s", err)
		return err
	}
	return err
}
