package dal

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	env "github.com/bezalel-media-core/v2/configuration"
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

func GetPublisherWatermarkInfo(accountId string, publisherProfileId string) (string, error) {
	// TODO https://trello.com/c/KoxquFya
	return env.GetEnvConfigs().DefaultPublisherWatermarkText, nil
}

func StoreOauthCredentials(accountId string, publisherProfileId string, bearerToken string, refreshToken string, expiryMilliSec int64, tokenType string) error {
	acc, err := GetPublisherAccount(accountId, publisherProfileId)
	if err != nil {
		log.Printf("cannot store oauth credentials: %s", err)
		return err
	}

	if len(acc.AccountID) == 0 {
		return fmt.Errorf("cannot store oauth credentials, no existing profile accountId: %s , profileId: %s", accountId, publisherProfileId)
	}
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
			":v0": {
				N: aws.String(strconv.FormatInt(expiryMilliSec, 10)),
			},
			":v1": {
				S: aws.String(bearerToken),
			},
			":v2": {
				S: aws.String(refreshToken),
			},
			":v3": {
				S: aws.String(tokenType),
			},
		},
		TableName:        aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		ReturnValues:     aws.String("NONE"),
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :v0, %s = :v1, %s = :v2, %s = :v3", "OauthExpiryEpochSec", "OauthToken", "OauthRefreshToken", "OauthTokenType")),
	}

	_, err = svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateItem to store oauth credentials: %s", err)
		return err
	}

	return nil
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

func RecordPublishTime(accountId string, publisherProfileId string) error {
	releaseTime := time.Now().UnixMilli()
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
			":v": {
				N: aws.String(strconv.FormatInt(releaseTime, 10)),
			},
		},
		TableName:        aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		ReturnValues:     aws.String("NONE"),
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :v", "LastPublishAtEpochMilli")),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateItem to record publish time: %s", err)
		return err
	}
	return nil
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
	return releaseLock(accountId, publisherProfileId, processId, "AssignmentLockID", "AssignmentLockTTL")
}

func ReleasePublishLock(accountId string, publisherProfileId string, processId string) error {
	return releaseLock(accountId, publisherProfileId, processId, "PublishLockID", "PublishLockTTL")
}

func SetProfileStaleFlag(accountId string, publisherProfileId string, isStale bool) error {
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
			":v": {
				BOOL: aws.Bool(isStale),
			},
		},
		TableName:        aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		ReturnValues:     aws.String("NONE"),
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :v", "IsStaleProfile")),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateItem in setProfileStaleFlag: %s", err)
		return err
	}
	return nil
}

func ForceAllLocksFree(accountId string, publisherProfileId string) error {
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
		},
		TableName:    aws.String(dynamo_configuration.TABLE_ACCOUNTS),
		ReturnValues: aws.String("NONE"),
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :r, %s = :v, %s = :r, %s = :v",
			"AssignmentLockID", "AssignmentLockTTL",
			"PublishLockID", "PublishLockTTL")),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateItem to release all locks: %s", err)
		return err
	}
	return nil
}

func releaseLock(accountId string, publisherProfileId string, oldLockId string, lockIdField string, lockTtlField string) error {
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
				S: aws.String(oldLockId),
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

func AssignPublisherProfile(processId string, distributionChannelName string, publisherLanguage string, publisherNiche string) (tables.AccountPublisher, error) {
	lpk := ""
	lsk := ""
	var err error
	resultItem := tables.AccountPublisher{}
	for {
		resultItem, lpk, lsk, err = queryActivePublisherProfile(distributionChannelName, lpk, lsk, publisherLanguage, publisherNiche)
		if err != nil {
			log.Printf("failed to query account publisher profile table: %s", err)
			return tables.AccountPublisher{}, err
		} else if lpk == "" && lsk == "" {
			break
		}
	}

	if resultItem.AccountID == "" {
		return resultItem, fmt.Errorf("no active account publisher profiles found distChannel: %s language: %s niche: %s",
			distributionChannelName, publisherLanguage, publisherNiche)
	}

	err = takeAssignmentLock(resultItem.AccountID, resultItem.PublisherProfileID, processId)
	if err != nil {
		log.Printf("error assigning publisher profile accountId: %s , profileId: %s , err: %s", resultItem.AccountID, resultItem.PublisherProfileID, err)
		return resultItem, err
	}
	return resultItem, nil
}

func queryActivePublisherProfile(distributionChannelName string, lastPageKeyPK string,
	lastPageKeySK string, publisherLanguage string, publisherNiche string) (tables.AccountPublisher, string, string, error) {
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
			":i": {
				S: aws.String(publisherNiche),
			},
			":b": {
				BOOL: aws.Bool(false),
			},
		},
		FilterExpression: aws.String(`NOT contains(AccountSubscriptionStatus, :e) AND AssignmentLockTTL < :n 
		AND PublisherLanguage = :l AND PublisherNiche = :i AND IsStaleProfile = :b`),
		Limit: aws.Int64(maxRecordsPerQuery),
	}
	if lastPageKeyPK != "" {
		queryInput.SetExclusiveStartKey(map[string]*dynamodb.AttributeValue{
			"ChannelName": {
				S: aws.String(lastPageKeyPK),
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
	return resultItem, pagePk, pageSk, nil
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
	expiryTime := time.Now().UnixMilli() + env.GetEnvConfigs().AssignmentLockMilliTTL
	err = takeLock(processId, account, "AssignmentLockID", "AssignmentLockTTL", account.AssignmentLockID, expiryTime)
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
	// Asserts PublishLock and AssignmentLock are for the same media event.
	expiryTime := account.AssignmentLockTTL
	err = takeLock(processId, account, "PublishLockID", "PublishLockTTL", account.PublishLockID, expiryTime)
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

func takeLock(processId string, account tables.AccountPublisher, lockIdField string, lockTtlField string, oldLockId string, lockEpochMilliTtl int64) error {
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
				N: aws.String(strconv.FormatInt(lockEpochMilliTtl, 10)),
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
		log.Printf("error calling takeLock: %s", err)
		return err
	}
	return err
}
