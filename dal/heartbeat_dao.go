package dal

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"

	"log"
)

type HeartbeatEntry struct {
	TimeBucket     string
	LedgerID       string
	HeartbeatCount int64
	TTL            int64 // epoch seconds
}

func CreateFutureHeartbeat(ledgerId string, heartbeatCount int64) error {
	const twentyFourHours = 86400
	ttl := time.Now().Unix() + twentyFourHours

	futureTime := time.Now().Add(time.Duration(15) * time.Minute)
	timeBucket := GetTimeBucketKey(futureTime)
	entry := HeartbeatEntry{
		TimeBucket:     timeBucket,
		LedgerID:       ledgerId,
		HeartbeatCount: heartbeatCount,
		TTL:            ttl,
	}

	av, err := dynamodbattribute.MarshalMap(entry)
	if err != nil {
		log.Printf("got error marshalling heartbeat entry: %s", err)
		return err
	}
	tableName := dynamo_configuration.TABLE_HEARTBEAT

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = svc.PutItem(input)
	if err != nil {
		log.Printf("got error calling PutItem heartbeat entry: %s", err)
		return err
	}

	return err
}

func GetTimeBucketKey(bucketFromTime time.Time) string {
	bucketGranularity := 5
	remainder := bucketFromTime.UTC().Minute() % bucketGranularity
	bucketTime := bucketFromTime.Add(time.Duration(remainder) * time.Minute * -1)
	timeBucket := fmt.Sprintf("%d-%d-%d:%d.%d", bucketTime.UTC().Month(), bucketTime.UTC().Day(),
		bucketTime.UTC().Year(), bucketTime.UTC().Hour(), bucketTime.UTC().Minute())
	return timeBucket
}

func GetHeartbeatEntries(lastPageKey string, lastPageSortKey string) ([]HeartbeatEntry, string, string, error) {
	timeBucketKey := GetTimeBucketKey(time.Now())
	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(dynamo_configuration.TABLE_HEARTBEAT),
		KeyConditionExpression: aws.String("TimeBucket = :p"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":p": {
				S: aws.String(timeBucketKey),
			},
		},
	}
	if lastPageKey != "" {
		queryInput.SetExclusiveStartKey(map[string]*dynamodb.AttributeValue{
			"TimeBucket": {
				S: aws.String(lastPageKey),
			},
			"LedgerID": {
				N: aws.String(lastPageSortKey),
			},
		})
	}
	queryOutput, err := svc.Query(queryInput)
	if err != nil {
		log.Printf("unalbe to query heartbeat records: %s", err)
		return []HeartbeatEntry{}, "", "", err
	}

	const pk = "TimeBucket"
	const sk = "LedgerID"
	pagePk := ""
	pageSk := ""
	if _, ok := queryOutput.LastEvaluatedKey[pk]; ok {
		pagePk = *queryOutput.LastEvaluatedKey[pk].S
	}
	if _, ok := queryOutput.LastEvaluatedKey[sk]; ok {
		pageSk = *queryOutput.LastEvaluatedKey[sk].S
	}
	if len(queryOutput.Items) == 0 {
		log.Printf("no records found in page for heartbeat now: %s", timeBucketKey)
		return []HeartbeatEntry{}, "", "", nil
	}
	results := []HeartbeatEntry{}

	for _, h := range queryOutput.Items {
		tmpItem := HeartbeatEntry{}
		err = dynamodbattribute.UnmarshalMap(h, &tmpItem)
		if err != nil {
			log.Printf("error unmarshalling heartbeat item: %s", err)
			return []HeartbeatEntry{}, "", "", err
		}
		results = append(results, tmpItem)
	}

	return results, pagePk, pageSk, nil
}
