package dal

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
)

type RateLimitEntry struct {
	RateTimeKeyBucket string // Represent granularity API_NAME:<date>:minute or some other granularity
	RequestCount      int64
	MaxRequests       int64
	TTL               int64 // epoch seconds
}

const (
	RATE_API_YOUTUBE_UPLOAD = "API YouTube Upload"
	RATE_API_TWITTER_POST   = "API Twitter Post"
	RATE_API_MEDIUM_POST    = "API Medium Post"
	RATE_API_REDDIT_POST    = "API Reddit Post"
)

// Assumes minute granularity. TODO: update this if you want additional granularities.
// If an error occurs, default to not-callble.
func IsCallable(apiName string, maxRequestsPerMin int64) bool {
	const twoHours = 7200
	ttl := time.Now().Unix() + twoHours
	rateTimeBucket := getRateTimeKeyBucket(apiName, time.Now())
	entry := RateLimitEntry{
		RateTimeKeyBucket: rateTimeBucket,
		MaxRequests:       maxRequestsPerMin,
		TTL:               ttl,
	}
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"RateTimeKeyBucket": {
				S: aws.String(entry.RateTimeKeyBucket),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v0": {
				N: aws.String(strconv.FormatInt(1, 10)),
			},
			":v1": {
				N: aws.String(strconv.FormatInt(entry.TTL, 10)),
			},
			":v2": {
				N: aws.String(strconv.FormatInt(entry.MaxRequests, 10)),
			},
		},
		TableName:    aws.String(dynamo_configuration.TABLE_RATE_LIMIT),
		ReturnValues: aws.String("ALL_NEW"),
		UpdateExpression: aws.String(fmt.Sprintf("ADD %s :v0 SET #ttlName = :v1, %s = :v2",
			"RequestCount", "MaxRequests")),
		ExpressionAttributeNames: map[string]*string{
			"#ttlName": aws.String("TTL"),
		},
	}

	response, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("WARN error checking rate limit: %s", err)
		return false
	}

	responseItem := RateLimitEntry{}
	err = dynamodbattribute.UnmarshalMap(response.Attributes, &responseItem)
	if err != nil {
		log.Printf("WARN error unmarshalling rate limit item: %s", err)
		return false
	}

	return responseItem.RequestCount <= responseItem.MaxRequests
}

func getRateTimeKeyBucket(apiName string, bucketTime time.Time) string {
	timeBucket := fmt.Sprintf("%s:%d-%d-%d:%d.%d", apiName, bucketTime.UTC().Month(), bucketTime.UTC().Day(),
		bucketTime.UTC().Year(), bucketTime.UTC().Hour(), bucketTime.UTC().Minute())
	return timeBucket
}
