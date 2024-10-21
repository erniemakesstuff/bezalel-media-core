package dal

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"log"
	"reflect"
	"time"

	"bitbucket.org/creachadair/stringset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	env "github.com/bezalel-media-core/v2/configuration"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

func CreateLedger(item tables.Ledger) error {
	item.MediaEventsVersion = start_version
	item.PublishEventsVersion = start_version
	item.LedgerStatus = tables.NEW_LEDGER
	item.LedgerCreatedAtEpochMilli = time.Now().UnixMilli()

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		log.Printf("got error marshalling ledger item: %s", err)
		return err
	}
	tableName := dynamo_configuration.TABLE_EVENT_LEDGER

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

func GetLedger(ledgerId string) (tables.Ledger, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerId),
			},
		},
	})

	resultItem := tables.Ledger{}
	if err != nil {
		log.Printf("got error calling GetItem ledger item: %s", err)
		return resultItem, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &resultItem)
	if err != nil {
		log.Printf("error unmarshalling ledger item: %s", err)
		return resultItem, err
	}

	return resultItem, err
}

func DeleteLedger(ledgerId string) error {
	_, err := svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerId),
			},
		},
	})

	return err
}

func AppendLedgerMediaEvents(ledgerId string, mediaEvents []tables.MediaEvent) error {
	var err error
	retryCount := 0
	maxRetries := env.GetEnvConfigs().AppendLedgerMaxRetries
	minSeconds := env.GetEnvConfigs().AppendLedgerRetryDelaySec
	success := false
	canRetry := true
	for retryCount < maxRetries && !success && canRetry {
		err = appendLedgerMediaEvents(ledgerId, mediaEvents)
		retryCount++
		if err != nil && hasVersionConflict(err) {
			time.Sleep(time.Duration(powInt(minSeconds, retryCount)) * time.Second)
		} else if err != nil {
			log.Printf("error appending media event to ledger: %s", err)
			canRetry = false
		} else {
			success = true
		}
	}

	return err
}

func appendLedgerMediaEvents(ledgerId string, mediaEvents []tables.MediaEvent) error {
	ledgerItem, err := GetLedger(ledgerId)
	if err != nil {
		log.Printf("error fetching ledger: %s", err)
		return err
	}

	anyExistingMediaEvents, err := ledgerItem.GetExistingMediaEvents()
	if err != nil {
		log.Printf("error fetching existing media events: %s", err)
		return err
	}

	setEvents := joinMediaEventSet(anyExistingMediaEvents, mediaEvents)
	joinedEventsJson, err := json.Marshal(setEvents)
	if err != nil {
		log.Printf("error marshalling joined mediaEvents: %s", err)
		return err
	}
	ledgerItem.MediaEvents = string(joinedEventsJson)
	const fieldKeyScript = "MediaEvents"
	const versionKeyScript = "MediaEventsVersion"
	err = updateLedgerEvents(ledgerItem, fieldKeyScript, versionKeyScript)
	return err
}

func AppendLedgerPublishEvents(ledgerId string, publishEvents []tables.PublishEvent) error {
	var err error
	retryCount := 0
	maxRetries := env.GetEnvConfigs().AppendLedgerMaxRetries
	minSeconds := env.GetEnvConfigs().AppendLedgerRetryDelaySec
	success := false
	canRetry := true
	for retryCount < maxRetries && !success && canRetry {
		err = appendLedgerPublishEvents(ledgerId, publishEvents)
		retryCount++
		if err != nil && hasVersionConflict(err) {
			time.Sleep(time.Duration(powInt(minSeconds, retryCount)) * time.Second)
		} else if err != nil {
			log.Printf("error appending publish event to ledger: %s", err)
			canRetry = false
		} else {
			success = true
		}
	}

	return err
}

func appendLedgerPublishEvents(ledgerId string, publishEvents []tables.PublishEvent) error {
	ledgerItem, err := GetLedger(ledgerId)
	if err != nil {
		log.Printf("error fetching ledger: %s", err)
		return err
	}

	anyExistingPublishEvents, err := ledgerItem.GetExistingPublishEvents()
	if err != nil {
		log.Printf("error fetching existing publish events: %s", err)
		return err
	}

	setEvents := joinPublishEventSet(anyExistingPublishEvents, publishEvents)
	joinedEventsJson, err := json.Marshal(setEvents)
	if err != nil {
		log.Printf("error marshalling joined publishEvents: %s", err)
		return err
	}
	ledgerItem.PublishEvents = string(joinedEventsJson)
	const fieldKeyScript = "PublishEvents"
	const versionKeyScript = "PublishEventsVersion"
	err = updateLedgerEvents(ledgerItem, fieldKeyScript, versionKeyScript)
	return err
}

func hasVersionConflict(err error) bool {
	if err == nil {
		return false
	}
	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException
	}
	return false
}

func powInt(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}

func SetLedgerStatus(ledgerEntry tables.Ledger, status tables.LedgerStatus) error {
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerEntry.LedgerID),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: aws.String(string(status)),
			},
		},
		TableName:        aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		ReturnValues:     aws.String("NONE"),
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :r", "LedgerStatus")),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateLedgerEvents to set ledger status: %s", err)
	}
	return err
}

func updateLedgerEvents(ledgerEntry tables.Ledger, fieldKey string, versionKey string) error {
	updatedValue := getField(&ledgerEntry, fieldKey)
	// Check to see that no one updated before us.
	oldVersionNumber := getField(&ledgerEntry, versionKey).Int()
	newVersionNumber := oldVersionNumber + 1
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerEntry.LedgerID),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: aws.String(updatedValue.String()),
			},
			":v": {
				N: aws.String(strconv.FormatInt(newVersionNumber, 10)),
			},
			":ov": {
				N: aws.String(strconv.FormatInt(oldVersionNumber, 10)),
			},
		},
		TableName:           aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		ReturnValues:        aws.String("NONE"),
		UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :r, %s = :v", fieldKey, versionKey)),
		ConditionExpression: aws.String(fmt.Sprintf("%s = :ov", versionKey)),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("error calling updateLedgerEvents: %s", err)
	}
	return err
}

func getField(v *tables.Ledger, field string) reflect.Value {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f
}

func joinMediaEventSet(s1 []tables.MediaEvent, s2 []tables.MediaEvent) []tables.MediaEvent {
	result := []tables.MediaEvent{}
	existing := stringset.New()
	for _, e := range s1 {
		existing.Add(e.GetEventID())
		result = append(result, e)
	}

	for _, e := range s2 {
		if !existing.Contains(e.GetEventID()) {
			result = append(result, e)
		}
	}
	return result
}

func joinPublishEventSet(s1 []tables.PublishEvent, s2 []tables.PublishEvent) []tables.PublishEvent {
	result := []tables.PublishEvent{}
	existing := stringset.New()
	for _, e := range s1 {
		existing.Add(e.GetEventID())
		existing.Add(e.GetRootMediaAssignmentKey())
		result = append(result, e)
	}

	for _, e := range s2 {
		if !existing.Contains(e.GetEventID()) {
			result = append(result, e)
		}
	}
	return result
}

func IncrementHeartbeat(ledgerEntry tables.Ledger) {
	const maxHeartbeat = 100
	if ledgerEntry.HeartbeatCount >= maxHeartbeat {
		log.Printf("correlationID: %s max heartbeat exceeded retuning nil noop", ledgerEntry.LedgerID)
	}
	// Prevent system spam messages onto diff queue.
	time.Sleep(time.Duration(ledgerEntry.HeartbeatCount) * time.Minute)

	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerEntry.LedgerID),
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {
				N: aws.String(strconv.FormatInt(1, 10)),
			},
		},
		TableName:        aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		ReturnValues:     aws.String("NONE"),
		UpdateExpression: aws.String(fmt.Sprintf("ADD %s :v", "HeartbeatCount")),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Printf("correlationID: %s WARN error incrementing heartbeat: %s", ledgerEntry.LedgerID, err)
	}
}
