package dal

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"bitbucket.org/creachadair/stringset"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	dynamo_tables "github.com/bezalel-media-core/v2/dal/tables/v1"

	"log"
	"reflect"
	"time"
)

var svc = dynamodb.New(aws_configuration.GetAwsSession())

const start_version = 0

func CreateLedger(item dynamo_tables.Ledger) error {
	item.MediaEventsVersion = start_version
	item.ScriptEventsVersion = start_version
	item.PublishEventsVersion = start_version
	item.LedgerStatus = dynamo_tables.NEW_LEDGER
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

func GetLedger(ledgerId string) (dynamo_tables.Ledger, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerId),
			},
		},
	})

	resultItem := dynamo_tables.Ledger{}
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

func AppendLedgerScriptEvents(ledgerId string, scriptEvents []dynamo_tables.ScriptEvent) error {
	var err error
	retryCount := 0
	const maxRetries = 5
	const minSeconds = 2
	success := false
	canRetry := true
	for retryCount < maxRetries && !success && canRetry {
		err = appendLedgerScriptEvents(ledgerId, scriptEvents)
		retryCount++
		if err != nil && hasVersionConflict(err) {
			time.Sleep(time.Duration(powInt(minSeconds, retryCount)) * time.Second)
		} else if err != nil {
			log.Printf("error appending event to ledger: %s", err)
			canRetry = false
		} else {
			success = true
		}
	}

	return err
}

func appendLedgerScriptEvents(ledgerId string, scriptEvents []dynamo_tables.ScriptEvent) error {
	ledgerItem, err := GetLedger(ledgerId)
	if err != nil {
		log.Printf("error fetching ledger: %s", err)
		return err
	}

	anyExistingScriptEvents, err := getExistingScriptEvents(ledgerItem)
	if err != nil {
		log.Printf("error fetching existing script events: %s", err)
		return err
	}

	setEvents := joinScriptEventSet(anyExistingScriptEvents, scriptEvents)
	joinedEventsJson, err := json.Marshal(setEvents)
	if err != nil {
		log.Printf("error marshalling joined scriptEvents: %s", err)
		return err
	}
	ledgerItem.ScriptEvents = string(joinedEventsJson)
	const fieldKeyScript = "ScriptEvents"
	const versionKeyScript = "ScriptEventsVersion"
	err = updateLedgerEvents(ledgerItem, fieldKeyScript, versionKeyScript)
	return err
}

func appendLedgerMediaEvents() error {
	// TODO: Conditional updates
	return nil
}

func appendLedgerPublishEvents() error {
	// TODO: Conditional updates
	return nil
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

func updateLedgerEvents(ledgerEntry dynamo_tables.Ledger, fieldKey string, versionKey string) error {

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
		log.Fatalf("error calling UpdateItem: %s", err)
	}
	return err
}

func getField(v *dynamo_tables.Ledger, field string) reflect.Value {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f
}

func joinScriptEventSet(s1 []dynamo_tables.ScriptEvent, s2 []dynamo_tables.ScriptEvent) []dynamo_tables.ScriptEvent {
	result := []dynamo_tables.ScriptEvent{}
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

func joinMediaEventSet(s1 []dynamo_tables.MediaEvent, s2 []dynamo_tables.MediaEvent) []dynamo_tables.MediaEvent {
	result := []dynamo_tables.MediaEvent{}
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

func joinPublishEventSet(s1 []dynamo_tables.PublishEvent, s2 []dynamo_tables.PublishEvent) []dynamo_tables.PublishEvent {
	result := []dynamo_tables.PublishEvent{}
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

func getExistingScriptEvents(ledgerItem dynamo_tables.Ledger) ([]dynamo_tables.ScriptEvent, error) {
	var existingScriptEvents []dynamo_tables.ScriptEvent
	if ledgerItem.ScriptEvents == "" {
		return existingScriptEvents, nil
	}

	err := json.Unmarshal([]byte(ledgerItem.ScriptEvents), &existingScriptEvents)
	if err != nil {
		log.Printf("error unmarshalling scriptEvents: %s", err)
		return existingScriptEvents, err
	}
	return existingScriptEvents, err
}
