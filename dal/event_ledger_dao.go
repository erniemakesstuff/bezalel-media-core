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
	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	ledger_table "github.com/bezalel-media-core/v2/dal/tables/v1"

	"log"
	"reflect"
	"time"
)

func CreateLedger(item ledger_table.Ledger) error {
	item.MediaEventsVersion = start_version
	item.ScriptEventsVersion = start_version
	item.PublishEventsVersion = start_version
	item.LedgerStatus = ledger_table.NEW_LEDGER
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

func GetLedger(ledgerId string) (ledger_table.Ledger, error) {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(dynamo_configuration.TABLE_EVENT_LEDGER),
		Key: map[string]*dynamodb.AttributeValue{
			"LedgerID": {
				S: aws.String(ledgerId),
			},
		},
	})

	resultItem := ledger_table.Ledger{}
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

func AppendLedgerScriptEvents(ledgerId string, scriptEvents []ledger_table.ScriptEvent) error {
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
			log.Printf("error appending script event to ledger: %s", err)
			canRetry = false
		} else {
			success = true
		}
	}

	return err
}

func AppendLedgerMediaEvents(ledgerId string, mediaEvents []ledger_table.MediaEvent) error {
	var err error
	retryCount := 0
	const maxRetries = 5
	const minSeconds = 2
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

func AppendLedgerPublishEvents(ledgerId string, publishEvents []ledger_table.PublishEvent) error {
	var err error
	retryCount := 0
	const maxRetries = 5
	const minSeconds = 2
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

func appendLedgerScriptEvents(ledgerId string, scriptEvents []ledger_table.ScriptEvent) error {
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

func appendLedgerMediaEvents(ledgerId string, mediaEvents []ledger_table.MediaEvent) error {
	ledgerItem, err := GetLedger(ledgerId)
	if err != nil {
		log.Printf("error fetching ledger: %s", err)
		return err
	}

	anyExistingMediaEvents, err := getExistingMediaEvents(ledgerItem)
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
	ledgerItem.ScriptEvents = string(joinedEventsJson)
	const fieldKeyScript = "MediaEvents"
	const versionKeyScript = "MediaEventsVersion"
	err = updateLedgerEvents(ledgerItem, fieldKeyScript, versionKeyScript)
	return err
}

func appendLedgerPublishEvents(ledgerId string, publishEvents []ledger_table.PublishEvent) error {
	ledgerItem, err := GetLedger(ledgerId)
	if err != nil {
		log.Printf("error fetching ledger: %s", err)
		return err
	}

	anyExistingPublishEvents, err := getExistingPublishEvents(ledgerItem)
	if err != nil {
		log.Printf("error fetching existing media events: %s", err)
		return err
	}

	setEvents := joinPublishEventSet(anyExistingPublishEvents, publishEvents)
	joinedEventsJson, err := json.Marshal(setEvents)
	if err != nil {
		log.Printf("error marshalling joined publishEvents: %s", err)
		return err
	}
	ledgerItem.ScriptEvents = string(joinedEventsJson)
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

func updateLedgerEvents(ledgerEntry ledger_table.Ledger, fieldKey string, versionKey string) error {

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

func getField(v *ledger_table.Ledger, field string) reflect.Value {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f
}

func joinScriptEventSet(s1 []ledger_table.ScriptEvent, s2 []ledger_table.ScriptEvent) []ledger_table.ScriptEvent {
	result := []ledger_table.ScriptEvent{}
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

func joinMediaEventSet(s1 []ledger_table.MediaEvent, s2 []ledger_table.MediaEvent) []ledger_table.MediaEvent {
	result := []ledger_table.MediaEvent{}
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

func joinPublishEventSet(s1 []ledger_table.PublishEvent, s2 []ledger_table.PublishEvent) []ledger_table.PublishEvent {
	result := []ledger_table.PublishEvent{}
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

func getExistingScriptEvents(ledgerItem ledger_table.Ledger) ([]ledger_table.ScriptEvent, error) {
	var existingScriptEvents []ledger_table.ScriptEvent
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
func getExistingMediaEvents(ledgerItem ledger_table.Ledger) ([]ledger_table.MediaEvent, error) {
	var existingMediaEvents []ledger_table.MediaEvent
	if ledgerItem.ScriptEvents == "" {
		return existingMediaEvents, nil
	}

	err := json.Unmarshal([]byte(ledgerItem.PublishEvents), &existingMediaEvents)
	if err != nil {
		log.Printf("error unmarshalling mediaEvents: %s", err)
		return existingMediaEvents, err
	}
	return existingMediaEvents, err
}

func getExistingPublishEvents(ledgerItem ledger_table.Ledger) ([]ledger_table.PublishEvent, error) {
	var existingPublishEvents []ledger_table.PublishEvent
	if ledgerItem.ScriptEvents == "" {
		return existingPublishEvents, nil
	}

	err := json.Unmarshal([]byte(ledgerItem.PublishEvents), &existingPublishEvents)
	if err != nil {
		log.Printf("error unmarshalling publishEvents: %s", err)
		return existingPublishEvents, err
	}
	return existingPublishEvents, err
}
