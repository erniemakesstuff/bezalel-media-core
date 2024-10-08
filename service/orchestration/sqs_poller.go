package orchestration

import (
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	sqs_model "github.com/bezalel-media-core/v2/service/models"
)

var sqs_svc = sqs.New(aws_configuration.GetAwsSession())

const queue_name = "ledger-queue"                    // os.Getenv("LEDGER_SQS_NAME")
const visibility_timeout = 180                       // seconds
const time_milliseconds_between_message_polls = 1000 // TODO re-evaluate
const max_messages_per_poll = 10
const max_concurrent_process_consumers = 1 // TODO: Update this, should be around 100

func PollForLedgerUpdates() {
	urlResult, err := sqs_svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queue_name),
	})
	if err != nil {
		log.Fatalf("failed to get queue url: %s", err)
	}
	queueURL := urlResult.QueueUrl
	log.Printf("QUEUE URL: %s", *queueURL)
	for i := 0; i < max_concurrent_process_consumers; i++ {
		go startConsumer(queueURL)
	}
}

func Purge() {
	// TODO: Add env config check to ensure this doesn't run in prod.
	urlResult, err := sqs_svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queue_name),
	})
	if err != nil {
		log.Fatalf("failed to get queue url: %s", err)
	}
	_, err = sqs_svc.PurgeQueue(&sqs.PurgeQueueInput{
		QueueUrl: urlResult.QueueUrl,
	})
	if err != nil {
		log.Fatalf("failed to purge queue url: %s", err)
	}
}

func startConsumer(queueURL *string) {
	log.Printf("started consumer")
	for {
		err := consumeMessages(queueURL)
		time.Sleep(time.Duration(time_milliseconds_between_message_polls) * time.Millisecond)
		if err != nil {
			log.Printf("failed to poll queue messages: %s", err)
		}
	}
}

func consumeMessages(queueURL *string) error {
	msgResult, err := sqs_svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(max_messages_per_poll),
		VisibilityTimeout:   aws.Int64(visibility_timeout),
	})
	if err != nil {
		return err
	}
	if len(msgResult.Messages) > 0 {
		processMessages(msgResult.Messages, queueURL)
	}
	return err
}

func processMessages(messages []*sqs.Message, queueUrl *string) {
	var wg sync.WaitGroup
	for _, m := range messages {
		wg.Add(1)
		go asyncProcessMessage(m, queueUrl, &wg)
	}
	wg.Wait()
}

func asyncProcessMessage(message *sqs.Message, queueUrl *string, wg *sync.WaitGroup) {
	err := executeRelevantWorkflow(message)
	if err != nil {
		log.Printf("unable to execute workflow for event: %s %s", *message.MessageId, err)
		wg.Done()
		return
	}
	err = ackMessage(message, queueUrl)
	if err != nil {
		log.Printf("unalbe to ack event: %s %s", message.GoString(), err)
	}
	wg.Done()
}

func executeRelevantWorkflow(message *sqs.Message) error {
	ledgerItem, err := decode(message)
	if err != nil {
		return err
	}
	log.Printf("correlationID: %s, executing workflow", ledgerItem.LedgerID)
	return RunWorkflows(ledgerItem)
}

func ackMessage(message *sqs.Message, queueUrl *string) error {
	_, err := sqs_svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: message.ReceiptHandle,
	})
	return err
}

func decode(message *sqs.Message) (tables.Ledger, error) {
	var sqsMessage sqs_model.SQSMessage
	err := json.Unmarshal([]byte(*message.Body), &sqsMessage)
	if err != nil {
		log.Printf("failed to unmarshall sqs body: %s", err)
		return tables.Ledger{}, err
	}
	isS3Event := strings.Contains(sqsMessage.Message, "aws:s3")
	if isS3Event {
		return decodeS3Event(sqsMessage)
	} else {
		return decodeDynamoEvent(sqsMessage)
	}
}

func transformDynamoEventToLedger(cdc sqs_model.DynamoCDC) tables.Ledger {
	resultItem := tables.Ledger{
		LedgerID: cdc.Dynamodb.Keys.LedgerID.S,
	}
	return resultItem
}

func transformS3EventToLedger(cdc sqs_model.S3CDC) (tables.Ledger, error) {
	if len(cdc.Records) == 0 {
		return tables.Ledger{}, errors.New("empty s3 event given, no records")
	}
	key := cdc.Records[0].S3.Object.Key
	contentLookupKeySegments := strings.Split(key, ".")
	if len(contentLookupKeySegments) < 3 {
		log.Printf("malformed s3-media-bucket key, exptect 3, was: %d for key: %s", len(contentLookupKeySegments), key)
		return tables.Ledger{}, errors.New("malformed s3 key:" + key)
	}
	// If you set the mediaEventsVersion to anything BUT 0,
	// your message will be ignored. See orchestration_service stale check.
	const index_ledger_id = 1
	resultItem := tables.Ledger{
		LedgerID: contentLookupKeySegments[index_ledger_id],
	}
	return resultItem, nil
}

func decodeDynamoEvent(sqsMessage sqs_model.SQSMessage) (tables.Ledger, error) {
	var streamMessage sqs_model.DynamoCDC
	err := json.Unmarshal([]byte(sqsMessage.Message), &streamMessage)
	if err != nil {
		log.Printf("failed to unmarshall sqs message: %s", err)
		return tables.Ledger{}, err
	}
	ledgerItem := transformDynamoEventToLedger(streamMessage)
	return ledgerItem, err
}

func decodeS3Event(sqsMessage sqs_model.SQSMessage) (tables.Ledger, error) {
	var streamMessage sqs_model.S3CDC
	err := json.Unmarshal([]byte(sqsMessage.Message), &streamMessage)
	if err != nil {
		log.Printf("failed to unmarshall sqs message: %s", err)
		return tables.Ledger{}, err
	}
	ledgerItem, err := transformS3EventToLedger(streamMessage)
	return ledgerItem, err
}
