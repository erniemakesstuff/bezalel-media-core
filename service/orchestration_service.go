package service

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
	ledger "github.com/bezalel-media-core/v2/dal/tables/v1"
	sqs_model "github.com/bezalel-media-core/v2/service/models"
)

var sqs_svc = sqs.New(aws_configuration.GetAwsSession())

const queue_name = "ledger-queue"                    // os.Getenv("LEDGER_SQS_NAME")
const visibility_timeout = 180                       // seconds
const time_milliseconds_between_message_polls = 1000 // TODO re-evaluate
const max_messages_per_poll = 1                      // TODO re-evaluate

func PollForLedgerUpdates() {
	urlResult, err := sqs_svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queue_name),
	})
	if err != nil {
		log.Fatalf("failed to get queue url: %s", err)
	}
	queueURL := urlResult.QueueUrl
	log.Printf("QUEUE URL: %s", *queueURL)
	for {
		err = consumeMessages(queueURL)
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
	log.Printf("POLLED %d", len(msgResult.Messages))
	if len(msgResult.Messages) > 0 {
		processMessages(msgResult.Messages, queueURL)
	}
	return err
}

func processMessages(messages []*sqs.Message, queueUrl *string) {
	for _, m := range messages {
		err := executeRelevantWorkflow(m)
		if err != nil {
			log.Printf("unable to execute workflow for event: %s %s", *m.MessageId, err)
			continue
		}
		err = ackMessage(m, queueUrl)
		if err != nil {
			log.Printf("unalbe to ack event: %s %s", m.GoString(), err)
		}
	}
}

func ackMessage(message *sqs.Message, queueUrl *string) error {
	_, err := sqs_svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: message.ReceiptHandle,
	})
	return err
}

func executeRelevantWorkflow(message *sqs.Message) error {
	ledgerItem, err := decode(message)
	if err != nil {
		return err
	}
	log.Printf("PAYLOAD: %s", ledgerItem.LedgerID)
	return nil
}

func decode(message *sqs.Message) (ledger.Ledger, error) {
	var sqsMessage sqs_model.SQSMessage
	err := json.Unmarshal([]byte(*message.Body), &sqsMessage)
	if err != nil {
		log.Printf("failed to unmarshall sqs body: %s", err)
		return ledger.Ledger{}, err
	}
	var streamMessage sqs_model.DynamoCDC
	err = json.Unmarshal([]byte(sqsMessage.Message), &streamMessage)
	if err != nil {
		log.Printf("failed to unmarshall sqs message: %s", err)
		return ledger.Ledger{}, err
	}
	ledgerItem, err := transformToLedger(streamMessage)
	return ledgerItem, err
}

func transformToLedger(cdc sqs_model.DynamoCDC) (ledger.Ledger, error) {
	createdAtTime, err := strconv.ParseInt(cdc.Dynamodb.NewImage.LedgerCreatedAtEpochMilli.N, 10, 64)
	if err != nil {
		log.Printf("failed to parse ledger numerics: %s", err)
		return ledger.Ledger{}, err
	}
	mediaVersion, err := strconv.Atoi(cdc.Dynamodb.NewImage.MediaEventsVersion.N)
	if err != nil {
		log.Printf("failed to parse ledger numerics: %s", err)
		return ledger.Ledger{}, err
	}
	scriptVersion, err := strconv.Atoi(cdc.Dynamodb.NewImage.ScriptEventsVersion.N)
	if err != nil {
		log.Printf("failed to parse ledger numerics: %s", err)
		return ledger.Ledger{}, err
	}
	publishVersion, err := strconv.Atoi(cdc.Dynamodb.NewImage.PublishEventsVersion.N)
	if err != nil {
		log.Printf("failed to parse ledger numerics: %s", err)
		return ledger.Ledger{}, err
	}

	resultItem := ledger.Ledger{
		LedgerID:                  cdc.Dynamodb.Keys.LedgerID.S,
		LedgerStatus:              ledger.LedgerStatus(cdc.Dynamodb.NewImage.LedgerStatus.S),
		LedgerCreatedAtEpochMilli: createdAtTime,
		RawEventPayload:           cdc.Dynamodb.NewImage.RawEventPayload.S,
		RawEventSource:            cdc.Dynamodb.NewImage.RawEventSource.S,
		RawEventMediaUrls:         cdc.Dynamodb.NewImage.RawEventMediaUrls.S,
		RawEventWebsiteUrls:       cdc.Dynamodb.NewImage.RawEventWebsiteUrls.S,
		RawEventLanguage:          cdc.Dynamodb.NewImage.RawEventLanguage.S,
		RawContentHash:            cdc.Dynamodb.NewImage.RawContentHash.S,
		MediaEvents:               cdc.Dynamodb.NewImage.MediaEvents.S,
		ScriptEvents:              cdc.Dynamodb.NewImage.ScriptEvents.S,
		PublishEvents:             cdc.Dynamodb.NewImage.PublishEvents.S,
		MediaEventsVersion:        int64(mediaVersion),
		ScriptEventsVersion:       int64(scriptVersion),
		PublishEventsVersion:      int64(publishVersion),
	}
	return resultItem, err
}
