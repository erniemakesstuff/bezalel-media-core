package orchestration

import (
	"encoding/json"
	"log"
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
	log.Printf("correlationID: %s, executing workflow", ledgerItem.LedgerID)
	if err != nil {
		return err
	}
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
	var streamMessage sqs_model.DynamoCDC
	err = json.Unmarshal([]byte(sqsMessage.Message), &streamMessage)
	if err != nil {
		log.Printf("failed to unmarshall sqs message: %s", err)
		return tables.Ledger{}, err
	}
	ledgerItem := transformToLedger(streamMessage)
	return ledgerItem, err
}

func transformToLedger(cdc sqs_model.DynamoCDC) tables.Ledger {
	resultItem := tables.Ledger{
		LedgerID: cdc.Dynamodb.Keys.LedgerID.S,
	}
	return resultItem
}
