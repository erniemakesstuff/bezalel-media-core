package service

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
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
			log.Printf("unalbe to execute workflow for event: %s", m.GoString())
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
	// TODO
	return nil
}
