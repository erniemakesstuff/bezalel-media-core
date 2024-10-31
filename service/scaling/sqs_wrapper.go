package scaling

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	config "github.com/bezalel-media-core/v2/configuration"
)

var sqs_svc = sqs.New(config.GetAwsSession())

var queue_name = config.GetEnvConfigs().LedgerQueueName
var visibility_timeout = config.GetEnvConfigs().PollVisibilityTimeoutSec
var time_milliseconds_between_message_polls = config.GetEnvConfigs().PollPeriodMilli
var max_messages_per_poll = config.GetEnvConfigs().MaxMessagesPerPoll // max size 10
var max_concurrent_process_consumers = config.GetEnvConfigs().MaxConsumers

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
		go consumeMessages(queueURL)
	}
}

func consumeMessages(queueURL *string) error {
	_, err := sqs_svc.ReceiveMessage(&sqs.ReceiveMessageInput{
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
	return err
}
