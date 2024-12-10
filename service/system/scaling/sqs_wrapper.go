package scaling

import (
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	config "github.com/bezalel-media-core/v2/configuration"
)

var sqs_svc = sqs.New(config.GetAwsSession())

func getPendingMessagesCount(queueName string) (int, error) {
	urlResult, err := sqs_svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		log.Fatalf("failed to get queue url: %s", err)
	}
	queueURL := urlResult.QueueUrl

	resp, err := sqs_svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameApproximateNumberOfMessages),
			aws.String(sqs.QueueAttributeNameApproximateNumberOfMessagesNotVisible),
		},
		QueueUrl: queueURL,
	})
	if err != nil {
		return 0, err
	}

	inFlightCount, err := strconv.Atoi(*resp.Attributes[sqs.QueueAttributeNameApproximateNumberOfMessagesNotVisible])
	if err != nil {
		return 0, err
	}
	pendingCount, err := strconv.Atoi(*resp.Attributes[sqs.QueueAttributeNameApproximateNumberOfMessages])
	if err != nil {
		return 0, err
	}
	return inFlightCount + pendingCount, nil
}
