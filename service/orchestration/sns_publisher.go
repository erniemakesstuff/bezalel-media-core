package orchestration

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

var snsSvc = sns.New(aws_configuration.GetAwsSession())

const SNS_MEDIA_TOPIC = "arn:aws:sns:us-west-2:971422718801:media-topic"

type Message struct {
	Default string `json:"default"`
}

func PublishMediaTopicSns(mediaEvent tables.MediaEvent) error {
	mediaBytes, err := json.Marshal(mediaEvent)
	if err != nil {
		log.Printf("error marhsalling media event: %s", err)
		return err
	}
	mediaMessage := string(mediaBytes)
	snsMessage := Message{
		Default: mediaMessage,
	}
	snsMessageBytes, err := json.Marshal(snsMessage)
	if err != nil {
		log.Printf("error marhsalling media event wrapper: %s", err)
		return err
	}
	snsMessageWrapper := string(snsMessageBytes)
	topicArn := SNS_MEDIA_TOPIC
	_, err = snsSvc.Publish(&sns.PublishInput{
		Message:          &snsMessageWrapper,
		TopicArn:         &topicArn,
		MessageStructure: aws.String("json"),

		MessageAttributes: map[string]*sns.MessageAttributeValue{
			"filterKey": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(mediaEvent.MediaType)),
			},
		},
	})
	if err != nil {
		log.Printf("failed publishing to media sns topic: %s", err)
		return err
	}

	return err
}
