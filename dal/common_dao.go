package dal

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
)

var svc = dynamodb.New(aws_configuration.GetAwsSession())

const start_version = 0
