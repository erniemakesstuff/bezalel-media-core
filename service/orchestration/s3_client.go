package orchestration

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	configs "github.com/bezalel-media-core/v2/configuration"
)

var s3_svc = s3.New(configs.GetAwsSession())

func MediaExists(contentLookupKey string) (bool, error) {
	_, err := s3_svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(configs.GetEnvConfigs().S3MediaBucket),
		Key:    aws.String(contentLookupKey),
	})

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() == "NotFound" {
			// Eventually consistent.
			return false, nil
		}
	}
	if err != nil {
		log.Printf("error checking %s media existence within MediaExists: %s", contentLookupKey, err)
		return false, err
	}
	return true, nil
}

func listObjs() {
	resp, _ := s3_svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(configs.GetEnvConfigs().S3MediaBucket)})

	for _, item := range resp.Contents {
		log.Println("Name:         ", *item.Key)
		log.Println("Last modified:", *item.LastModified)
		log.Println("Size:         ", *item.Size)
		log.Println("Storage class:", *item.StorageClass)
		log.Println("")
	}
}
