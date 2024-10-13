package publisherdrivers

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	aws_configuration "github.com/bezalel-media-core/v2/configuration"
)

var s3_downloader = s3manager.NewDownloader(aws_configuration.GetAwsSession())

const media_bucket_name = "truevine-media-storage" // os.Getenv("media_bucket")

func LoadAsString(contentLookupKey string) (string, error) {
	file, err := os.Create(contentLookupKey)
	if err != nil {
		log.Printf("%s error creating temp file: %s", contentLookupKey, err)
		return "", err
	}
	_, err = s3_downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(media_bucket_name),
			Key:    aws.String(contentLookupKey),
		})
	if err != nil {
		log.Printf("error checking media existence: %s", err)
		return "", err
	}

	b, err := os.ReadFile(contentLookupKey)
	if err != nil {
		log.Printf("%s error reading temp file: %s", contentLookupKey, err)
		return "", err
	}

	return string(b), nil
}
