package publisherdrivers

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	configs "github.com/bezalel-media-core/v2/configuration"
)

var s3_downloader = s3manager.NewDownloader(configs.GetAwsSession())

func LoadAsString(contentLookupKey string) (string, error) {
	b, err := LoadAsBytes(contentLookupKey)
	return string(b), err
}

func LoadAsBytes(contentLookupKey string) ([]byte, error) {
	err := DownloadFile(contentLookupKey)
	if err != nil {
		log.Printf("error checking %s media existence within LoadAsString: %s", contentLookupKey, err)
		os.Remove(contentLookupKey)
		return []byte{}, err
	}

	b, err := os.ReadFile(contentLookupKey)
	if err != nil {
		log.Printf("%s error reading temp file: %s", contentLookupKey, err)
		return []byte{}, err
	}
	err = os.Remove(contentLookupKey)
	if err != nil {
		log.Printf("%s error cleaning-up file: %s", contentLookupKey, err)
		return []byte{}, err
	}

	return b, nil
}

func DownloadFile(contentLookupKey string) error {
	file, err := os.Create(contentLookupKey)
	if err != nil {
		log.Printf("%s error creating temp file: %s", contentLookupKey, err)
		return err
	}

	_, err = s3_downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(configs.GetEnvConfigs().S3MediaBucket),
			Key:    aws.String(contentLookupKey),
		})
	return err
}
