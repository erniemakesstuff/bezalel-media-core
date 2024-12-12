package publisherdrivers

import (
	"fmt"
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
	err := tryDownloadWithRetry(contentLookupKey, 0)
	if err != nil {
		log.Printf("%s error downloading file: %s", contentLookupKey, err)
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

func tryDownloadWithRetry(contentLookupKey string, retry int) error {
	const maxRetry = 3
	if retry > maxRetry {
		return fmt.Errorf("max download retries exceeded for file: %s", contentLookupKey)
	}
	err := DownloadFile(contentLookupKey)
	if err != nil {
		log.Printf("error checking %s media existence within LoadAsString: %s", contentLookupKey, err)
		os.Remove(contentLookupKey)
		return err
	}
	// Seeing a race condition between downloading, and reading...
	if _, err = os.Stat(contentLookupKey); err != nil {
		log.Printf("error checking %s file doesn't exist after download, retrying: %s", contentLookupKey, err)
		return tryDownloadWithRetry(contentLookupKey, retry+1)
	}
	return err
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
