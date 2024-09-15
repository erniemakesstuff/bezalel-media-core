package configuration

import (
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

var sessInst *session.Session
var once sync.Once

func GetAwsSession() *session.Session {
	if sessInst != nil {
		return sessInst
	}
	once.Do(func() {
		creds := credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
		sess, err := session.NewSession(&aws.Config{
			Region:      aws.String(os.Getenv("AWS_REGION")),
			Credentials: creds,
		})
		if err != nil {
			panic(err)
		}
		sessInst = sess
	})

	return sessInst
}
