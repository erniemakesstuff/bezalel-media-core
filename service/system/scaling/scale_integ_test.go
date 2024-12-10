package scaling

import (
	"os"
	"sync"
	"testing"
	"time"

	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	manifest "github.com/bezalel-media-core/v2/manifest"
	"github.com/stretchr/testify/assert"
)

var once sync.Once

func setupTest() {
	once.Do(func() {
		os.Chdir("../..") // For manifest file loads.
		dynamo_configuration.Init()
		manifest.GetManifestLoader()
	})
}

func TestScaling(t *testing.T) {
	setupTest()
	clusterName := "fargate-test"
	taskName := "arn:aws:ecs:us-west-2:971422718801:task-definition/media-text:14"
	err := ScaleEcsTasks(clusterName, 3, taskName)
	assert.Nil(t, err, "expected Scale-up error to be nil")
	time.Sleep(3 * time.Minute) // Wait for scale-up to finish.
	err = ScaleEcsTasks(clusterName, 0, taskName)
	assert.Nil(t, err, "expected Scale-down error to be nil")
}
