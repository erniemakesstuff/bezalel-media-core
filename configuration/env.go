package configuration

import (
	"log"
	"os"
	"sync"

	"gopkg.in/yaml.v2"
)

type EnvConfigVals struct {
	DefaultPublisherWatermarkText string `yaml:"DefaultPublisherWatermarkText"`
	AssignmentLockMilliTTL        int64  `yaml:"AssignmentLockMilliTTL"`
	PublishLockMilliTTL           int64  `yaml:"PublishLockMilliTTL"`
	AppendLedgerMaxRetries        int    `yaml:"AppendLedgerMaxRetries"`
	AppendLedgerRetryDelaySec     int    `yaml:"AppendLedgerRetryDelaySec"`
	LedgerQueueName               string `yaml:"LedgerQueueName"`
	PollVisibilityTimeoutSec      int64  `yaml:"PollVisibilityTimeoutSec"`
	PollPeriodMilli               int64  `yaml:"PollPeriodMilli"`
	MaxMessagesPerPoll            int64  `yaml:"MaxMessagesPerPoll"`
	MaxConsumers                  int    `yaml:"MaxConsumers"`
	SNSMediaTopic                 string `yaml:"SNSMediaTopic"`
}

var configSync sync.Once
var EnvConfigs *EnvConfigVals

func GetEnvConfigs() *EnvConfigVals {
	if EnvConfigs != nil {
		return EnvConfigs
	}
	configSync.Do(func() {
		var configFile []byte
		var err error
		if os.Getenv("env") == "" || os.Getenv("env") != "prod" {
			configFile, err = os.ReadFile("./configuration/env-dev.yml")
		} else {
			configFile, err = os.ReadFile("./configuration/env-prod.yml")
		}

		if err != nil {
			log.Fatalf("failed to load config file: %s", err)
		}

		var vals EnvConfigVals
		err = yaml.Unmarshal(configFile, &vals)
		if err != nil {
			log.Fatalf("failed to unmarshall config file values: %s", err)
		}
		EnvConfigs = &vals
	})
	return EnvConfigs
}
