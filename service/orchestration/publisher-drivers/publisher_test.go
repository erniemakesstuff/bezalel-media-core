package publisherdrivers

import (
	"log"
	"os"
	"sync"
	"testing"

	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
)

var once sync.Once

// Try not to delete test accounts :)
var PubProfile_EN_Medium_1 = tables.AccountPublisher{
	AccountID:                 "TestPublisherUser1",
	PublisherProfileID:        "MediumProfileId1",
	ChannelName:               tables.Channel_Medium,
	LastPublishAtEpochMilli:   0,
	AccountSubscriptionStatus: tables.EVERGREEN_ADMIN,
	PublisherNiche:            "TestingNiche",
	PublisherLanguage:         "EN",
	PublisherAPISecretID:      "TrueVineAIToken",
}

func setupTest() {
	once.Do(func() {
		os.Chdir("../../..") // For manifest file loads.
		dynamo_configuration.Init()
		manifest.GetManifestLoader()
	})
}

func cleanupTestData() {
	err := dal.ForceAllLocksFree(PubProfile_EN_Medium_1.AccountID, PubProfile_EN_Medium_1.PublisherProfileID)
	if err != nil {
		log.Fatalf("failed to release locks on cleanup: %s", err)
	}
}

func TestPublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		OwnerAccountID:     PubProfile_EN_Medium_1.AccountID,
		PublisherProfileID: PubProfile_EN_Medium_1.PublisherProfileID,
	}
	cmd := PublishCommand{
		RootPublishEvent: pubEvent,
	}
	driver := MediumDriver{}
	err := driver.Publish(cmd)
	if err != nil {
		log.Printf("publisher error: %s", err)
	}
	cleanupTestData()
}
