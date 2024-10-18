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

var PubProfile_EN_Medium_1 = tables.AccountPublisher{
	AccountID:                 "TestPublisherUser1",
	PublisherProfileID:        "MediumProfileId1",
	ChannelName:               tables.Channel_Medium,
	LastPublishAtEpochMilli:   0,
	AccountSubscriptionStatus: tables.EVERGREEN_ADMIN,
	PublisherNiche:            "TestingNiche",
	PublisherLanguage:         "EN",
	PublisherAPISecretID:      "TrueVineAIToken",
	// TODO: Inject api secret when testing.
}

func setupTest() {
	once.Do(func() {
		os.Chdir("../../..") // For manifest file loads.
		dynamo_configuration.Init()
		manifest.GetManifestLoader()
	})
}

func cleanupTestData() {
	err := dal.DeletePublisherAccount(PubProfile_EN_Medium_1.AccountID, PubProfile_EN_Medium_1.PublisherProfileID)
	if err != nil {
		log.Fatalf("failed to delete publisher account: %s", err)
	}
}

func TestMediumPublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		AccountID:          PubProfile_EN_Medium_1.AccountID,
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

func TestTwitterPublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		AccountID:          Live_EN_Twitter_1.AccountID,
		PublisherProfileID: Live_EN_Twitter_1.PublisherProfileID,
		LedgerID:           "INTEG-TestTwitterPublish",
	}
	cmd := PublishCommand{
		RootPublishEvent: pubEvent,
	}
	driver := TwitterDriver{}
	err := driver.Publish(cmd)
	if err != nil {
		log.Printf("publisher error: %s", err)
	}
	cleanupTestData()
}

func TestRedditPublish(t *testing.T) {
	setupTest()
	pubEvent := tables.PublishEvent{
		AccountID:          Live_EN_Reddit_1.AccountID,
		PublisherProfileID: Live_EN_Reddit_1.PublisherProfileID,
		LedgerID:           "INTEG-TestRedditPublish",
	}
	cmd := PublishCommand{
		RootPublishEvent: pubEvent,
	}
	driver := RedditDriver{}
	err := driver.Publish(cmd)
	if err != nil {
		log.Printf("publisher error: %s", err)
	}
	cleanupTestData()
}
