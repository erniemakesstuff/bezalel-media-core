package orchestration

import (
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"encoding/json"

	"github.com/stretchr/testify/assert"

	dynamo_configuration "github.com/bezalel-media-core/v2/configuration/dynamo"
	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
)

var once sync.Once

var Test_PublisherProfile_Medium = tables.AccountPublisher{
	AccountID:                 "TestUser",
	PublisherProfileID:        "MediumProfileId",
	ChannelName:               tables.Channel_Medium,
	LastPublishAtEpochMilli:   0,
	AccountSubscriptionStatus: tables.EVERGREEN_ADMIN,
	PublisherNiche:            "Blog",
}

var Test_Ledger_Blog = tables.Ledger{
	LedgerID:        "TestLedgerId",
	LedgerStatus:    tables.NEW_LEDGER,
	RawEventSource:  "v1/source/blog",
	RawEventPayload: "The weather in Seattle is fair.",
}

func setupTest() {
	once.Do(func() {
		os.Chdir("../..") // For manifest file loads.
		PollForLedgerUpdates()
		dynamo_configuration.Init()
		manifest.GetManifestLoader()
	})
	dal.CreatePublisherAccount(Test_PublisherProfile_Medium)
	dal.CreateLedger(Test_Ledger_Blog)
}

func cleanupTestData() {
	dal.DeletePublisherAccount(Test_PublisherProfile_Medium.AccountID,
		Test_PublisherProfile_Medium.PublisherProfileID)
	dal.DeleteLedger(Test_Ledger_Blog.LedgerID)
	Purge()
}

func TestAssignment(t *testing.T) {
	setupTest()
	time.Sleep(time.Duration(1) * time.Second)
	ledgerItem, err := dal.GetLedger(Test_Ledger_Blog.LedgerID)
	if err != nil {
		log.Fatalf("test assignment, error retrieving ledger: %s", err)
	}
	assert.Equal(t, ledgerItem.LedgerStatus, tables.NEW_LEDGER, "should be status new.")
	b, _ := json.MarshalIndent(ledgerItem, "", "  ")
	log.Print("\n" + string(b) + "\n")

	time.Sleep(time.Duration(1) * time.Second)
	ledgerItem, err = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	assert.NotEmpty(t, ledgerItem.MediaEvents, "media events should not be empty.")

	cleanupTestData()
	if err != nil {
		log.Fatalf("test assignment, error occurred: %s", err)
	}
}
