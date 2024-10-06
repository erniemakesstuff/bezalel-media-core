package orchestration

import (
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"encoding/json"

	"github.com/google/uuid"
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
	Test_Ledger_Blog.LedgerID = uuid.New().String() + "-INTEG-TEST"
	dal.CreateLedger(Test_Ledger_Blog)
}

func cleanupTestData() {
	dal.DeletePublisherAccount(Test_PublisherProfile_Medium.AccountID,
		Test_PublisherProfile_Medium.PublisherProfileID)
	dal.DeleteLedger(Test_Ledger_Blog.LedgerID)
	time.Sleep(time.Duration(40) * time.Second)
	Purge()
}

func TestAssignment(t *testing.T) {
	setupTest()
	time.Sleep(time.Duration(5) * time.Second)
	ledgerItem, _ := dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ := dal.GetPublisherAccount(Test_PublisherProfile_Medium.AccountID, Test_PublisherProfile_Medium.PublisherProfileID)
	// 1. Create new trigger event; verify created.
	assert.Equal(t, ledgerItem.LedgerStatus, tables.NEW_LEDGER, "should be status new")
	assert.Empty(t, publisherAcc.AssignmentLockID, "no assignment lock should be present")
	b, _ := json.MarshalIndent(ledgerItem, "", "  ")
	log.Print("\n TestAssignmentDebugPrint: " + string(b) + "\n")
	// 2. Wait for mediaEvent to be created
	time.Sleep(time.Duration(30) * time.Second)
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	assert.NotEmpty(t, ledgerItem.MediaEvents, "media events should not be empty")

	// 3. Assert publisher profile assignment
	time.Sleep(time.Duration(70) * time.Second)
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ = dal.GetPublisherAccount(Test_PublisherProfile_Medium.AccountID, Test_PublisherProfile_Medium.PublisherProfileID)
	assert.NotEmpty(t, ledgerItem.PublishEvents, "publish events should not be empty")
	assert.NotEmpty(t, publisherAcc.AssignmentLockID, "publisher account should have assignment lock")
	assert.NotEmpty(t, publisherAcc.AssignmentLockTTL, "publisher account should lock ttl")

	//cleanupTestData()
}
