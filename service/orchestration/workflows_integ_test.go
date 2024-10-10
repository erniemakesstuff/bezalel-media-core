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

var Test_PublisherProfile_EN_Medium = tables.AccountPublisher{
	AccountID:                 "TestUser",
	PublisherProfileID:        "MediumProfileId",
	ChannelName:               tables.Channel_Medium,
	LastPublishAtEpochMilli:   0,
	AccountSubscriptionStatus: tables.EVERGREEN_ADMIN,
	PublisherNiche:            "Blog",
	PublisherLanguage:         "EN",
}

var Test_Ledger_Blog = tables.Ledger{
	LedgerID:             "TestLedgerId",
	LedgerStatus:         tables.NEW_LEDGER,
	TriggerEventSource:   "v1/source/blog",
	TriggerEventPayload:  "The weather in Seattle is fair.",
	TriggerEventLanguage: "EN",
}

func setupTest() {
	once.Do(func() {
		os.Chdir("../..") // For manifest file loads.
		PollForLedgerUpdates()
		dynamo_configuration.Init()
		manifest.GetManifestLoader()
	})
	dal.CreatePublisherAccount(Test_PublisherProfile_EN_Medium)
	Test_Ledger_Blog.LedgerID = uuid.New().String() + "-INTEG-TEST"
	dal.CreateLedger(Test_Ledger_Blog)
}

func cleanupTestData() {
	dal.DeletePublisherAccount(Test_PublisherProfile_EN_Medium.AccountID,
		Test_PublisherProfile_EN_Medium.PublisherProfileID)
	//dal.DeleteLedger(Test_Ledger_Blog.LedgerID)
	time.Sleep(time.Duration(40) * time.Second)
	Purge()
}

func TestWorkflows(t *testing.T) {
	setupTest()
	ledgerItem, _ := dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ := dal.GetPublisherAccount(Test_PublisherProfile_EN_Medium.AccountID, Test_PublisherProfile_EN_Medium.PublisherProfileID)
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
	time.Sleep(time.Duration(30) * time.Second)
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ = dal.GetPublisherAccount(Test_PublisherProfile_EN_Medium.AccountID, Test_PublisherProfile_EN_Medium.PublisherProfileID)
	assert.NotEmpty(t, ledgerItem.PublishEvents, "publish events should not be empty")
	assert.NotEmpty(t, publisherAcc.AssignmentLockID, "publisher account should have assignment lock")
	assert.NotEmpty(t, publisherAcc.AssignmentLockTTL, "publisher account should lock ttl")

	// 4. Verify final render media event created
	time.Sleep(time.Duration(5) * time.Second)
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	assert.True(t, hasFinalMediaRender(ledgerItem), "expected FinalRender media event")
	assert.True(t, hasRenderPublishEvent(ledgerItem), "expected RENDERING publish event")

	// 4. Verify PUBLISHING media event created
	time.Sleep(time.Duration(5) * time.Second)
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ = dal.GetPublisherAccount(Test_PublisherProfile_EN_Medium.AccountID, Test_PublisherProfile_EN_Medium.PublisherProfileID)
	assert.True(t, hasPublishingPublishEvent(ledgerItem), "expected PUBLISHING publish event")
	assert.NotEmpty(t, publisherAcc.PublishLockID, "expected PublisherLockID to be set")
	assert.NotEmpty(t, publisherAcc.PublishLockTTL, "expected PublishLockTTL to be set")
	cleanupTestData()
}

func hasFinalMediaRender(ledgerItem tables.Ledger) bool {
	mediaEvents, _ := ledgerItem.GetExistingMediaEvents()
	for _, m := range mediaEvents {
		if m.IsFinalRender {
			return true
		}
	}
	return false
}

func hasRenderPublishEvent(ledgerItem tables.Ledger) bool {
	publishEvents, _ := ledgerItem.GetExistingPublishEvents()
	for _, p := range publishEvents {
		if p.PublishStatus == tables.RENDERING {
			return true
		}
	}
	return false
}

func hasPublishingPublishEvent(ledgerItem tables.Ledger) bool {
	publishEvents, _ := ledgerItem.GetExistingPublishEvents()
	for _, p := range publishEvents {
		if p.PublishStatus == tables.PUBLISHING {
			return true
		}
	}
	return false
}
