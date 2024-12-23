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

// Try not to delete test accounts :)
var PubProfile_EN_Medium_1 = tables.AccountPublisher{
	AccountID:                 "TestUser1",
	PublisherProfileID:        "MediumProfileId1",
	ChannelName:               tables.Channel_Medium,
	LastPublishAtEpochMilli:   0,
	AccountSubscriptionStatus: tables.EVERGREEN_ADMIN,
	PublisherNiche:            "TestingNiche",
	PublisherLanguage:         "EN",
}

var Test_Ledger_Blog = tables.Ledger{
	LedgerID:                   "TestLedgerId",
	LedgerStatus:               tables.NEW_LEDGER,
	TriggerEventSource:         "WorkflowIntegTest",
	TriggerEventPayload:        "The weather in Seattle is fair.",
	TriggerEventTargetLanguage: "EN",
}

func setupTest() {
	once.Do(func() {
		os.Chdir("../..") // For manifest file loads.
		PollForLedgerUpdates()
		dynamo_configuration.Init()
		manifest.GetManifestLoader()
	})
	err := dal.ForceAllLocksFree(PubProfile_EN_Medium_1.AccountID,
		PubProfile_EN_Medium_1.PublisherProfileID)
	if err != nil {
		log.Fatalf("failed to release locks on cleanup: %s", err)
	}
	Test_Ledger_Blog.LedgerID = uuid.New().String() + "-INTEG-TEST"
	dal.CreateLedger(Test_Ledger_Blog)
}

func cleanupTestData() {
	err := dal.ForceAllLocksFree(PubProfile_EN_Medium_1.AccountID,
		PubProfile_EN_Medium_1.PublisherProfileID)
	if err != nil {
		log.Fatalf("failed to release locks on cleanup: %s", err)
	}
	//dal.DeleteLedger(Test_Ledger_Blog.LedgerID)
	time.Sleep(time.Duration(40) * time.Second)
	Purge()
}

func TestWorkflows(t *testing.T) {
	setupTest()
	ledgerItem, _ := dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ := dal.GetPublisherAccount(PubProfile_EN_Medium_1.AccountID, PubProfile_EN_Medium_1.PublisherProfileID)
	// 1. Create new trigger event; verify created.
	assert.Equal(t, ledgerItem.LedgerStatus, tables.NEW_LEDGER, "should be status new")
	assert.Empty(t, publisherAcc.AssignmentLockID, "no assignment lock should be present")
	// If issue with flakey tests; examine sstepWaitSecond
	const stepWaitSec = 50
	// 2. Wait for mediaEvent to be created
	time.Sleep(time.Duration(stepWaitSec) * time.Second)
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	assert.NotEmpty(t, ledgerItem.MediaEvents, "media events should not be empty")
	b, _ := json.MarshalIndent(ledgerItem, "", "  ")
	log.Print("\n MediaEventsDebug: " + string(b) + "\n")

	// 3. Assert publisher profile assignment
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ = dal.GetPublisherAccount(PubProfile_EN_Medium_1.AccountID, PubProfile_EN_Medium_1.PublisherProfileID)
	assert.NotEmpty(t, ledgerItem.PublishEvents, "publish events should not be empty")

	// 4. Verify final render media event created
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	assert.True(t, hasFinalMediaRender(ledgerItem), "expected FinalRender media event")
	assert.True(t, hasRenderPublishEvent(ledgerItem), "expected RENDERING publish event")

	// 4. Verify PUBLISHING media event created
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ = dal.GetPublisherAccount(PubProfile_EN_Medium_1.AccountID, PubProfile_EN_Medium_1.PublisherProfileID)
	assert.True(t, hasPublishingPublishEvent(ledgerItem), "expected PUBLISHING publish event")

	// 5. Confirm ledger is marked complete
	time.Sleep(time.Duration(stepWaitSec) * time.Second)
	ledgerItem, _ = dal.GetLedger(Test_Ledger_Blog.LedgerID)
	publisherAcc, _ = dal.GetPublisherAccount(PubProfile_EN_Medium_1.AccountID, PubProfile_EN_Medium_1.PublisherProfileID)
	assert.True(t, hasCompletionEvent(ledgerItem), "expected COMPLETE publish event")
	assert.Empty(t, publisherAcc.PublishLockID, "expected PublisherLockID to be released")
	assert.Empty(t, publisherAcc.PublishLockTTL, "expected PublishLockTTL to be released")
	assert.Empty(t, publisherAcc.AssignmentLockID, "expected AssignmentLockID to be released")
	assert.Empty(t, publisherAcc.AssignmentLockTTL, "expected AssignmentLockTTL to be released")
	assert.Equal(t, tables.FINISHED_LEDGER, ledgerItem.LedgerStatus, "expected FINISHED_LEDGER status")
	b, _ = json.MarshalIndent(ledgerItem, "", "  ")
	log.Print("\n LedgerDebugDeprint: " + string(b) + "\n")
	pubEvents, _ := ledgerItem.GetExistingPublishEvents()
	for _, p := range pubEvents {
		b, _ = json.MarshalIndent(p, "", "  ")
		log.Print("\n LedgerDebugDeprint-PUBEVENT: " + string(b) + "\n")
	}
	cleanupTestData()
}

func hasFinalMediaRender(ledgerItem tables.Ledger) bool {
	mediaEvents, _ := ledgerItem.GetExistingMediaEvents()
	for _, m := range mediaEvents {
		if m.MetaMediaDescriptor == tables.FINAL_RENDER {
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

func hasCompletionEvent(ledgerItem tables.Ledger) bool {
	publishEvents, _ := ledgerItem.GetExistingPublishEvents()
	for _, p := range publishEvents {
		if p.PublishStatus == tables.COMPLETE {
			return true
		}
	}
	return false
}
