package orchestration

import (
	"log"

	"github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/google/uuid"
)

type Workflow interface {
	GetWorkflowName() string
	Run(tables.Ledger, string) error
}

// TODO: Add workflows in-order here.
// Workflows run in order.
var workflowsToRun = []Workflow{
	&ScriptWorkflow{},
	&EnrichmentWorkflow{},
	&EmbeddingWorkflow{},
	&AssignmentWorkflow{},
	&FinalRenderWorkflow{},
	&PublishWorkFlow{},
	&CompletionWorkflow{},
}

func RunWorkflows(ledgerItem tables.Ledger) error {
	latestLedger, err := dal.GetLedger(ledgerItem.LedgerID)
	if err != nil {
		log.Printf("correlationID: %s run workflows error: %s", ledgerItem.LedgerID, err)
		return err
	}

	if isCompleteWorkflow(latestLedger) {
		return nil
	}

	if isStaleLedgerEvent(ledgerItem, latestLedger) {
		log.Printf("correlationID: %s ignoring stale ledger event", ledgerItem.LedgerID)
		return nil
	}
	processId := uuid.New().String()
	for _, w := range workflowsToRun {
		err := w.Run(latestLedger, processId)
		if err != nil {
			log.Printf("correlationID: %s workflow %s failed: %s", latestLedger.LedgerID, w.GetWorkflowName(), err)
		}
	}
	return nil
}

func isCompleteWorkflow(ledgerItem tables.Ledger) bool {
	return ledgerItem.LedgerStatus == tables.FINISHED_LEDGER
}

func isStaleLedgerEvent(triggerLedger tables.Ledger, latestLedger tables.Ledger) bool {
	// Event triggers should result in at-least one MediaEvent.
	// If it is zero, implies new - since a new event will trigger at least one MediaEvent.
	// S3 callbacks --> ledger event model will also be 0.
	if triggerLedger.MediaEventsVersion == 0 {
		return false
	}

	return (triggerLedger.MediaEventsVersion < latestLedger.MediaEventsVersion) ||
		(triggerLedger.PublishEventsVersion < latestLedger.PublishEventsVersion)
}
