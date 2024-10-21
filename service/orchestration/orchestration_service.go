package orchestration

import (
	"fmt"
	"log"

	"github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/google/uuid"
)

type Workflow interface {
	GetWorkflowName() string
	Run(tables.Ledger, string) error
}

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

func RunWorkflows(triggerLedger tables.Ledger) error {
	latestLedger, err := dal.GetLedger(triggerLedger.LedgerID)
	if err != nil {
		log.Printf("correlationID: %s run workflows error retrieving latest ledger: %s", triggerLedger.LedgerID, err)
		return err
	}

	if isCompleteWorkflow(latestLedger) {
		return nil
	}

	processId := fmt.Sprintf("%s.LedgerID:%s", uuid.New().String(), latestLedger.LedgerID)
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
