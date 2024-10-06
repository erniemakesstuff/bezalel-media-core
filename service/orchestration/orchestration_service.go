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
	//&EnrichmentWorkflow{},
	//&EmbeddingWorkflow{},
	&AssignmentWorkflow{},
	//&FinalRenderWorkflow{},
	//&PublishWorkFlow{},
	//&CompletionWorkflow{},
}

func RunWorkflows(ledgerItem tables.Ledger) error {
	latestLedger, err := dal.GetLedger(ledgerItem.LedgerID)
	if err != nil {
		log.Printf("correlationID: %s run workflows error: %s", ledgerItem.LedgerID, err)
		return err
	}
	if latestLedger.LedgerID == "" {
		log.Printf("----\n%+v\n------", latestLedger)
		log.Panic(latestLedger.LedgerID + " was empty!")
	}
	if isCompleteWorkflow(latestLedger) {
		return nil
	}
	processId := uuid.New().String()
	for _, w := range workflowsToRun {
		log.Printf("correlationID: %s running %s", latestLedger.LedgerID, w.GetWorkflowName())
		err := w.Run(latestLedger, processId)
		if err != nil {
			log.Printf("correlationID: %s workflow %s failed: %s", latestLedger.LedgerID, w.GetWorkflowName(), err)
		}
		log.Printf("correlationID: %s finished %s", latestLedger.LedgerID, w.GetWorkflowName())
	}
	return nil
}

func isCompleteWorkflow(ledgerItem tables.Ledger) bool {
	if ledgerItem.LedgerStatus == tables.FINISHED_LEDGER {
		log.Printf("correlationID: %s ledger finished.", ledgerItem.LedgerID)
		return true
	}
	return false
}
