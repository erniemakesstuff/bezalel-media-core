package orchestration

import (
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

// AKA reaper workflow
type CompletionWorkflow struct{}

func (s *CompletionWorkflow) GetWorkflowName() string {
	return "CompletionWorkflow"
}

func (s *CompletionWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO:
	// Mark LedgerItem COMPLETE if fully syndicated (examine PublishEvents FINISHED per-distributino channel)
	// Set PublishEvents to Expired if no corresponding FINISHED within TTL.
	//	Examine AssignmentLocks, and PublishLocks; publish invalidation events as needed.
	return nil
}
