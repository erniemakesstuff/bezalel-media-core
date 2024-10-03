package orchestration

import (
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

type EmbeddingWorkflow struct{}

func (s *EmbeddingWorkflow) GetWorkflowName() string {
	return "EmbeddingWorkflow"
}

func (s *EmbeddingWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO:
	// Check MediaEvents all ready.
	// Embed them: call embedding api
	// UPSERT into pgvector
	return nil
}
