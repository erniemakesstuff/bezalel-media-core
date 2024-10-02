package orchestration

import (
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

type PublishWorkFlow struct{}

func (s *PublishWorkFlow) GetWorkflowName() string {
	return "PublishWorkFlow"
}

func (s *PublishWorkFlow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO:
	// Collect mediaEvents where IsFinalMedia for an assigned root-media event.
	// Set status to PUBLISHING
	// Start upload to distribution channel.
	// WAIT for 30 minutes; periodically polling to verify upload.
	//	Set COMPLETE for success.
	// 	Set EXPIRED for failure.
	return nil
}
