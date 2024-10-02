package orchestration

import (
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

type FinalRenderWorkflow struct{}

func (s *FinalRenderWorkflow) GetWorkflowName() string {
	return "FinalRenderWorkflow"
}

func (s *FinalRenderWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO:
	// Select PublishEvent ASSIGNED w/o RENDERING state recorded.
	// Verify child media is ready.
	// If Root mediaEvent distribution channel is TEXT-ONLY, set state to RENDERING.
	//	Set IsFinalRender to true
	// 	Skip other final-rendering steps.
	// Spawn child MediaEvent (singular!) for final-rendering
	// 	Final rendering should specify all visual and audio resources in-order.
	// 	Final rendering MediaEvent should include watermark info
	// 	Set IsFinalRender to true for media events.
	// WAIT for 30 minutes; periodically polling contentLookupKey to see if finished.
	//	Set status EXPIRED on timeout.
	return nil
}
