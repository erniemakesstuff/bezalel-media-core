package orchestration

import (
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
)

// Creates final-render that compiles child media events.
type FinalRenderWorkflow struct{}

func (s *FinalRenderWorkflow) GetWorkflowName() string {
	return "FinalRenderWorkflow"
}

func (s *FinalRenderWorkflow) Run(ledgerItem tables.Ledger, processId string) error {
	// TODO:
	// Select PublishEvent ASSIGNED w/o RENDERING state recorded.
	// Verify child media is ready, or Root mediaEvent distribution channel is has no children
	// Spawn child MediaEvent (singular!) for final-rendering
	// 	Final rendering should specify all visual and audio resources in-order.
	// 	Final rendering MediaEvent should include watermark info
	// 	Set IsFinalRender to true for media events.
	// Append status RENDERING
	// Don't need to wait; rely on reaper-workflow to invalidate the PublishEvent.
	return nil
}
