package v1

type Prompt struct {
	// Required
	PromptID string

	// Optional
	InstructionText string // Instructions for downstream LLMs.
}
