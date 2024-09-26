package models

type Custom_Prompt_Request struct {
	Source     string `json:"source"`
	PromptText string `json:"promptText"`
}

type Blog_Request struct {
	Source string `json:"source"`
	Text   string `json:"text"`
}
