package models

type Custom_Prompt_Request struct {
	Source     string `json:"source"`
	PromptText string `json:"promptText"`
}

type Blog_Request struct {
	Source         string `json:"source"`
	TargetLanguage string `json:"targetLanguage"`
	Text           string `json:"text"`
}

type Forum_Dump_Request struct {
	Source         string `json:"source"`
	TargetLanguage string `json:"targetLanguage"`
	ForumMainPost  string `json:"forumMainPost"`
	Comments       string `json:"comments"`
}
