package models

type CustomPromptRequest struct {
	Source     string `json:"source"`
	PromptText string `json:"promptText"`
}

type BlogRequest struct {
	Source         string `json:"source"`
	TargetLanguage string `json:"targetLanguage"`
	Text           string `json:"text"`
}

type ForumDumpRequest struct {
	Source         string `json:"source"`
	TargetLanguage string `json:"targetLanguage"`
	ForumMainPost  string `json:"forumMainPost"`
	Comments       string `json:"comments"`
}

type ReactionRequest struct {
	Source         string `json:"source"`
	TargetLanguage string `json:"targetLanguage"`
	ContentUrl     string `json:"contentUrl"`
}
