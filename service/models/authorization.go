package models

type AuthorizationCodeState struct {
	AccountId          string `json:"accountId"`
	PublisherProfileId string `json:"publisherProfileId"`
}

// TODO: see HandlerOauthCodeCallback
// Should be passed as body from SPA
type AuthorizationCodeCallback struct {
	Code         string `json:"code"`
	EncodedState string `json:"encodedState"`
}
