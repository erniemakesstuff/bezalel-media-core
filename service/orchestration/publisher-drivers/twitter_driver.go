package publisherdrivers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	manifest "github.com/bezalel-media-core/v2/manifest"
	"github.com/michimani/gotwi"
	"github.com/michimani/gotwi/tweet/managetweet"
	"github.com/michimani/gotwi/tweet/managetweet/types"
)

type TwitterDriver struct{}

type TwitterPostContents struct {
	TweetTextBody string
}

func (s TwitterDriver) Publish(pubCommand PublishCommand) error {
	acc, err := dal.GetPublisherAccount(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error loading publisher account for medium driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	blogPayload, err := s.loadMediaContents(pubCommand.FinalRenderMediaRoot)
	if err != nil {
		log.Printf("correlationID: %s error downloading content for tinyblog: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	err = s.publishTwitterPost(pubCommand.RootPublishEvent.LedgerID, acc, blogPayload)
	if err != nil {
		log.Printf("correlationID: %s error uploading blog contents to Medium: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return err
	}
	return err
}

func (s TwitterDriver) loadMediaContents(mediaEvent tables.MediaEvent) (TwitterPostContents, error) {
	// TODO: allow enrichment with images.
	result := TwitterPostContents{}
	var err error
	scriptPayload, err := s.loadScriptPayload(mediaEvent)
	if err != nil {
		log.Printf("correlationID: %s error initializing twitter blog contents: %s", mediaEvent.LedgerID, err)
		return result, err
	}

	result.TweetTextBody = scriptPayload.BlogText
	return result, err
}

func (s TwitterDriver) loadScriptPayload(rootFinalRender tables.MediaEvent) (manifest.TinyBlogJsonSchema, error) {
	payload, err := LoadAsString(rootFinalRender.ContentLookupKey)
	if err != nil {
		log.Printf("correlationID: %s error loading script content as string: %s", rootFinalRender.LedgerID, err)
		return manifest.TinyBlogJsonSchema{}, err
	}
	return s.scriptPayloadToBlogJson(payload)
}

func (s TwitterDriver) scriptPayloadToBlogJson(payload string) (manifest.TinyBlogJsonSchema, error) {
	result := manifest.TinyBlogJsonSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error unmarshalling script text to blog schema object: %s", err)
		log.Printf("error payload: <%s>", payload)
		return result, err
	}

	if len(result.BlogText) == 0 {
		return manifest.TinyBlogJsonSchema{}, fmt.Errorf("empty payload received: %s", payload)
	}

	return result, err
}

func (s TwitterDriver) publishTwitterPost(ledgerId string, account tables.AccountPublisher, tweetPayload TwitterPostContents) error {
	// TODO: Move PublisherAPISecretID to be a global-service config.
	// Retain the AccountPublisher fields; necessary.
	// https://trello.com/c/ol3Lvvop
	in := &gotwi.NewClientInput{
		AuthenticationMethod: gotwi.AuthenMethodOAuth1UserContext,
		OAuthToken:           account.UserAccessToken,
		OAuthTokenSecret:     account.UserAccessTokenSecret,
		APIKey:               account.PublisherAPISecretID,
		APIKeySecret:         account.PublisherAPISecretKey,
	}

	c, err := gotwi.NewClient(in)
	if err != nil {
		log.Printf("correlationID: %s error creating twitter client: %s", ledgerId, err)
	}
	p := &types.CreateInput{
		Text: gotwi.String(tweetPayload.TweetTextBody),
	}

	res, err := managetweet.Create(context.Background(), c, p)
	if err != nil {
		return s.setAnyBadRequestCode(err)
	}

	log.Printf("correlationID: %s tweeted: %s", ledgerId, gotwi.StringValue(res.Data.ID))
	return err
}

func (s TwitterDriver) setAnyBadRequestCode(err error) error {
	is400StatusCode := strings.Contains(fmt.Sprintf("%s", err), "httpStatusCode=4")
	if is400StatusCode {
		return fmt.Errorf("%s: Twitter profile resulted in bad request: %s", BAD_REQUEST_PROFILE_CODE, err)
	}
	return err
}
