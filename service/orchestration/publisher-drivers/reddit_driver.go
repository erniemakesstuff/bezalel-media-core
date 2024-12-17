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
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

type RedditDriver struct{}

type RedditDriverContents struct {
	TextBody  string
	Title     string
	Subreddit string
}

func (s RedditDriver) Publish(pubCommand PublishCommand) (string, error) {
	acc, err := dal.GetPublisherAccount(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error loading publisher account for Reddit driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return "", err
	}
	blogPayloads, err := s.loadMediaContents(pubCommand.FinalRenderMedia, acc)
	if err != nil {
		log.Printf("correlationID: %s error downloading content for blog: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return "", err
	}
	id, err := s.publishRedditPost(pubCommand.RootPublishEvent.LedgerID, acc, blogPayloads)
	if err != nil {
		log.Printf("correlationID: %s error uploading blog contents to Reddit: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return "", err
	}
	return id, err
}

func (s RedditDriver) loadMediaContents(mediaEvent tables.MediaEvent, pubAccount tables.AccountPublisher) ([]RedditDriverContents, error) {
	// TODO: allow enrichment with images.
	result := []RedditDriverContents{}
	var err error
	scriptPayload, err := s.loadScriptPayload(mediaEvent)
	if err != nil {
		log.Printf("correlationID: %s error initializing Reddit blog contents: %s", mediaEvent.LedgerID, err)
		return result, err
	}

	subredditTargets := strings.Split(pubAccount.RedditSubredditTargetsCSV, ",")
	if len(subredditTargets) == 0 {
		log.Printf("correlationID: %s publisher profile missing subreddit targets %s %s", mediaEvent.LedgerID,
			pubAccount.AccountID, pubAccount.PublisherProfileID)
		return result, fmt.Errorf("missing subreddit targets for account %s", BAD_REQUEST_PROFILE_CODE)
	}
	for _, subs := range subredditTargets {
		result = append(result, RedditDriverContents{
			Title:     scriptPayload.BlogTitle,
			TextBody:  scriptPayload.BlogText,
			Subreddit: subs,
		})
	}
	return result, err
}

func (s RedditDriver) loadScriptPayload(rootFinalRender tables.MediaEvent) (manifest.BlogSchema, error) {
	payload, err := LoadAsString(rootFinalRender.ContentLookupKey)
	if err != nil {
		log.Printf("correlationID: %s error loading script content as string: %s", rootFinalRender.LedgerID, err)
		return manifest.BlogSchema{}, err
	}
	return s.scriptPayloadToBlogJson(payload)
}

func (s RedditDriver) scriptPayloadToBlogJson(payload string) (manifest.BlogSchema, error) {
	result := manifest.BlogSchema{}
	err := json.Unmarshal([]byte(payload), &result)
	if err != nil {
		log.Printf("error unmarshalling script text to blog schema object: %s", err)
		log.Printf("error payload: <%s>", payload)
		return result, err
	}

	if len(result.BlogText) == 0 {
		return manifest.BlogSchema{}, fmt.Errorf("reddit empty payload received: %s", payload)
	}

	return result, err
}

func (s RedditDriver) publishRedditPost(ledgerId string, account tables.AccountPublisher, redditPayloads []RedditDriverContents) (string, error) {
	// TODO: Move PublisherAPISecretID to be a global-service config.
	// Retain the AccountPublisher fields; necessary.
	// https://trello.com/c/ol3Lvvop
	credentials := reddit.Credentials{
		ID:       account.PublisherAPISecretID,
		Secret:   account.PublisherAPISecretKey,
		Username: account.UserAccessToken,
		Password: account.UserAccessTokenSecret}
	client, err := reddit.NewClient(credentials)
	if err != nil {
		log.Printf("correlationID: %s error creating Reddit client: %s", ledgerId, err)
	}
	postIds := []string{}
	for _, r := range redditPayloads {
		post, _, err := client.Post.SubmitText(context.Background(), reddit.SubmitTextRequest{
			Subreddit: r.Subreddit,
			Title:     r.Title,
			Text:      r.TextBody,
		})
		if err != nil {
			return "", s.setAnyBadRequestCode(err)
		}
		fmt.Printf("correlationID: %s Reddit text post is available at: %s", ledgerId, post.URL)
		postIds = append(postIds, post.FullID)
	}

	return strings.Join(postIds, ","), err
}

func (s RedditDriver) setAnyBadRequestCode(err error) error {
	isCredentialError := strings.Contains(fmt.Sprintf("%s", err), "httpStatusCode=403") ||
		strings.Contains(fmt.Sprintf("%s", err), "httpStatusCode=401")
	if isCredentialError {
		return fmt.Errorf("%s: Reddit profile resulted in bad request: %s", BAD_REQUEST_PROFILE_CODE, err)
	}
	return err
}
