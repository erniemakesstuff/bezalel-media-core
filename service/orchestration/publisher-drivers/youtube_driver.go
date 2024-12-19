package publisherdrivers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	env "github.com/bezalel-media-core/v2/configuration"
	dal "github.com/bezalel-media-core/v2/dal"
	tables "github.com/bezalel-media-core/v2/dal/tables/v1"
	"github.com/bezalel-media-core/v2/manifest"
	auth "github.com/bezalel-media-core/v2/service/authorization"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

type YouTubeDriver struct{}

type YouTubeContents struct {
	VideoTitle                     string
	VideoDescription               string
	Tags                           []string
	VideoContentLookupKey          string
	VideoThumbnailContentLookupKey string
}

/*
*
* YouTube Profiles need to be phone verified in order to enable Thumbnail uploads!
 */
func (s YouTubeDriver) Publish(pubCommand PublishCommand) (string, error) {
	acc, err := dal.GetPublisherAccount(pubCommand.RootPublishEvent.AccountID, pubCommand.RootPublishEvent.PublisherProfileID)
	if err != nil {
		log.Printf("correlationID: %s error loading publisher account for YouTube driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return "", err
	}

	googleAuthClient := auth.GoogleAuth{}
	client, err := googleAuthClient.GetClient(acc)
	if err != nil {
		log.Printf("correlationID: %s error creating http client for YouTube driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return "", err
	}
	svc, err := youtube.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Printf("correlationID: %s error creating youtube service for YouTube driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return "", err
	}
	contents, err := s.loadVideoDetails(pubCommand)
	if err != nil {
		log.Printf("correlationID: %s error fetching contents for YouTube driver: %s", pubCommand.RootPublishEvent.LedgerID, err)
		return "", err
	}
	return s.uploadMedia(pubCommand.RootPublishEvent.LedgerID, svc, contents)
}

func (s YouTubeDriver) refreshAccountCredentials(account tables.AccountPublisher) (tables.AccountPublisher, error) {
	googleAuthClient := auth.GoogleAuth{}
	latestToken, err := googleAuthClient.StoreAuthorizationCode(account.OauthRefreshToken, account.AccountID, account.PublisherProfileID)
	if err != nil {
		return account, err
	}

	account.OauthTokenType = latestToken.TokenType
	account.OauthExpiryEpochSec = latestToken.Expiry.Unix()
	account.OauthRefreshToken = latestToken.RefreshToken
	account.OauthToken = latestToken.AccessToken
	return account, err
}

func (s YouTubeDriver) loadVideoDetails(pubc PublishCommand) (YouTubeContents, error) {
	if pubc.FinalRenderMedia.DistributionFormat == "ShortVideo" {
		return s.getShortFormContents(pubc)
	}
	// TODO: Longform content
	return YouTubeContents{}, errors.New("no matching distribution format within YouTube driver")
}

func (s YouTubeDriver) getShortFormContents(pubc PublishCommand) (YouTubeContents, error) {
	result := YouTubeContents{}
	result.VideoContentLookupKey = pubc.FinalRenderMedia.ContentLookupKey // Final render video file
	scriptPayload, err := LoadAsBytes(pubc.ScriptMedia.ContentLookupKey)
	if err != nil {
		log.Printf("correlationID: %s error downloading script bytes: %s", pubc.ScriptMedia.LedgerID, err)
		return result, err
	}
	scriptContents := manifest.ShortVideoSchema{}
	err = json.Unmarshal(scriptPayload, &scriptContents)
	if err != nil {
		log.Printf("correlationID: %s error deserializing shortform script contents: %s", pubc.ScriptMedia.LedgerID, err)
		return result, err
	}
	videoThumbnailKey, err := s.getThumbnailLookupKey(pubc.FinalRenderMedia.LedgerID, pubc.FinalRenderMedia.FinalRenderSequences)
	if err != nil {
		log.Printf("correlationID: %s error retrieving video thumbnail lookup key: %s", pubc.ScriptMedia.LedgerID, err)
		return result, err
	}
	result.Tags = scriptContents.VideoTags
	result.VideoDescription = scriptContents.VideoDescription
	result.VideoTitle = scriptContents.VideoTitle
	result.VideoThumbnailContentLookupKey = videoThumbnailKey
	return result, nil
}

func (s YouTubeDriver) uploadMedia(ledgerId string, svc *youtube.Service, contents YouTubeContents) (string, error) {
	err := TryDownloadWithRetry(contents.VideoContentLookupKey, 0)
	if err != nil {
		log.Printf("correlationID: %s error deserializing shortform script contents: %s", ledgerId, err)
		return "", err
	}
	videoFilename := s.getDescriptiveFilename(contents.VideoTitle)
	err = os.Rename(contents.VideoContentLookupKey, videoFilename)
	if err != nil {
		log.Printf("correlationID: %s error renaming file: %s", ledgerId, err)
		return "", err
	}

	upload := &youtube.Video{
		Snippet: &youtube.VideoSnippet{
			Title:       contents.VideoTitle,
			Description: contents.VideoDescription,
			Tags:        contents.Tags,
		},
		Status: &youtube.VideoStatus{PrivacyStatus: "public", MadeForKids: false},
	}

	call := svc.Videos.Insert([]string{"snippet", "status"}, upload)
	file, err := os.Open(videoFilename)
	if err != nil {
		log.Printf("correlationID: %s error opening video file: %s", ledgerId, err)
		return "", err
	}

	if !dal.IsCallable(dal.RATE_API_YOUTUBE_UPLOAD, env.GetEnvConfigs().MaxRequestYouTubeMinute) {
		return "", fmt.Errorf("rate limit breached: %s", dal.RATE_API_YOUTUBE_UPLOAD)
	}

	uploadVideoResp, err := call.Media(file).Do()
	if err != nil {
		log.Printf("correlationID: %s error uploading YouTube video: %s", ledgerId, err)
		return "", s.setAnyBadRequestCode(err)
	}
	file.Close()
	os.Remove(videoFilename)
	/*
		//Decision to bypass custom thumbnails by default since it won't be used for the majority of our accounts:
		//	https://trello.com/c/4mAAlR7B#comment-6753642fccb3f1faac6b8c53
		videoId := response.Id

		err = TryDownloadWithRetry(contents.VideoThumbnailContentLookupKey, 0)
		if err != nil {
			log.Printf("correlationID: %s error downloading thumbnail image: %s", ledgerId, err)
			return err
		}
		thumbnailFile, err := os.Open(contents.VideoThumbnailContentLookupKey)
		if err != nil {
			log.Printf("correlationID: %s error opening thumbnail file: %s", ledgerId, err)
			return err
		}


		thumbnailCall := svc.Thumbnails.Set(videoId)
		_, err = thumbnailCall.Media(thumbnailFile).Do()
		if err != nil {
			log.Printf("correlationID: %s WARN error uploading YouTube thumbnail: %s", ledgerId, err)
			err = nil // ignore; non-critical path.
		}
		defer thumbnailFile.Close()
		defer os.Remove(contents.VideoThumbnailContentLookupKey)
	*/

	return uploadVideoResp.Id, s.setAnyBadRequestCode(err)
}

func (s YouTubeDriver) getDescriptiveFilename(videoTitle string) string {
	// YouTube uses the filename as part of its SEO.
	w0 := strings.ReplaceAll(videoTitle, "'", "")
	w1 := strings.ReplaceAll(w0, "\"", "")
	w2 := strings.ReplaceAll(w1, " ", "_")
	w3 := strings.ReplaceAll(w2, ",", "_")
	w4 := strings.ReplaceAll(w3, ".", "_")
	w5 := strings.ReplaceAll(w4, "!", "_")
	w6 := strings.ReplaceAll(w5, "?", "_")
	w7 := strings.ReplaceAll(w6, "|", "_")
	w8 := strings.ReplaceAll(w7, "\\", "_")
	w9 := strings.ReplaceAll(w8, "/", "_")
	w10 := strings.ReplaceAll(w9, ":", "_")
	w11 := strings.ReplaceAll(w10, "<", "_")
	w12 := strings.ReplaceAll(w11, ">", "_")
	w13 := strings.ReplaceAll(w12, "*", "_")
	final := strings.TrimSpace(w13)
	return final + ".mp4"
}

func (s YouTubeDriver) getThumbnailLookupKey(ledgerId string, finalRenderSequences string) (string, error) {
	renderSequences := []tables.RenderMediaSequence{}
	err := json.Unmarshal([]byte(finalRenderSequences), &renderSequences)
	if err != nil {
		log.Printf("correlationID: %s error deserializing renderSequences for twitter: %s", ledgerId, err)
		return "", err
	}
	for _, r := range renderSequences {
		if r.MediaType == tables.MEDIA_IMAGE && r.PositionLayer == tables.IMAGE_THUMBNAIL {
			return r.ContentLookupKey, nil
		}
	}
	return "", errors.New("image thumbnail not found in YouTube driver")
}

func (s YouTubeDriver) setAnyBadRequestCode(err error) error {
	isCredentialError := strings.Contains(fmt.Sprintf("%s", err), "httpStatusCode=403") ||
		strings.Contains(fmt.Sprintf("%s", err), "httpStatusCode=401") ||
		strings.Contains(fmt.Sprintf("%s", err), "Error 403") ||
		strings.Contains(fmt.Sprintf("%s", err), "Error 401") ||
		strings.Contains(fmt.Sprintf("%s", err), "Error 401")
	if isCredentialError {
		return fmt.Errorf("%s: YouTube profile resulted in bad request: %s", BAD_REQUEST_PROFILE_CODE, err)
	}
	return err
}
