package authorization

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"os"

	dal "github.com/bezalel-media-core/v2/dal"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/youtube/v3"
)

// This variable indicates whether the script should launch a web server to
// initiate the authorization flow or just display the URL in the terminal
// window. Note the following instructions based on this setting:
// * launchWebServer = true
//  1. Use OAuth2 credentials for a web application
//  2. Define authorized redirect URIs for the credential in the Google APIs
//     Console and set the RedirectURL property on the config object to one
//     of those redirect URIs. For example:
//     config.RedirectURL = "http://localhost:8090"
//  3. In the startWebServer function below, update the URL in this line
//     to match the redirect URI you selected:
//     listener, err := net.Listen("tcp", "localhost:8090")
//     The redirect URI identifies the URI to which the user is sent after
//     completing the authorization flow. The listener then captures the
//     authorization code in the URL and passes it back to this script.
//
// * launchWebServer = false
//  1. Use OAuth2 credentials for an installed application. (When choosing
//     the application type for the OAuth2 client ID, select "Other".)
//  2. Set the redirect URI to "urn:ietf:wg:oauth:2.0:oob", like this:
//     config.RedirectURL = "urn:ietf:wg:oauth:2.0:oob"
//  3. When running the script, complete the auth flow. Then copy the
//     authorization code from the browser and enter it on the command line.
const launchWebServer = false

const missingClientSecretsMessage = `
Please configure OAuth 2.0
To make this sample run, you need to populate the client_secrets.json file
found at:
   %v
with information from the {{ Google Cloud Console }}
{{ https://cloud.google.com/console }}
For more information about the client_secrets.json file format, please visit:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
`

func GetClient(bearerToken string, refreshToken string, expiresInSec int64) (*http.Client, error) {
	ctx := context.Background()
	config, err := getGoogleConfig()
	if err != nil {
		log.Printf("failed to load google config: %s", err)
		return nil, err
	}
	token := oauth2.Token{
		AccessToken:  bearerToken,
		RefreshToken: refreshToken,
		ExpiresIn:    expiresInSec,
	}
	return config.Client(ctx, &token), err
}

func getGoogleConfig() (*oauth2.Config, error) {
	credsBytes, err := os.ReadFile("creds_google_oauth.json") // TODO: Move this to env config
	if err != nil {
		log.Fatalf("Unable to load credentials file %v", err)
	}
	config, err := google.ConfigFromJSON(credsBytes, youtube.YoutubeScope, youtube.YoutubeUploadScope, youtube.YoutubepartnerScope)
	if err != nil {
		log.Fatalf("Unable to load config from json file %v", err)
	}
	domain := "http://localhost:8080" // TODO: Move this to env config
	config.RedirectURL = domain + "/v1/authcode/youtube/callback"
	return config, err
}

// Exchange the authorization code for an access token
func exchangeToken(code string) (*oauth2.Token, error) {
	config, err := getGoogleConfig()
	if err != nil {
		log.Fatalf("Unable to load config from json %v", err)
	}
	tok, err := config.Exchange(context.Background(), code)
	if err != nil {
		log.Fatalf("Unable to retrieve token %v", err)
	}
	return tok, nil
}

// StartOauthCodeFlow uses Config to request a Token.
// It returns the retrieved Token.
func StartOauthCodeFlow(accountId string, publisherProfileId string) (string, error) {
	config, err := getGoogleConfig()
	if err != nil {
		log.Fatalf("Unable to create google confige: %v", err)
	}
	statePayload := fmt.Sprintf("{\"accountId\": \"%s\", \"publisherProfileId\": \"%s\"}", accountId, publisherProfileId)
	encodedState := base64.StdEncoding.EncodeToString([]byte(statePayload))
	authUrl := config.AuthCodeURL(encodedState, oauth2.AccessTypeOffline)
	if err != nil {
		log.Fatalf("Unable to generate authorization URL in web server: %v", err)
	}
	return authUrl, err
}

func StoreAuthorizationCode(authCode string, accountId string, publisherProfileId string) error {
	token, err := exchangeToken(authCode)
	if err != nil {
		log.Printf("error exchanging token to store authorization code: %s", err)
		return err
	}
	err = dal.StoreOauthCredentials(accountId, publisherProfileId, token.AccessToken, token.RefreshToken, token.ExpiresIn)
	return err
}
