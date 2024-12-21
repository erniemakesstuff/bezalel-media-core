package ingestion

import (
	"errors"
	"io"

	"github.com/bezalel-media-core/v2/service/ingestion/drivers"
)

func GetDriver(source string, payloadIO io.ReadCloser) (drivers.Driver, error) {
	switch {
	case source == "v1/source/prompt":
		val := drivers.NewCustomPromptDriver(payloadIO, source)
		return val, nil
	case source == "v1/source/blog" || source == "WorkflowIntegTest":
		val := drivers.NewBlogPromptDriver(payloadIO, source)
		return val, nil
	case source == "v1/source/forum":
		val := drivers.NewForumDriver(payloadIO, source)
		return val, nil
	case source == "v1/reactions/short/images" || source == "v1/reactions/short/videos":
		return drivers.NewReactDriver(payloadIO, source), nil
	}

	return nil, errors.New("no matching source-to-driver found")
}
