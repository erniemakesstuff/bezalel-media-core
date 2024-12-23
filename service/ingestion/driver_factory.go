package ingestion

import (
	"errors"
	"io"

	"github.com/bezalel-media-core/v2/service/ingestion/drivers"
)

func GetDriver(source string, payloadIO io.ReadCloser) (drivers.Driver, error) {
	var err error
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
	case source == "v1/reactions/short/image" || source == "v1/reactions/short/video" ||
		source == "v1/reactions/long/image" || source == "v1/reactions/long/video":
		driverReact := drivers.NewReactDriver(source)
		err = driverReact.WithMedia(payloadIO)
		return driverReact, err
	}

	return nil, errors.New("no matching source-to-driver found")
}
