package scaling

import (
	"log"
	"time"

	dal "github.com/bezalel-media-core/v2/dal"
	"github.com/google/uuid"
)

func StartWatching() {
	err := dal.InitRenderFarmEntry()
	if err != nil {
		log.Panic(err)
	}
	processId := uuid.New().String()
	// Check every 10 min if can take process lock
	// if no, wait 10min
	for {
		hasOwnership, err := dal.TakeSystemLockOwnership(dal.SYSTEM_RENDER_FARM, processId)
		if err != nil {
			log.Printf("error verifying lock ownership: %s", err)
		}

		if !hasOwnership {
			time.Sleep(time.Duration(10) * time.Minute)
		} else {
			break
		}
	}

	// if yes, update status, scaling, and increment expiry every 5 min
	// TODO
}
