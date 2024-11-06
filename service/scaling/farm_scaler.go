package scaling

import (
	"log"
	"time"

	config "github.com/bezalel-media-core/v2/configuration"
	dal "github.com/bezalel-media-core/v2/dal"
	"github.com/google/uuid"
)

func StartWatching() {
	err := dal.InitRenderFarmEntry()
	if err != nil {
		log.Panic(err)
	}

	go processWatch(uuid.New().String())
}

func processWatch(processId string) {
	for { // infinite
		// Check every 10 min if can take process lock
		// if no, wait 10min
		waitForOwnership(processId, dal.SYSTEM_RENDER_FARM)

		// if yes, update status, scaling, and increment expiry every 5 min
		_, err := getPendingMessagesCount(config.GetEnvConfigs().LedgerQueueName)
		if err != nil {
			log.Printf("error fetching pending ledger messages count: %s", err)
		}

		dal.TakeSystemLockOwnership(dal.SYSTEM_RENDER_FARM, processId)
		time.Sleep(time.Duration(5) * time.Minute)
	}
}

func waitForOwnership(processId string, system string) {
	for {
		hasOwnership, err := dal.TakeSystemLockOwnership(system, processId)
		if err != nil {
			log.Printf("error verifying lock ownership for system %s: %s", system, err)
		}

		if !hasOwnership {
			time.Sleep(time.Duration(10) * time.Minute)
		} else {
			break
		}
	}
}

func setScaleIn(pendingMessages int) {
	if pendingMessages > 0 {

	}
}
