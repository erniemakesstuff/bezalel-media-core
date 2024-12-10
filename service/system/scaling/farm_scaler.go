package scaling

import (
	"log"
	"time"

	config "github.com/bezalel-media-core/v2/configuration"
	dal "github.com/bezalel-media-core/v2/dal"
	"github.com/google/uuid"
)

func StartWatching() {
	err := dal.InitDaemonEntry(dal.SYSTEM_RENDER_FARM)
	if err != nil {
		log.Panic(err)
	}

	go processWatch(uuid.New().String())
}

func processWatch(processId string) {
	for { // infinite
		// Check every 10 min if can take process lock
		// if no, wait 10min
		const tenMinutesMilli = 600_000
		waitForOwnership(processId, dal.SYSTEM_RENDER_FARM, tenMinutesMilli)

		// if yes, update status, scaling, and increment expiry every 5 min
		scaleCoreService()
		scaleMediaTextConsumer()
		scaleMediaRenderConsumer()

		dal.TakeSystemLockOwnership(dal.SYSTEM_RENDER_FARM, processId, tenMinutesMilli)
		time.Sleep(time.Duration(5) * time.Minute)
	}
}

func waitForOwnership(processId string, system string, expiryTimeMilli int64) {
	for {
		hasOwnership, err := dal.TakeSystemLockOwnership(system, processId, expiryTimeMilli)
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

func scaleCoreService() {
	pendingMessagesCount, err := getPendingMessagesCount(config.GetEnvConfigs().LedgerQueueName)
	if err != nil {
		log.Printf("error fetching pending ledger messages count: %s", err)
	}
	numTasks := (pendingMessagesCount / config.GetEnvConfigs().ConsumerTaskPerMessages) + 1 // Always at-least one active Service for HTTP-request traffic.
	ScaleEcsTasks(config.GetEnvConfigs().ECSCoreClusterName, numTasks, config.GetEnvConfigs().ECSCoreTaskName)
}

func scaleMediaTextConsumer() {
	pendingMessagesCount, err := getPendingMessagesCount(config.GetEnvConfigs().MediaTextQueueName)
	if err != nil {
		log.Printf("error fetching pending ledger messages count: %s", err)
	}

	if pendingMessagesCount == 0 {
		return
	}

	numTasks := (pendingMessagesCount / config.GetEnvConfigs().ConsumerTaskPerMessages) + 1 // Always at-least one active Service for HTTP-request traffic.
	ScaleEcsTasks(config.GetEnvConfigs().ECSMediaClusterName, numTasks, config.GetEnvConfigs().ECSMediaConsumerTextTaskName)
}

func scaleMediaRenderConsumer() {
	pendingMessagesCount, err := getPendingMessagesCount(config.GetEnvConfigs().MediaRenderQueueName)
	if err != nil {
		log.Printf("error fetching pending ledger messages count: %s", err)
	}

	if pendingMessagesCount == 0 {
		return
	}

	numTasks := (pendingMessagesCount / config.GetEnvConfigs().ConsumerTaskPerMessages) + 1 // Always at-least one active Service for HTTP-request traffic.
	ScaleEcsTasks(config.GetEnvConfigs().ECSMediaClusterName, numTasks, config.GetEnvConfigs().ECSMediaConsumerTextTaskName)
}
