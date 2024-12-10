package scaling

import (
	"log"
	"time"

	dal "github.com/bezalel-media-core/v2/dal"
	"github.com/google/uuid"
)

func StartHeartbeatWatch() {
	err := dal.InitDaemonEntry(dal.SYSTEM_HEARTBEAT_MONITOR)
	if err != nil {
		log.Panic(err)
	}

	go processWatch(uuid.New().String())
}

func processWatch(processId string) {
	for { // infinite
		// Check every 3 min if can take process lock
		// if no, wait 5min
		const sixMinutes = 360000
		waitForOwnership(processId, dal.SYSTEM_HEARTBEAT_MONITOR, sixMinutes)
		processHeartbeats()
		dal.TakeSystemLockOwnership(dal.SYSTEM_HEARTBEAT_MONITOR, processId, sixMinutes)
		time.Sleep(time.Duration(5) * time.Minute)
	}
}

func processHeartbeats() {
	heartbeatEntries, err := getAllHeartBeatEntries()
	if err != nil {
		return
	}
	for _, h := range heartbeatEntries {
		err = dal.IncrementHeartbeat(h.LedgerID, h.HeartbeatCount)
		if err != nil {
			log.Printf("correlationID: %s error incrementing heartbeat: %s", h.LedgerID, err)
		}
	}

}

func getAllHeartBeatEntries() ([]dal.HeartbeatEntry, error) {
	results := []dal.HeartbeatEntry{}
	pk := ""
	sk := ""
	var err error
	var queryResults []dal.HeartbeatEntry
	completedInitialCall := false
	for pk != "" || !completedInitialCall {
		queryResults, pk, sk, err = dal.GetHeartbeatEntries(pk, sk)
		if err != nil {
			log.Printf("error retrieving heartbeat entries: %s", err)
			return results, err
		}
		results = append(results, queryResults...)
		completedInitialCall = true
	}

	return results, err
}

func waitForOwnership(processId string, system string, expiryMilli int64) {
	for {
		hasOwnership, err := dal.TakeSystemLockOwnership(system, processId, expiryMilli)
		if err != nil {
			log.Printf("error verifying lock ownership for system %s: %s", system, err)
		}

		if !hasOwnership {
			time.Sleep(time.Duration(3) * time.Minute)
		} else {
			break
		}
	}
}
