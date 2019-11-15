package raft

import "log"
import "math/rand"
import "time"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func generateElectionTimeoutTime() int64 {
	return int64(time.Duration(rand.Intn(300-150)+150) * time.Millisecond)
}

func generateHeartbeatTime() int64 {
	return int64(100 * time.Millisecond)
}
