package raft

import (
	"log"
	"math/rand"
)
import "time"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func generateElectionTimeoutTime() time.Duration {
	n := time.Duration(rand.Intn(300-150)+150) * time.Millisecond
	return n
}

func generateHeartbeatTime() int64 {
	return int64(100 * time.Millisecond)
}
