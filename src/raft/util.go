package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func getElectionTimeoutTime() int64 {
	rand.Seed(time.Now().UnixNano())
	n := int64(time.Duration(rand.Intn(300-150)+150) * time.Millisecond)
	return n
}
