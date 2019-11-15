package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "sync/atomic"

// import "bytes"
// import "encoding/gob"

const (
	_ = iota
	follower
	candidate
	leader
)

type state struct {
	v *atomic.Value
}

func (s state) load() int {
	return s.v.Load().(int)
}

func (s state) store(v int) {
	s.v.Store(v)
}

func (s state) name() string {
	v := s.load()
	switch v {
	case follower:
		return "Follower"
	case candidate:
		return "Candidate"
	case leader:
		return "leader"
	default:
		return "unknow"
	}
}

// Log struct
type Log struct {
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs        []Log
	votedFor    int32
	currentTerm uint32

	nextIndex  []int
	matchIndex []int

	state state

	startElectionCh chan bool
	resetElectionCh chan bool

	startHeartbeatCh chan bool

	leaderCond   *sync.Cond
	noLeaderCond *sync.Cond

	electionTimeoutTime      int64 // 选举超时时间
	lastReceiveHeartbeatTime int64 // 最后收到心跳信息时间

}

func (rf *Raft) getCurrentTerm() (term int) {
	return int(atomic.LoadUint32(&rf.currentTerm))
}

func (rf *Raft) setCurrentTerm(term int) {
	atomic.StoreUint32(&rf.currentTerm, uint32(term))
}

func (rf *Raft) incrementCurrentTerm() {
	atomic.AddUint32(&rf.currentTerm, 1)
}

func (rf *Raft) getVotedFor() int {
	return int(atomic.LoadInt32(&rf.votedFor))
}

func (rf *Raft) setVotedFor(voteFor int) {
	atomic.StoreInt32(&rf.votedFor, int32(voteFor))
}

func (rf *Raft) getElectionTimeoutTime() int64 {
	return atomic.LoadInt64(&rf.electionTimeoutTime)
}

func (rf *Raft) setElectionTimeoutTime(timestamp int64) {
	atomic.StoreInt64(&rf.electionTimeoutTime, timestamp)
}

func (rf *Raft) getLastReceiveHeartbeatTime() int64 {
	return atomic.LoadInt64(&rf.lastReceiveHeartbeatTime)
}

func (rf *Raft) setLastReceiveHeartbeatTime(timestamp int64) {
	atomic.StoreInt64(&rf.lastReceiveHeartbeatTime, timestamp)
}

func (rf *Raft) stateConvert(state int) {
	switch rf.state.load() {
	case candidate: // 当前状态是candidate
		rf.state.store(state)

		// candidate -> leader
		if state == leader {
			rf.mu.Lock()

			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}

			rf.mu.Unlock()

			rf.leaderCond.Broadcast()
		}

	case leader:
		rf.state.store(state)
	default:
		rf.state.store(state)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	if rf.state.load() == leader {
		isleader = true
		term = rf.getCurrentTerm()
	}

	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.getCurrentTerm() <= argsx.Term {
		if rf.getCurrentTerm() < args.Term {
			rf.setCurrentTerm(args.Term)
			rf.stateConvert(follower)
			rf.setVotedFor(-1)
		}

		rf.mu.Lock()
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		rf.mu.Lock()

		if rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateID {
			if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				rf.resetElectionCh <- true
				rf.stateConvert(follower)
				rf.setVotedFor(args.CandidateID)
				reply.Term = rf.getCurrentTerm()
				reply.VoteGranted = true

			} else {
				reply.Term = rf.getCurrentTerm()
				reply.VoteGranted = false
			}
		}

	} else {
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) electionLoop() {
	timer := time.NewTimer(time.Duration(generateElectionTimeoutTime()))
	for {
		// leader状态不需要心跳选举计时器
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			rf.leaderCond.Wait()
			rf.mu.Unlock()
		} else {
			select {
			case <-timer.C:
				rf.startElectionCh <- true
			case <-rf.resetElectionCh:
				timer.Reset(time.Duration(generateElectionTimeoutTime()))
			}
		}
	}
}

func (rf *Raft) leaderLoop() {
	timer := time.NewTimer(time.Duration(generateHeartbeatTime()))
	for {
		select {
		case <-timer.C:
			rf.startHeartbeatCh <- true
			time.Sleep(time.Millisecond * 10)
			timer.Reset(time.Duration(generateHeartbeatTime()))
		}
	}
}

func (rf *Raft) startHeartbeat() {
	var (
		term     int
		index    int
		isLeader bool
	)

	if term, isLeader = rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	index = len(rf.logs) - 1
	replicaed := 1
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int, term int, index int, pReplicaed *int) {
		Retry:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}

		}(peer, term, index, &replicaed)
	}

}

func (rf *Raft) startElection() {
	// conversion to candidate
	rf.stateConvert(candidate)

	// increment currentTerm
	rf.incrementCurrentTerm()

	// vote for self
	voted := 1

	// Reset election timer
	rf.resetElectionCh <- true

	// Send RequestVote RPCs to all other servers
	go func() {
		majority := len(rf.peers) / 2
		for peer := range rf.peers {
			// 不需要给自己投票
			if peer == rf.me {
				continue
			}

			// 不是candidate不需要再发送投票请求
			if rf.state.load() != candidate {
				return
			}

			// 并行发起投票请求
			go func(peer int, pVoted *int) {
				rf.mu.Lock()
				lastLogIndex := len(rf.logs) - 1
				lastLogTerm := rf.logs[lastLogIndex].Term
				rf.mu.Unlock()
				args := &RequestVoteArgs{
					Term:         rf.getCurrentTerm(),
					CandidateID:  rf.me,
					LastLogTerm:  lastLogTerm,
					LastLogIndex: lastLogIndex,
				}

				reply := &RequestVoteReply{}

				if rf.sendRequestVote(peer, args, reply) {
					if reply.VoteGranted {
						rf.mu.Lock()

						*pVoted++

						if *pVoted > majority && rf.state.load() == candidate {
							rf.stateConvert(leader)
						}

						rf.mu.Unlock()

					} else if reply.Term > rf.getCurrentTerm() {
						rf.setCurrentTerm(reply.Term)
						rf.stateConvert(follower)
						rf.setVotedFor(-1)
					}
				}
			}(peer, &voted)
		}
	}()
}

func (rf *Raft) eventLoop() {
	for {
		select {
		case <-rf.startElectionCh:
			rf.startElection()
		case <-rf.startHeartbeatCh:

		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Term: 0})

	atomic.StoreInt32(&rf.votedFor, -1)
	atomic.StoreUint32(&rf.currentTerm, 0)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = state{v: &atomic.Value{}}
	rf.state.store(follower)

	rf.startElectionCh = make(chan bool)
	rf.resetElectionCh = make(chan bool)

	rf.startHeartbeatCh = make(chan bool)

	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.noLeaderCond = sync.NewCond(&rf.mu)

	atomic.StoreInt64(&rf.lastReceiveHeartbeatTime, time.Now().UnixNano())
	atomic.StoreInt64(&rf.electionTimeoutTime, generateElectionTimeoutTime())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
