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
	commitIndex int32
	lastApplied int32

	nextIndex  []int
	matchIndex []int

	state state

	startElectionCh  chan bool
	startHeartbeatCh chan bool

	isLeaderCh  chan bool
	notLeaderCh chan bool

	electionTimeoutTime int64
	latestHeartbeatTime int64

	periodHeartbeatTime     int64
	latestSendHeartbeatTime int64

	leaderID int32
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

func (rf *Raft) getLeaderID() int {
	return int(atomic.LoadInt32(&rf.leaderID))
}

func (rf *Raft) setLeaderID(leaderID int) {
	atomic.StoreInt32(&rf.leaderID, int32(leaderID))
}

func (rf *Raft) getCommitIndex() int {
	return int(atomic.LoadInt32(&rf.commitIndex))
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	atomic.StoreInt32(&rf.commitIndex, int32(commitIndex))
}

func (rf *Raft) stateConvert(state int) {
	switch state {
	case follower:
		prevState := rf.state.load()
		rf.state.store(state)

		// leader -> follower
		if prevState == leader {
			rf.notLeaderCh <- true
		}

	case candidate:
		rf.state.store(state)

	case leader:
		prevState := rf.state.load()
		rf.state.store(state)

		// candidate -> leader
		if prevState == candidate {
			rf.isLeaderCh <- true
		}

	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var (
		term     int
		isleader bool
	)

	if rf.state.load() == leader {
		isleader = true
	} else {
		isleader = false
	}

	term = rf.getCurrentTerm()

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
	if rf.getCurrentTerm() <= args.Term {

		if rf.getCurrentTerm() < args.Term {
			rf.setCurrentTerm(args.Term)
			rf.stateConvert(follower)
			rf.setVotedFor(-1)
		}

		rf.resetElectionTimeoutTime()

		// 同一个任期中，每个服务器节点只会投给一个candidate (first-come-first-served)
		if rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateID {

			rf.mu.Lock()
			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIndex].Term
			rf.mu.Unlock()

			// 选举限制（up-to-date）：candidate的日志至少和过半的服务器节点一样新
			// candidate要想获得本地投票，需要满足以下条件之一
			// 1. candidate最新的日志任期号大于服务器节点
			// 2. candidate和服务器节点的最新的日志任期号一样新且candidate的日志条目大于等于服务器节点
			if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				rf.stateConvert(follower)
				rf.setVotedFor(args.CandidateID)
				reply.Term = rf.getCurrentTerm()
				reply.VoteGranted = true

			} else {
				reply.Term = rf.getCurrentTerm()
				reply.VoteGranted = false
			}

		} else {
			reply.Term = rf.getCurrentTerm()
			reply.VoteGranted = false
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

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.getCurrentTerm() <= args.Term {

		if rf.getCurrentTerm() < args.Term {
			rf.setCurrentTerm(args.Term)
			rf.stateConvert(follower)
			rf.setVotedFor(-1)
		}

		rf.mu.Lock()
		if args.PrevLogIndex <= len(rf.logs) && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.mu.Unlock()

			rf.resetElectionTimeoutTime()

			reply.Success = true
			reply.Term = rf.getCurrentTerm()

		} else {
			rf.mu.Unlock()
			reply.Success = false
			reply.Term = rf.getCurrentTerm()
		}

	} else {
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

func (rf *Raft) isElectionTimeout() bool {
	elapseTime := time.Now().UnixNano() - atomic.LoadInt64(&rf.latestHeartbeatTime)

	if elapseTime >= atomic.LoadInt64(&rf.electionTimeoutTime) {
		return true
	}

	return false
}

func (rf *Raft) electionLoop() {

	for {
		// leader状态不需要心跳选举计时器
		// 等待leader退出
		if _, isLeader := rf.GetState(); isLeader {
			<-rf.notLeaderCh

		} else {
			// 判断是否选举超时，超时开始选举
			if rf.isElectionTimeout() {
				rf.startElectionCh <- true
			}

			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) needSendHeartbeat() bool {
	elapsedTime := time.Now().UnixNano() - atomic.LoadInt64(&rf.latestSendHeartbeatTime)
	if elapsedTime >= rf.periodHeartbeatTime {
		return true
	}

	return false
}

func (rf *Raft) leaderLoop() {
	for {
		// 非leader状态不需要发送heartbeat，等待成为leader
		if _, isLeader := rf.GetState(); !isLeader {
			<-rf.isLeaderCh

		} else {

			if rf.needSendHeartbeat() {
				rf.startHeartbeatCh <- true
			}

			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) updateSendHeartbeatTime() {
	atomic.StoreInt64(&rf.latestSendHeartbeatTime, time.Now().UnixNano())
}

func (rf *Raft) startHeartbeat() {
	var (
		term     int
		isLeader bool
	)

	if term, isLeader = rf.GetState(); !isLeader {
		return
	}

	// leader每次发送心跳信息时记录一下最新的发送时间戳
	rf.updateSendHeartbeatTime()

	go func(term int) {
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			go func(peer int, term int) {
				if _, isLeader := rf.GetState(); !isLeader {
					return
				}

				rf.mu.Lock()
				nextIndex := rf.nextIndex[peer]
				prevLogIndex := nextIndex - 1
				prevLogTerm := rf.logs[prevLogIndex].Term
				entries := make([]Log, 0)
				rf.mu.Unlock()

				args := &AppendEntriesArgs{
					Term:         rf.getCurrentTerm(),
					LeaderID:     rf.getLeaderID(),
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
				}

				reply := &AppendEntriesReply{}

				if rf.sendAppendEntries(peer, args, reply) {
					if reply.Success {
					} else {
						if args.Term < reply.Term {
							rf.setCurrentTerm(reply.Term)
							rf.stateConvert(follower)
							rf.setVotedFor(-1)
							rf.setLeaderID(-1)
						}

					}
				}

			}(peer, term)
		}
	}(term)

}

func (rf *Raft) resetElectionTimeoutTime() {
	// 重置选举超时时间
	atomic.StoreInt64(&rf.electionTimeoutTime, getElectionTimeoutTime())
	// 重置最新心跳时间
	atomic.StoreInt64(&rf.latestHeartbeatTime, time.Now().UnixNano())
}

func (rf *Raft) startElection() {
	// conversion to candidate
	rf.stateConvert(candidate)

	// increment currentTerm
	rf.incrementCurrentTerm()

	// vote for self
	voted := 1

	// Reset election timer
	rf.resetElectionTimeoutTime()

	DPrintf("[rf.startElection] Candidate-%d start election\n", rf.me)

	// Send RequestVote RPCs to all other servers
	go func(pVoted *int) {
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
			go func(peer int) {

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
						// candidate 赢得选举，立即成为leader
						// 然后向其他服务器节点发送心跳消息来确定自己的地位并阻止心的选举
						if *pVoted > majority && rf.state.load() == candidate {
							rf.mu.Unlock()
							rf.stateConvert(leader)
							rf.setLeaderID(rf.me)
							DPrintf("%s-%d convert leader\n", rf.state.name(), rf.me)

							rf.mu.Lock()
							// 初始化nextIndex和matchIndex
							for i := range rf.peers {
								rf.nextIndex[i] = len(rf.logs)
								rf.matchIndex[i] = 0
							}
							rf.mu.Unlock()

							// 立即发送一次心跳信息，防止follower开始无意义的选举
							rf.startHeartbeat()

						} else {
							rf.mu.Unlock()
						}

					} else {

						if reply.Term > rf.getCurrentTerm() {
							rf.setCurrentTerm(reply.Term)
							rf.stateConvert(follower)
							rf.setVotedFor(-1)
							rf.setLeaderID(-1)
						}
					}
				}
			}(peer)
		}
	}(&voted)
}

func (rf *Raft) eventLoop() {
	for {
		select {
		case <-rf.startElectionCh:
			rf.startElection()
		case <-rf.startHeartbeatCh:
			rf.startHeartbeat()
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
	atomic.StoreInt32(&rf.commitIndex, 0)
	atomic.StoreInt32(&rf.lastApplied, 0)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = state{v: &atomic.Value{}}
	rf.state.store(follower)

	rf.startElectionCh = make(chan bool)
	rf.startHeartbeatCh = make(chan bool)

	atomic.StoreInt32(&rf.leaderID, -1)

	// 当candidate转变为leader时发送channel
	rf.isLeaderCh = make(chan bool)

	// 初始化等待不是leader channel
	rf.notLeaderCh = make(chan bool)

	// 初始化选举超时时间
	atomic.StoreInt64(&rf.electionTimeoutTime, getElectionTimeoutTime())

	// 初始化最新心跳时间
	atomic.StoreInt64(&rf.latestHeartbeatTime, time.Now().UnixNano())

	// 周期发送心跳时间，Part2A 要求每秒10次
	rf.periodHeartbeatTime = int64(100 * time.Millisecond)

	// 记录最新发送心跳的时间，初始化为0
	atomic.StoreInt64(&rf.latestSendHeartbeatTime, 0)

	go rf.eventLoop()
	go rf.electionLoop()
	go rf.leaderLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
