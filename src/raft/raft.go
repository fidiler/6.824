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
import "bytes"
import "encoding/gob"

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
	votedFor    int
	currentTerm int
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state state

	applyCh chan ApplyMsg

	// commitCh         chan bool
	startElectionCh  chan bool
	startHeartbeatCh chan bool

	commitCond    *sync.Cond
	isLeaderCond  *sync.Cond
	notLeaderCond *sync.Cond

	electionTimeoutTime int64
	latestHeartbeatTime int64

	periodHeartbeatTime     int64
	latestSendHeartbeatTime int64

	leaderID int
}

func (rf *Raft) stateConvert(state int) {
	switch state {
	case follower:
		prevState := rf.state.load()
		rf.state.store(state)

		// leader -> follower
		if prevState == leader {
			rf.notLeaderCond.Broadcast()
		}

	case candidate:
		rf.state.store(state)

	case leader:
		prevState := rf.state.load()
		rf.state.store(state)

		// candidate -> leader
		if prevState == candidate {
			rf.isLeaderCond.Broadcast()
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
	}

	rf.mu.Lock()
	term = rf.currentTerm
	rf.mu.Unlock()

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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	rf.mu.Unlock()
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

	rf.mu.Lock()
	if rf.currentTerm <= args.Term {

		// if RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1

			rf.persist()

			rf.stateConvert(follower)

			rf.resetElectionTimeoutTime()
		}

		// if votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4）
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIndex].Term

			// If the logs have last entries with different terms,
			// then the log with the later term is more up-to-date.
			// If the logsend with the same term, then whichever
			// log is longer ismore up-to-date.
			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {

				rf.votedFor = args.CandidateID

				rf.persist()

				rf.stateConvert(follower)

				rf.resetElectionTimeoutTime()

				reply.Term = rf.currentTerm
				reply.VoteGranted = true
			}
		}

		rf.mu.Unlock()

	} else {
		// 1. Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
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

func (rf *Raft) parallelSendRequestVote(pVoted *int) {
	majority := len(rf.peers)/2 + 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if rf.state.load() != candidate {
			return
		}

		rf.mu.Lock()
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogTerm:  lastLogTerm,
			LastLogIndex: lastLogIndex,
		}

		reply := &RequestVoteReply{}
		rf.mu.Unlock()

		go func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
			ok := rf.sendRequestVote(peer, args, reply)
			if !ok {
				return
			}
			if reply.VoteGranted == false {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1

					rf.persist()

					rf.stateConvert(follower)

				}
				rf.mu.Unlock()

			} else {
				rf.mu.Lock()
				if *pVoted++; *pVoted >= majority && rf.state.load() == candidate {
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = 0
					}

					DPrintf("[rf.stateConvert] candidate-%d convert leader  (term = %d), leader logs = %v\n",
						rf.me, rf.currentTerm, rf.logs)

					rf.leaderID = rf.me

					rf.stateConvert(leader)

					// rf.startHeartbeat()

					// rf.persist()
				}
				rf.mu.Unlock()
			}
		}(peer, args, reply)
	}
}

func (rf *Raft) startElection() {
	// On conversion to candidate, start election:
	rf.stateConvert(candidate)

	// Increment currentTerm
	rf.mu.Lock()
	rf.currentTerm++

	// Vote for self
	rf.votedFor = rf.me

	rf.persist()

	DPrintf("[rf.stateConvert] Server-%d on conversion to candidate (term = %d), start election\n",
		rf.me, rf.currentTerm)

	rf.mu.Unlock()

	// Reset election timer
	rf.resetElectionTimeoutTime()

	// Send RequestVote RPCs to all other servers
	voted := 1
	go rf.parallelSendRequestVote(&voted)
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
		if _, isLeader := rf.GetState(); isLeader {
			// leader状态不需要选举计时器
			// 等待leader变为follower
			rf.mu.Lock()
			rf.notLeaderCond.Wait()
			rf.mu.Unlock()
		} else {
			// follower状态需要选举计时器
			// 等待选举计时器超时
			if rf.isElectionTimeout() {
				rf.startElectionCh <- true
			}

			time.Sleep(time.Millisecond * 10)
		}
	}
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
	if term, isLeader = rf.GetState(); isLeader {
		// If command received from client: append entry to local log,
		// respond after entry applied to state machine (§5.3)
		rf.mu.Lock()
		log := Log{
			Term:    term,
			Command: command,
		}
		rf.logs = append(rf.logs, log)

		rf.persist()

		index = len(rf.logs) - 1
		replicated := 1
		rf.mu.Unlock()

		go rf.parallelSendAppendEntries(term, index, &replicated, "Replica")
	}

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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
	IsConflict    bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.currentTerm <= args.Term {
		// Figure 2:
		// All Servers:
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()

			rf.stateConvert(follower)

			rf.resetElectionTimeoutTime()
		}

		// Figure 2:
		// AppednEntries:
		// 2.  Reply false if log doesn’t contain an entry at prevLogIndex
		//	   whose term matches prevLogTerm (§5.3)
		if len(rf.logs) > args.PrevLogIndex && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			isMatch := true
			nextIndex := args.PrevLogIndex + 1
			end := len(rf.logs) - 1
			for i := 0; isMatch && i < len(args.Entries); i++ {
				// 如果args.Entries还有元素，而log已经达到结尾，则不匹配
				if end < nextIndex+i {
					isMatch = false
				} else if rf.logs[nextIndex+i].Term != args.Entries[i].Term {
					isMatch = false
				}
			}

			if isMatch == false {
				// 2.1. 进行日志复制，并更新commitIndex
				// 注意，这里没有直接使用rf.Log = append(rf.Log[:nextIndex], args.Entries...)，
				// 而是先申请和args.Entries大小相同的切片，然后对其进行复制，这么做是因为前一种做法
				// 在使用-race进行测试时，出现了竞争条件，报错说这里对args进行了写操作，猜测可能是因为
				// args.Entries...的省略号相当于把切片包含的所有元素打散后传入，这里可能涉及到写操作？
				entries := make([]Log, len(args.Entries))
				copy(entries, args.Entries)
				rf.logs = append(rf.logs[:nextIndex], entries...) // [0, nextIndex) + entries
			}

			// Figure 2
			// AppendEntriesRPC
			// 5.  If leaderCommit > commitIndex, set commitIndex =
			// 	   min(leaderCommit, index of last new entry)
			indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > indexOfLastNewEntry {
					rf.commitIndex = indexOfLastNewEntry
				}

				rf.commitCond.Signal()
				// DPrintf("[rf.AppendEntries] Server-%d commit index = %d", rf.me, rf.getCommitIndex())
			}

			rf.persist()

			rf.stateConvert(follower)

			rf.resetElectionTimeoutTime()

			reply.Success = true
			reply.Term = rf.currentTerm

			rf.mu.Unlock()

		} else {
			// DPrintf("[rf.AppendEntries] Server-%d (logs = %v, term = %d) refused Leader-%d (args.Entries = %v, term = %d) AppendEntires, because log not match,\n",
			// rf.me, rf.logs, rf.getCurrentTerm(), args.LeaderID, args.Entries, args.Term)

			// nextIndex快速回溯优化
			if len(rf.logs) <= args.PrevLogIndex {
				reply.ConflictIndex = len(rf.logs)
				reply.ConflictTerm = -1
				reply.Success = false
				reply.Term = rf.currentTerm
				rf.mu.Unlock()

			} else if len(rf.logs) > args.PrevLogIndex && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
				for i, log := range rf.logs {
					if log.Term == reply.ConflictTerm {
						reply.ConflictIndex = i
						break
					}
				}
				reply.Success = false
				reply.Term = rf.currentTerm
				rf.mu.Unlock()

			} else {
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.ConflictIndex = 0
				reply.ConflictTerm = -1
				rf.mu.Unlock()
			}
		}

	} else {
		// DPrintf("[rf.AppendEntries] Server-%d (term = %d) refused Leader-%d (term = %d) AppendEntires, because term not match,\n",
		// rf.me, rf.getCurrentTerm(), args.LeaderID, args.Term)
		// Figure 2:
		// AppendEntries RPC:
		// 1.  Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) parallelSendAppendEntries(term, lastLogIndex int, pReplicated *int, name string) {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	majority := len(rf.peers)/2 + 1
	majorityAgree := false

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int, term int) {
		Retry:
			if rf.state.load() != leader {
				return
			}

			rf.mu.Lock()
			if rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			rf.mu.Lock()
			entries := make([]Log, 0)
			nextIndex := rf.nextIndex[peer]
			if lastLogIndex+1 > nextIndex {
				entries = rf.logs[nextIndex : lastLogIndex+1]
			}

			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.logs[prevLogIndex].Term
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			reply := &AppendEntriesReply{}
			rf.mu.Unlock()

			// if len(entries) > 0 {
			// DPrintf("Leader-%d (term = %d) send AppendEntries (entries = %v) to Server-%d\n",
			// rf.me, rf.getCurrentTerm(), entries, peer)
			// }

			ok := rf.sendAppendEntries(peer, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.Success {
				// If successful: update nextIndex and matchIndex for
				// follower (§5.3)
				rf.mu.Lock()
				if lastLogIndex+1 > nextIndex {
					rf.matchIndex[peer] = lastLogIndex
					rf.nextIndex[peer] = lastLogIndex + 1
				}

				if *pReplicated++; *pReplicated >= majority && !majorityAgree && rf.state.load() == leader {
					majorityAgree = true

					// rf.mu.Lock()
					// for i := range rf.peers {
					// 	if rf.matchIndex[i] >= lastLogIndex {
					// 		majorityCommitCnt++
					// 	}
					// }
					// if majorityCommitCnt >= majority {
					// 	majorityMatchCommitted = true
					// }
					// DPrintf("majorityCommitCnt = %d, majorityMatchCommitted = %v\n", majorityCommitCnt, majorityMatchCommitted)
					// rf.mu.Unlock()

					// If there exists an N such that N > commitIndex, a majority
					// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
					// set commitIndex = N
					if lastLogIndex > rf.commitIndex && rf.logs[lastLogIndex].Term == term {

						// DPrintf("[rf.concurrentSendAppendEntries] Leader-%d commit index: %d\n",
						// rf.me, lastLogIndex)

						rf.commitIndex = lastLogIndex

						rf.commitCond.Signal()
					}

				}

				rf.mu.Unlock()

			} else {
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if args.Term < reply.Term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.leaderID = -1
					rf.persist()
					rf.stateConvert(follower)
					rf.mu.Unlock()

					DPrintf("[rf.concurrentSendAppendEntries] Leader-%d term is less than Server-%d, convert follower\n",
						rf.me, peer)

				} else {
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (§5.3）
					rf.mu.Lock()

					// follower没有包含args.PrevLogIndex
					if reply.ConflictIndex != 0 && reply.ConflictTerm == -1 {
						rf.nextIndex[peer] = reply.ConflictIndex

					} else if reply.ConflictIndex != 0 && reply.ConflictTerm != -1 {
						// 冲突的日志任期在leafer日志中找到该任期第一个位置的索引
						firstConflictTermOfIndex := -1
						for i, log := range rf.logs {
							if log.Term == reply.ConflictTerm {
								firstConflictTermOfIndex = i
								break
							}
						}

						// 没有在leader日志中找到冲突日志的第一个任期的索引
						if firstConflictTermOfIndex == -1 {
							rf.nextIndex[peer] = reply.ConflictIndex
						} else {
							lastConflictTermOfIndex := firstConflictTermOfIndex + 1
							// 从这个位置开始，找到leader日志条目位于该任期期间的最后一个条目索引
							for i := firstConflictTermOfIndex; i < len(rf.logs); i++ {
								if rf.logs[i].Term != reply.ConflictTerm {
									lastConflictTermOfIndex = i + 1
									break
								}
							}
							rf.nextIndex[peer] = lastConflictTermOfIndex
						}

					} else {
						rf.nextIndex[peer]--
					}
					rf.mu.Unlock()
					goto Retry
				}
			}

		}(peer, term)
	}
}

func (rf *Raft) startHeartbeat() {
	var (
		term     int
		isLeader bool
	)

	if term, isLeader = rf.GetState(); isLeader {
		// leader每次发送心跳信息时记录一下最新的发送时间戳
		atomic.StoreInt64(&rf.latestSendHeartbeatTime, time.Now().UnixNano())

		rf.mu.Lock()
		index := len(rf.logs) - 1
		replicated := 1
		rf.mu.Unlock()
		go rf.parallelSendAppendEntries(term, index, &replicated, "Heartbeat")
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
			rf.mu.Lock()
			rf.isLeaderCond.Wait()
			rf.mu.Unlock()
		} else {
			if rf.needSendHeartbeat() {
				rf.startHeartbeatCh <- true
			}

			time.Sleep(time.Millisecond * 10)
		}
	}
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

func (rf *Raft) resetElectionTimeoutTime() {
	// 重置选举超时时间
	atomic.StoreInt64(&rf.electionTimeoutTime, getElectionTimeoutTime())
	// 重置最新心跳时间
	atomic.StoreInt64(&rf.latestHeartbeatTime, time.Now().UnixNano())
}

func (rf *Raft) commitLoop() {
	for {
		rf.mu.Lock()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		if lastApplied == commitIndex {
			rf.mu.Lock()
			rf.commitCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			for i := lastApplied + 1; i <= commitIndex; i++ {
				msg := ApplyMsg{
					Command: rf.logs[i].Command,
					Index:   i,
				}
				rf.applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
			// for commitIndex > lastApplied {
			// 	lastApplied++
			// 	rf.mu.Lock()
			// 	command := rf.logs[lastApplied].Command
			// 	rf.mu.Unlock()

			// 	rf.applyCh <- ApplyMsg{
			// 		Command: command,
			// 		Index:   lastApplied,
			// 	}

			// 	rf.setLastApplied(lastApplied)
			// }
		}

		// select {
		// case <-rf.commitCh:
		// 	lastApplied := rf.getLastApplied()
		// 	commitIndex := rf.getCommitIndex()
		// 	for commitIndex > lastApplied {
		// 		lastApplied++
		// 		rf.mu.Lock()
		// 		command := rf.logs[lastApplied].Command
		// 		rf.mu.Unlock()

		// 		rf.applyCh <- ApplyMsg{
		// 			Command: command,
		// 			Index:   lastApplied,
		// 		}

		// 		rf.setLastApplied(lastApplied)
		// 	}
		// }
	}
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
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderID = -1
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = state{v: &atomic.Value{}}
	rf.state.store(follower)
	// rf.commitCh = make(chan bool)
	rf.startElectionCh = make(chan bool)
	rf.startHeartbeatCh = make(chan bool)
	// 当candidate转变为leader时发送channel
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.isLeaderCond = sync.NewCond(&rf.mu)
	rf.notLeaderCond = sync.NewCond(&rf.mu)
	// 初始化选举超时时间
	atomic.StoreInt64(&rf.electionTimeoutTime, getElectionTimeoutTime())
	// 初始化最新心跳时间
	atomic.StoreInt64(&rf.latestHeartbeatTime, time.Now().UnixNano())
	// 周期发送心跳时间，Part2A 要求每秒10次
	rf.periodHeartbeatTime = int64(100 * time.Millisecond)
	// 记录最新发送心跳的时间，初始化为0
	atomic.StoreInt64(&rf.latestSendHeartbeatTime, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionLoop()
	go rf.leaderLoop()
	go rf.eventLoop()
	go rf.commitLoop()

	return rf
}
