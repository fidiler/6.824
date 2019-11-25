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
	commitIndex int64
	lastApplied int64

	nextIndex  []int
	matchIndex []int

	state state

	applyCh chan ApplyMsg

	commitCh         chan bool
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
	return int(atomic.LoadInt64(&rf.commitIndex))
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	atomic.StoreInt64(&rf.commitIndex, int64(commitIndex))
}

func (rf *Raft) getLastApplied() int {
	return int(atomic.LoadInt64(&rf.lastApplied))
}

func (rf *Raft) setLastApplied(lastApplied int) {
	atomic.StoreInt64(&rf.lastApplied, int64(lastApplied))
}

func (rf *Raft) incrementLastApplied() {
	atomic.AddInt64(&rf.lastApplied, 1)
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
		DPrintf("[rf.stateConvert] Server-%d on conversion to candidate, start election",
			rf.me)

		rf.state.store(state)

	case leader:
		prevState := rf.state.load()
		rf.state.store(state)

		// candidate -> leader
		if prevState == candidate {
			rf.isLeaderCh <- true
		}

		DPrintf("[rf.stateConvert] candidate-%d convert leader, leader logs = %v\n",
			rf.me, rf.logs)
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

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.getCurrentTerm() {
		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = false

	} else {
		// if RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if args.Term > rf.getCurrentTerm() {
			rf.setCurrentTerm(args.Term)
			rf.stateConvert(follower)
			rf.setVotedFor(-1)
			rf.resetElectionTimeoutTime()
		}

		// term equl

		// if votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4）
		if rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateID {
			rf.mu.Lock()
			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIndex].Term
			rf.mu.Unlock()

			// If the logs have last entries with different terms,
			// then the log with the later term is more up-to-date.
			// If the logsend with the same term, then whichever
			// log is longer ismore up-to-date.
			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {

				rf.stateConvert(follower)
				rf.setVotedFor(args.CandidateID)
				rf.resetElectionTimeoutTime()

				reply.Term = rf.getCurrentTerm()
				reply.VoteGranted = true
				return
			}
		}
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
	if term, isLeader = rf.GetState(); isLeader {
		DPrintf("[rf.Start] Start replica Command: %v\n", command)

		rf.mu.Lock()
		rf.logs = append(rf.logs, Log{
			Term:    term,
			Command: command,
		})

		index = len(rf.logs) - 1
		replicated := 1
		rf.mu.Unlock()

		rf.mu.Lock()
		go rf.concurrentSendAppendEntries(term, index, rf.getCommitIndex(), &replicated, "Replica")
		rf.mu.Unlock()
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.getCurrentTerm() <= args.Term {

		if rf.getCurrentTerm() < args.Term {
			rf.setCurrentTerm(args.Term)
			rf.resetElectionTimeoutTime()
			rf.setVotedFor(-1)
			rf.stateConvert(follower)
		}

		rf.mu.Lock()
		if args.PrevLogIndex < len(rf.logs) && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.mu.Unlock()

			// 切换为follower
			rf.stateConvert(follower)

			// 把follower中跟leader冲突的日志条目删除然后追加leader中的日志条目
			rf.mu.Lock()
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
			rf.mu.Unlock()

			// 设置commitIndex
			indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
			if args.LeaderCommit > rf.getCommitIndex() {
				rf.setCommitIndex(args.LeaderCommit)
				if rf.getCommitIndex() > indexOfLastNewEntry {
					rf.setCommitIndex(indexOfLastNewEntry)
				}

				// 提交了日志，通知应用到状态机
				rf.commitCh <- true
			}
			// 重置选举计时器
			rf.resetElectionTimeoutTime()

			reply.Success = true
			reply.Term = rf.getCurrentTerm()

		} else {
			rf.mu.Unlock()

			DPrintf("[rf.AppendEntries] Server-%d (logs = %v) refused Leader-%d (args.Entries = %v) AppendEntires, because log not match,\n",
				rf.me, rf.logs, args.LeaderID, args.Entries)

			// TODO nextIndex快速回溯优化
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

func (rf *Raft) concurrentSendAppendEntries(term, index, commitIndex int, pReplicated *int, name string) {
	majority := len(rf.peers) / 2
	isCommitted := false

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		rf.mu.Lock()
		nextIndex := rf.nextIndex[peer]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		entries := make([]Log, 0)
		if index+1 > nextIndex {
			entries = rf.logs[nextIndex : index+1]
		}
		rf.mu.Unlock()

		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}

		reply := &AppendEntriesReply{}

		go func(peer int, term int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
		Retry:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}

			// 复制日志时的term必须和当前leader的term是同一任期
			if term != rf.getCurrentTerm() {
				return
			}

			ok := rf.sendAppendEntries(peer, args, reply)
			if !ok {
				goto Retry
			}

			DPrintf("Leader-%d sent %s to Server-%d\n", rf.me, name, peer)

			if reply.Success {
				rf.mu.Lock()
				// 更新对应的nextIndex和matchIndex
				if index+1 > rf.nextIndex[peer] {
					rf.matchIndex[peer] = index
					rf.nextIndex[peer] = index + 1
				}
				rf.mu.Unlock()

				rf.mu.Lock()
				*pReplicated++
				if isCommitted == false && *pReplicated > majority && rf.state.load() == leader {
					rf.mu.Unlock()

					// If there exists an N such that N > commitIndex, a majority
					// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
					// set commitIndex = N
					rf.mu.Lock()
					if index > commitIndex && rf.logs[index].Term == rf.getCurrentTerm() {
						isCommitted = true
						rf.mu.Unlock()

						DPrintf("[rf.concurrentSendAppendEntries] Leader-%d commit index: %d\n",
							rf.me, index)

						rf.setCommitIndex(index)

						// 通知已经提交日志，应用日志到状态机
						rf.commitCh <- true

						// 发送一次心跳信息，通知其他服务器节点更新commitIndex
						// rf.startHeartbeat()

					} else {
						rf.mu.Unlock()
					}

				} else {
					rf.mu.Unlock()
				}

			} else {
				if args.Term < reply.Term {
					DPrintf("[rf.concurrentSendAppendEntries] Leader-%d (term: %d) is less than Server-%d (term: %d), convert follower\n",
						rf.me, args.Term, peer, reply.Term)

					rf.setCurrentTerm(reply.Term)
					rf.stateConvert(follower)
					rf.setVotedFor(-1)
					rf.setLeaderID(-1)
					return
				}

				rf.mu.Lock()
				if rf.nextIndex[peer]-1 > 0 {
					rf.nextIndex[peer]--
					rf.mu.Unlock()
					goto Retry

				} else {
					rf.mu.Unlock()
				}
			}

		}(peer, term, args, reply)
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

func (rf *Raft) startHeartbeat() {
	var (
		term     int
		isLeader bool
	)

	if term, isLeader = rf.GetState(); !isLeader {
		return
	}

	// leader每次发送心跳信息时记录一下最新的发送时间戳
	atomic.StoreInt64(&rf.latestSendHeartbeatTime, time.Now().UnixNano())

	rf.mu.Lock()
	index := len(rf.logs) - 1
	replicated := 1
	rf.mu.Unlock()

	rf.mu.Lock()
	go rf.concurrentSendAppendEntries(term, index, rf.getCommitIndex(), &replicated, "Heartbeat")
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimeoutTime() {
	// 重置选举超时时间
	atomic.StoreInt64(&rf.electionTimeoutTime, getElectionTimeoutTime())
	// 重置最新心跳时间
	atomic.StoreInt64(&rf.latestHeartbeatTime, time.Now().UnixNano())
}

func (rf *Raft) startElection() {
	// On conversion to candidate, start election:
	rf.stateConvert(candidate)

	// Increment currentTerm
	rf.incrementCurrentTerm()

	// Vote for self
	voted := 1

	// Reset election timer
	rf.resetElectionTimeoutTime()

	// Send RequestVote RPCs to all other servers
	go func(term int, pVoted *int) {
		majority := len(rf.peers) / 2

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
			rf.mu.Unlock()

			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}

			reply := &RequestVoteReply{}

			go func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
				ok := rf.sendRequestVote(peer, args, reply)
				if !ok {
					return
				}

				if !reply.VoteGranted {
					if reply.Term > rf.getCurrentTerm() {
						rf.setCurrentTerm(reply.Term)
						rf.stateConvert(follower)
						rf.setVotedFor(-1)
					}
					return
				}

				rf.mu.Lock()
				*pVoted++
				// candidate 赢得选举，立即成为leader
				// 然后向其他服务器节点发送心跳消息来确定自己的地位并阻止心的选举
				if *pVoted > majority && rf.state.load() == candidate {
					rf.mu.Unlock()

					rf.stateConvert(leader)
					rf.setLeaderID(rf.me)

					rf.mu.Lock()
					// 初始化nextIndex和matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()

					// 立即发送一次心跳信息，防止follower开始无意义的选举
					rf.startHeartbeat()

				} else {
					rf.mu.Unlock()
				}

			}(peer, args, reply)
		}
	}(rf.getCurrentTerm(), &voted)
}

func (rf *Raft) commitLoop() {
	for {
		select {
		case <-rf.commitCh:
			lastApplied := rf.getLastApplied()
			commitIndex := rf.getCommitIndex()
			for commitIndex > lastApplied {
				lastApplied++
				rf.mu.Lock()
				command := rf.logs[lastApplied].Command
				rf.mu.Unlock()

				rf.applyCh <- ApplyMsg{
					Command: command,
					Index:   lastApplied,
				}

				rf.setLastApplied(lastApplied)
			}
		}
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
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Term: 0})

	rf.applyCh = applyCh

	atomic.StoreInt32(&rf.votedFor, -1)
	atomic.StoreUint32(&rf.currentTerm, 0)
	atomic.StoreInt64(&rf.commitIndex, 0)
	atomic.StoreInt64(&rf.lastApplied, 0)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = state{v: &atomic.Value{}}
	rf.state.store(follower)

	rf.commitCh = make(chan bool)
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

	go rf.commitLoop()
	go rf.eventLoop()
	go rf.electionLoop()
	go rf.leaderLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
