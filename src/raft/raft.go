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

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2A
	state          NodeState   //节点状态
	currentTerm    int         //当前任期
	votedFor       int         //给谁投过票
	electionTimer  *time.Timer //选举时间
	heartbeatTimer *time.Timer //心跳时间

	//2B
	logs        []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries

}

type NodeState int
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int     //leader中上一次同步的日志索引
	PrevLogTerm  int     //leader中上一次同步的日志任期
	LeaderCommit int     //领导者的已知已提交的最高的日志条目的索引
	Entries      []Entry //同步日志
}

type AppendEntriesReply struct {
	Term          int  //当前任期号
	Success       bool //是否同步成功
	ConflictTerm  int  //冲突Term
	ConflictIndex int  //冲突Index
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term %v,LeaderId %v,PrevLogIndex %v,PrevLogTerm %v,LeaderCommit %v,Entries %v}", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
}

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 1000
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	//return term, isleader
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == StateLeader

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	// persist currentTerm, votedFor and logs
	data := rf.encodeState()
	if data != nil {
		rf.persister.SaveRaftState(data)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		// decode failed; ignore
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still vaild in term %v}", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snpshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]Entry, 1)
	} else {

		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil

	}

	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotArgs %v and reply InstallSnapshotReply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	// ensure reply term reflects any updates we made above

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteArgs %v and reply requestVoteReply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// only grant vote if haven't voted this term or already voted for this candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		DPrintf("{Node %v} grants vote to %v in term %v", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("{Node %v} rejects vote for %v in term %v (already voted %v)", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor)
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} appends new log %v at index %v in term %v", rf.me, newLog, newLog.Index, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C: //进入候选者状态，进行选举
			rf.mu.Lock()
			rf.ChangeState(StateCandidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C: //领导者发送心跳维持领导力
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		//2A
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		//2B
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
	}
	//rf.peers = peers
	//rf.persister = persister
	//rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// After restoring persisted logs/snapshot dummy entry, align volatile
	// indexes so applier/replicator won't use stale lastApplied/commitIndex
	// that refer to compacted entries. Use the first log's Index (the
	// dummy entry) as the base (this may be the lastIncludedIndex).
	first := rf.getFirstLog().Index
	if rf.commitIndex < first {
		rf.commitIndex = first
	}
	if rf.lastApplied < first {
		rf.lastApplied = first
	}

	//2A
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})

			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections， 用来触发 heartbeat timeout 和 election timeout
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once， ，用来往 applyCh 中 push 提交的日志并保证 exactly once
	go rf.applier()
	return rf
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

// package-level RNG used to generate randomized election timeouts.
// rand.New returns a *rand.Rand which is NOT safe for concurrent use,
// so protect it with a mutex.

type randIntner interface{ Intn(n int) int }
type randFunc func(int) int
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.rand.Intn(n)
	return v
}

var globalRand = &lockedRand{rand: rand.New(rand.NewSource(time.Now().UnixNano()))}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func (rf *Raft) ChangeState(State NodeState) {
	if rf.state == State {
		return
	}

	rf.state = State
	switch State {
	case StateFollower:
		stopAndDrainTimer(rf.heartbeatTimer)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case StateCandidate:
		stopAndDrainTimer(rf.heartbeatTimer)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case StateLeader:
		stopAndDrainTimer(rf.electionTimer)
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
		// Initialize leader volatile state: nextIndex and matchIndex
		// caller should hold rf.mu when calling ChangeState, but be defensive
		lastLog := rf.getLastLog()
		for i := range rf.peers {
			rf.nextIndex[i] = lastLog.Index + 1
			if i == rf.me {
				rf.matchIndex[i] = lastLog.Index
			} else {
				rf.matchIndex[i] = 0
			}
		}
	}
}

func stopAndDrainTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func (rf *Raft) StartElection() {
	args := rf.genRequestVoteArgs()
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)
	//use Closure
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteReply %v from {Node %v} after sending RequestVoteArgs %v in term %v", rf.me, reply, peer, args, rf.currentTerm)
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							// Become leader when receiving majority votes
							rf.ChangeState(StateLeader)
							// send initial empty AppendEntries (heartbeat) to assert leadership
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v ", rf.me, peer, reply.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()

					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesArgs %v and reply AppendEntriesReply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesArgs %v from {Node %v} because preLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PrevLogIndex, rf.getFirstLog().Index)
		return
	}
	// 判断PrevLog存不存在
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		//1.  Follower 的 log 不够新，prevLogIndex 已经超出 log 长度
		if lastIndex < args.PrevLogIndex {
			reply.ConflictIndex = lastIndex + 1
			reply.ConflictTerm = -1 //None
			// 2. Follower prevLogIndex 处存在 log
			// 向主节点上报信息，加速下次复制日志
			// 当PreLogTerm与当前日志的任期不匹配时，找出日志第一个不匹配任期的index
		} else {
			firstIndex := rf.getFirstLog().Index
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		return
	}
	firstIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		idx := entry.Index - firstIndex
		// Defensive: if entry index is before our first log (already compacted by a snapshot),
		// skip it. This can happen when the follower has installed a snapshot and
		// the leader still sends older entries.

		if idx >= len(rf.logs) || rf.logs[idx].Term != entry.Term {
			rf.logs = shrinkEntriesArray(append(rf.logs[:idx], args.Entries[index:]...))
			break
		}
	}

	rf.advanceCommitIndexForFollower(args.LeaderCommit)

	reply.Term, reply.Success = rf.currentTerm, true

}

// advanceCommitIndexForFollower moves follower commitIndex to min(leaderCommit, lastLogIndex).
// Caller must hold rf.mu.
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	if leaderCommit <= rf.commitIndex {
		return
	}

	lastLogIndex := rf.getLastLog().Index
	newCommitIndex := leaderCommit
	if newCommitIndex > lastLogIndex {
		newCommitIndex = lastLogIndex
	}

	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) advanceCommitIndexForLeader() {
	// Caller must hold rf.mu.
	// Find largest N such that N > rf.commitIndex, a majority of matchIndex[i] >= N,
	// and rf.logs[N].Term == rf.currentTerm. Then set rf.commitIndex = N and signal applier.
	firstIndex := rf.getFirstLog().Index
	lastIndex := rf.getLastLog().Index
	for N := lastIndex; N > rf.commitIndex; N-- {
		// only consider entries from current term (leader restriction)
		if N-firstIndex < 0 || N-firstIndex >= len(rf.logs) {
			continue
		}
		if rf.logs[N-firstIndex].Term != rf.currentTerm {
			continue
		}
		cnt := 1 // leader itself
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			return
		}
	}
}

func (rf *Raft) getFirstLog() Entry {
	if len(rf.logs) == 0 {
		return Entry{}
	}
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	if len(rf.logs) == 0 {
		return Entry{}
	}
	return rf.logs[len(rf.logs)-1]
}

// shrinkEntriesArray reduces backing-array capacity when there is significant slack,
// preventing old log segments from being retained in memory after truncation.
func shrinkEntriesArray(entries []Entry) []Entry {
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {
		newEntries := make([]Entry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// 如果不需要为这个 peer 复制条目，只要释放 CPU 并等待其他 goroutine 的信号，如果服务添加了新的命令
		// 如果这个peer需要复制条目，这个goroutine会多次调用replicateOneRound(peer)直到这个peer赶上，然后等待
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}

		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// 心跳维持领导力
func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.replicateOneRound(peer)

		} else {

			rf.replicatorCond[peer].Signal()

		}
	}
}

// Leader 向Follwer 发送复制请求
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()

			rf.handleInstallSnapshotReply(peer, args, reply)
			rf.mu.Unlock()
		}

	} else {
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, args, reply)
			rf.mu.Unlock()
		}
	}
}

// Leader处理Follower 日志复制响应
func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state == StateLeader && rf.currentTerm == args.Term {
		if reply.Success {
			// Advance matchIndex only if it increases. RPCs may be reordered or
			// retried, so ensure we don't move matchIndex backwards.
			newMatch := args.PrevLogIndex + len(args.Entries)
			if newMatch > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatch
			}
			// nextIndex should follow matchIndex
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeader()
		} else {
			if reply.Term > rf.currentTerm {
				rf.ChangeState(StateFollower)
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := args.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesReply %v for AppendEntriesArgs %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), reply, args)
}

func (rf *Raft) appendNewEntry(command interface{}) Entry {
	lastLog := rf.getLastLog()
	newEntry := Entry{
		Index:   lastLog.Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, newEntry)
	// Leader always has its own newest log.
	rf.matchIndex[rf.me] = newEntry.Index
	rf.nextIndex[rf.me] = newEntry.Index + 1
	rf.persist()
	return newEntry
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		// Compute slice bounds relative to current rf.logs and clamp to avoid negative indices
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			// Before sending the ApplyMsg, re-check under lock that this entry
			// hasn't been compacted away by a snapshot installed concurrently.
			rf.mu.RLock()
			curFirst := rf.getFirstLog().Index
			rf.mu.RUnlock()
			if entry.Index <= curFirst {
				// this entry has been superseded by a snapshot; skip it
				DPrintf("{Node %v} skip ApplyMsg idx=%v because current firstIndex=%v (superseded by snapshot)", rf.me, entry.Index, curFirst)
				continue
			}
			// debug: print apply details
			DPrintf("{Node %v} sending ApplyMsg idx=%v term=%v (lastApplied=%v commitIndex=%v firstIndex=%v)", rf.me, entry.Index, entry.Term, lastApplied, commitIndex, firstIndex)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, rf.commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}

}

func (rf *Raft) encodeState() []byte {
	// persist currentTerm, votedFor and logs in this order so readPersist can decode them
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.logs) != nil {
		return nil
	}
	return w.Bytes()
}

func (rf *Raft) matchLog(term int, index int) bool {
	firstIndex := rf.getFirstLog().Index
	lastIndex := rf.getLastLog().Index
	if index < firstIndex || index > lastIndex {
		return false
	}
	return rf.logs[index-firstIndex].Term == term
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[peer].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
}

// handleInstallSnapshotReply is called while holding rf.mu.
func (rf *Raft) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = reply.Term, -1
		rf.persist()
		return
	}

	// If still leader in same term, update nextIndex/matchIndex for peer
	if rf.state == StateLeader && reply.Term == rf.currentTerm {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.advanceCommitIndexForLeader()
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	firstLog := rf.getFirstLog()
	rawSnapshot := rf.persister.ReadSnapshot()
	snapshot := make([]byte, len(rawSnapshot))
	copy(snapshot, rawSnapshot)
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              snapshot,
	}
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	// caller should hold rf.mu (or be reading under RLock). prevLogIndex is index of previous log entry.
	firstIndex := rf.getFirstLog().Index
	lastLogIndex := rf.getLastLog().Index

	var prevLogTerm int
	if prevLogIndex == firstIndex-1 {
		// prevLog refers to the dummy entry before first real entry (shouldn't normally happen here)
		prevLogTerm = rf.logs[0].Term
	} else {
		prevLogTerm = rf.logs[prevLogIndex-firstIndex].Term
	}

	// entries start from prevLogIndex+1 to lastLog.Index
	start := prevLogIndex + 1
	if start > lastLogIndex {
		// no entries to send
		return &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      nil,
		}
	}

	// copy entries
	entries := make([]Entry, lastLogIndex-start+1)
	copy(entries, rf.logs[start-firstIndex:lastLogIndex-firstIndex+1])

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	// caller usually holds rf.mu; don't lock here to avoid deadlock.
	lastLog := rf.getLastLog()
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
}
