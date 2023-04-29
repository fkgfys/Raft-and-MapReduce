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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
)

// import "bytes"
// import "cs350/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const (
	FOLLOWER = 1
	CANDIDATE = 2
	LEADER = 3
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	

	term              int
	currentTerm       int
	votedFor          int
	log               []LogEntry
	commitIndex       int
	lastApplied       int
	state             int
	numVotes          int
	nextIndex         []int
	matchIndex        []int
	applyCh           chan ApplyMsg
	votedCh           chan bool
	electionCh        chan bool
	heartbeatCh       chan bool
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()].Term
}

func (rf *Raft) getLog(index int) LogEntry {
	if index > rf.lastLogIndex() {
		return rf.log[0]
	}

	index = index - rf.lastIncludedIndex

	if index <= 0 {
		return rf.log[0]
	} else {
		return rf.log[index]
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	if rf.killed() {
		return
	}
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	//DPrintf("Log persisted: %d", rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistForSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	//DPrintf("Log persisted: %d", rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		// DPrintf("New server, nothing to read: %d", rf.me)
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var votedFor int
	var currentTerm int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&log) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil {
		return
	} else {
	  rf.log = log
	  rf.votedFor = votedFor
	  rf.currentTerm = currentTerm
	  rf.lastIncludedIndex = lastIncludedIndex
	  rf.lastIncludedTerm = lastIncludedTerm
	//   rf.lastApplied = lastIncludedIndex
	//   rf.commitIndex = lastIncludedIndex
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int 
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// DPrintf("%d by InstallSnapshot", 9999)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%d", 9999)

	if rf.currentTerm > args.Term || args.Data == nil {
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	// DPrintf("New base for Server %d: %d, its old base: %d, its current log: %d", rf.me, args.LastIncludedIndex, rf.lastIncludedIndex, rf.log)
	// if args.LastIncludedIndex < len(rf.log) - 1 + rf.lastIncludedIndex {
	// 	rf.log = rf.log[args.LastIncludedIndex - rf.lastIncludedIndex:]
	// } else {
	// 	rf.log = make([]LogEntry, 0)
	// }
	
	// DPrintf("Server %d's current log: %d", rf.me, rf.log)
	// rf.snapshot = args.Data
	// rf.lastIncludedIndex = args.LastIncludedIndex
	// rf.lastIncludedTerm = args.LastIncludedTerm
	// rf.persist()
	// if args.LastIncludedIndex > rf.lastApplied {
	// 	rf.lastApplied = args.LastIncludedIndex
	// }
	// if args.LastIncludedIndex > rf.commitIndex {
	// 	rf.commitIndex = args.LastIncludedIndex
	// }
	// rf.persist()

	msg := ApplyMsg {
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	go func() {
		rf.applyCh <- msg	
	}()

	rf.persister.SaveStateAndSnapshot(rf.persistForSnapshot(), args.Data)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%d", 9999)

	if ok {

		if rf.currentTerm != args.Term || rf.state != LEADER {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.persist()
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1

	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex <= len(rf.log) - 1 + rf.lastIncludedIndex && rf.lastIncludedTerm == lastIncludedTerm {
		rf.log = append([]LogEntry(nil), rf.log[lastIncludedIndex - rf.lastIncludedIndex:]...)
	} else {
		rf.log = append([]LogEntry(nil), LogEntry{Term: lastIncludedTerm})
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	if lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = lastIncludedIndex
	}
	if lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = lastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.persistForSnapshot(), snapshot)


	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}
	// Your code here (2D).
	go func() {	

		rf.mu.Lock()
		defer rf.mu.Unlock()
		
		// DPrintf("%d", 9999)

		if index <= rf.lastIncludedIndex {
			return
		}

		// rf.log = rf.log[index - rf.lastIncludedIndex:]
		// DPrintf("Server %d's current log: %d", rf.me, rf.log)
		// rf.snapshot = snapshot
		// rf.lastIncludedIndex = index
		// rf.lastIncludedTerm = rf.log[0].Term
		// if index > rf.lastApplied {
		// 	rf.lastApplied = index
		// }
		// if index > rf.commitIndex {
		// 	rf.commitIndex = index
		// }

		rf.log = rf.log[index - rf.lastIncludedIndex:]
		rf.lastIncludedIndex = index
		if index == len(rf.log) + rf.lastIncludedIndex {
			rf.lastIncludedTerm = rf.log[len(rf.log) - 1].Term
		} else {
			rf.lastIncludedTerm = rf.log[index - rf.lastIncludedIndex].Term
		}
		rf.snapshot = snapshot
		if index > rf.lastApplied {
			rf.lastApplied = index
		}
		if index > rf.commitIndex {
			rf.commitIndex = index
		}
	
		rf.persister.SaveStateAndSnapshot(rf.persistForSnapshot(), snapshot)
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.upToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.votedCh <- true
	}

	return
}

func (rf *Raft) upToDate(cTerm int, cIndex int) bool {
	return cTerm > rf.log[len(rf.log) - 1].Term || (cTerm == rf.log[len(rf.log) - 1].Term && cIndex >= len(rf.log) - 1 + rf.lastIncludedIndex)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {

		if rf.state != CANDIDATE || rf.currentTerm != args.Term {
			return ok
		}

		if rf.currentTerm < reply.Term {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		if reply.VoteGranted {
			rf.numVotes++
			if rf.numVotes > len(rf.peers) / 2 {
				rf.state = LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
					rf.matchIndex[i] = len(rf.log) + rf.lastIncludedIndex - 1
				}
				// DPrintf("Server %d becomes a leader!!!", rf.me)
				time.Sleep(time.Millisecond * 60)
				rf.electionCh <- true
			}
		}
		

	}

	return ok
}

func (rf *Raft) startVoting() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1 + rf.lastIncludedIndex,
		LastLogTerm:  rf.log[len(rf.log) - 1].Term,
	}



	if rf.state == CANDIDATE {
		for i := range rf.peers {
			if i != rf.me {
				reply := &RequestVoteReply{}
				go rf.sendRequestVote(i, args, reply)
			}
		}
	}
}

func (rf *Raft) apply() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		log := ApplyMsg {
			CommandValid: true,
			Command:      rf.log[rf.lastApplied - rf.lastIncludedIndex].Command,
			CommandIndex: rf.lastApplied,
		}
		// DPrintf("Server %d's current log: %d",rf.me, rf.log)
		rf.applyCh <- log
		// DPrintf("Server %d applied log %d!!!", rf.me, rf.lastApplied)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Success = false

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if rf.state == FOLLOWER || rf.state == CANDIDATE {
		reply.Term = rf.currentTerm

		rf.heartbeatCh <- true
		temp := args.PrevLogIndex - rf.lastIncludedIndex
		if temp < 0 {
			temp = 0
		}
		if (args.PrevLogIndex > len(rf.log) - 1 + rf.lastIncludedIndex || rf.log[temp].Term != args.PrevLogTerm) {
			return
		}

		for ety := range args.Entries {
			i := args.PrevLogIndex + ety + 1
			temp := i - rf.lastIncludedIndex
			if temp < 0 {
				temp = 0
			}
			if i >= len(rf.log) + rf.lastIncludedIndex || (i < len(rf.log) + rf.lastIncludedIndex && rf.log[temp].Term != args.Entries[ety].Term) {
				if i < len(rf.log) + rf.lastIncludedIndex {
					rf.log = rf.log[:temp]
					rf.persist()
				}
				break
			}
		}

		for ety := range args.Entries {
			i := args.PrevLogIndex + ety + 1
			if i >= len(rf.log) + rf.lastIncludedIndex {
				rf.log = append(rf.log, args.Entries[ety])
			} else {
				temp := i - rf.lastIncludedIndex
				if temp < 0 {
					temp = 0
				}
				rf.log[temp] = args.Entries[ety]
			}
		}
		rf.persist()

		reply.Success = true
		// DPrintf("Server %d agrees!!!", rf.me)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1 + rf.lastIncludedIndex)
			go rf.apply()
			return
		}
	}
	return
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != LEADER || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		rf.commitIndex = rf.lastIncludedIndex
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex + len(args.Entries) + 1)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		plt := args.PrevLogTerm
		for rf.log[args.PrevLogIndex - rf.lastIncludedIndex].Term == plt {
			args.PrevLogIndex--
			if args.PrevLogIndex < rf.lastIncludedIndex {
				break
			}
		}
		rf.nextIndex[server] = args.PrevLogIndex + 1

	}
	for N := len(rf.log) - 1 + rf.lastIncludedIndex; N > rf.commitIndex; N-- {
		count := 1
		for i := range rf.peers {
			if (rf.log[N - rf.lastIncludedIndex].Term != rf.currentTerm) {
				break
			}

			if i != rf.me && rf.matchIndex[i] >= N && rf.log[N - rf.lastIncludedIndex].Term == rf.currentTerm {
				count++
				// DPrintf("Current number of agreement for Server %d: %d", rf.me, count)
			}
		}
		if count > len(rf.peers) / 2 {
			// DPrintf("Server %d succeeds to commit log %d: %d!!!", rf.me, N, rf.log[N - rf.lastIncludedIndex].Command)
			rf.commitIndex = N
			go rf.apply()
			break
		}
	}

	return ok
}

func max(a int, b int) int {
	if a <= b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {

		if i != rf.me && rf.state == LEADER {

			if rf.nextIndex[i] <= rf.lastIncludedIndex {

				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
	
				reply := &InstallSnapshotReply{}
				
				go rf.sendInstallSnapshot(i, args, reply)

				// DPrintf("%d by sendHeartBeat", 9999)

			} else {	

				pli := rf.nextIndex[i] - 1
				if pli == len(rf.log) - 1 + rf.lastIncludedIndex + 1 {
					pli = len(rf.log) - 1 + rf.lastIncludedIndex
				}

				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: pli,
					PrevLogTerm:  rf.log[pli - rf.lastIncludedIndex].Term,
					LeaderCommit: rf.commitIndex,
					Entries: nil,
				}

				if len(rf.log) - 1 + rf.lastIncludedIndex >= rf.nextIndex[i] {
					args.Entries = rf.log[rf.nextIndex[i] - rf.lastIncludedIndex:]
				}

				reply := &AppendEntriesReply{}

				go rf.sendAppendEntries(i, args, reply)

			}
		}
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.state == LEADER)

	if isLeader {
		index = len(rf.log) + rf.lastIncludedIndex
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		// DPrintf("Server %d's current log: %d",rf.me, rf.log)
		rf.persist()
	}

	return index, term, isLeader
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
	// DPrintf("Server %d has been killed!!!", rf.me)
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
		
		rf.mu.Lock()
		s := rf.state
		rf.mu.Unlock()
		switch s {
			case FOLLOWER:
				select {
				case <- rf.votedCh:
				case <- rf.heartbeatCh:
				case <- time.After(time.Millisecond * time.Duration(rand.Intn(500) + 500)):
					rf.mu.Lock()
					rf.state = CANDIDATE
					rf.mu.Unlock()
				}
			case CANDIDATE: 
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.numVotes = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.startVoting()
				select {
				case <- rf.heartbeatCh:
					rf.mu.Lock()
					rf.state = FOLLOWER
					rf.mu.Unlock()
				case <- rf.electionCh:
					time.Sleep(time.Millisecond * 300)
				case <- time.After(time.Millisecond * time.Duration(rand.Intn(500) + 500)):
				}
			case LEADER:
				time.Sleep(time.Millisecond * 60)
				go rf.sendHeartBeat()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = rf.lastIncludedTerm
	
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	
	rf.applyCh = applyCh
	
	rf.votedCh = make(chan bool, 100)
	rf.electionCh = make(chan bool, 100)
	rf.heartbeatCh = make(chan bool, 100)
	rf.lastIncludedIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludedIndex >= 1 {
		go func() {
				rf.applyCh <- ApplyMsg {
					SnapshotValid: true,
					Snapshot:      rf.persister.ReadSnapshot(),
					SnapshotTerm:  rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
			}()
	} else {
		log := ApplyMsg {
			CommandValid: true,
			CommandIndex: 0,
			Command: 0,
		}
		rf.applyCh <- log
	}

	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
		// rf.commitIndex = rf.lastIncludedIndex
	}

	// rf.snapshot = persister.ReadSnapshot()

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
		rf.matchIndex[i] = 0 + rf.lastIncludedIndex
	}

	// DPrintf("Here comes Server %d with commitIndex of %d and lastApplied of %d!!!", rf.me, rf.commitIndex, rf.lastApplied)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

