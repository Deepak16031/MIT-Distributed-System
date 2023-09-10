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
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command string
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persisten
	currentTerm int
	votedFor    int
	log         []Log

	//volatile
	//for all
	lastTimeCalled time.Time
	commitIndex    int
	lastApplied    int
	waitTime       time.Duration

	//leaders
	nextIndex  []int
	matchIndex []int
	state      State
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	defer rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[%d] received request from %d in term %d", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if -1 == rf.votedFor {
		reply.VoteGranted = isUptoDate(args, rf)
		rf.lastTimeCalled = time.Now()
		DPrintf("[%d] Grant Vote to %d in Term %d", rf.me, args.CandidateId, args.Term)
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
}

func isUptoDate(args *RequestVoteArgs, rf *Raft) bool {
	rfLogLen := len(rf.log)
	if rf.log[rfLogLen-1].Term > args.LastLogTerm {
		return false
	}
	//  cant give vote, "not up-to-date" $5.4.1
	if rf.log[rfLogLen-1].Term == args.LastLogTerm && rfLogLen > args.LastLogIndex {
		return false
	}
	return true
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	DPrintf("[%d] Received Append Entries from  %d in Term %d", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.lastTimeCalled = time.Now()
	DPrintf("I am here")

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	//DPrintf("Raft Log Length : %d and arguments PrevLogIndex : %d and Term: %d", len(rf.log), args.PrevLogIndex, args.Term)
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.Term {
		reply.Success = false
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	// TODO new entries
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

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// no need to take a lock, lastTimeCalled always increasing
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(rf.waitTime)
		if !(time.Since(rf.lastTimeCalled) < time.Duration(rf.waitTime)) {
			go rf.startVoting()
		}

	}
}

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

func (rf *Raft) startVoting() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.votedFor = rf.me
	ms := 300 + rand.Int63()%300
	rf.waitTime = time.Duration(ms) * time.Millisecond
	rf.lastTimeCalled = time.Now()
	rf.currentTerm++

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	DPrintf("[%d] started election in term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	sendTerm := args.Term
	var cond = sync.NewCond(&rf.mu)
	totalVote := 1
	successVotes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)
			rf.mu.Lock()
			if sendTerm != rf.currentTerm || rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				DPrintf("[%d] Received vote from %d in term %d", rf.me, i, args.Term)
				successVotes += 1
				cond.Broadcast()
				rf.mu.Unlock()
			}
		}(i)
	}

	rf.mu.Lock()
	for !(successVotes > len(rf.peers)/2 || totalVote == len(rf.peers)) {
		cond.Wait()
	}
	rf.mu.Unlock()

	// check for sufficient votes
	if successVotes > len(rf.peers)/2 {
		DPrintf("[%d] Becomes LEADER in term %d", rf.me, rf.currentTerm)
		rf.becomeLeader()
	}

}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = Leader
	rf.lastTimeCalled = time.Now()
	DPrintf("[%d] Becomes Leader at %s in Term %d", rf.me, rf.lastTimeCalled, rf.currentTerm)
	args := RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log),
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := RequestAppendEntriesReply{
		Term:    0,
		Success: false,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestAppendEntries(i, &args, &reply)
	}
	go rf.tickerHeartBeat()
}

func (rf *Raft) tickerHeartBeat() {
	for {
		if rf.state != Leader {
			return
		}
		time.Sleep(rf.waitTime / 3)
		DPrintf("[%d] Check for heartbeat required at %s in Term %d", rf.me, rf.lastTimeCalled, rf.currentTerm)
		if time.Since(rf.lastTimeCalled) > rf.waitTime/3 {
			rf.mu.Lock()
			args := RequestAppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.log),
				PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			reply := RequestAppendEntriesReply{
				Term:    0,
				Success: false,
			}
			rf.lastTimeCalled = time.Now()
			rf.mu.Unlock()
			DPrintf("[%d] No halchal in a while, send heartbeats at %s in term %d", rf.me, rf.lastTimeCalled, rf.currentTerm)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendRequestAppendEntries(i, &args, &reply)
			}
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
	rf.state = Follower

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]Log, 10)
	rf.lastTimeCalled = time.Now()
	rf.waitTime = time.Duration(300+rand.Int63()%300) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	DPrintf("[%d] created", rf.me)
	go rf.ticker()

	return rf
}
