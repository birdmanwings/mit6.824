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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

// Begin: from here it's defined by myself
type NodeState uint8

const (
	// time.Duration() use nanosecond
	HeartbeatInterval    = time.Duration(120) * time.Millisecond
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
)

const (
	Follower  = NodeState(1)
	Candidate = NodeState(2)
	Leader    = NodeState(3)
)

// Prepare for NodeState to use printf
func (s NodeState) String() string {
	switch {
	case s == Follower:
		return "Follower"
	case s == Candidate:
		return "Candidate"
	case s == Leader:
		return "Leader"
	}
	return "Unknown"
}

// Prepare for raft to use printf
func (rf Raft) String() string {
	return fmt.Sprintf("[node(%d), state(%v), term(%d), votedFor(%d)]",
		rf.me, rf.state, rf.currentTerm, rf.votedFor)
}

// convert raft peer's state and deal with the condition of each state
func (rf *Raft) convertTo(s NodeState) {
	// if it converts to itself ,return
	if s == rf.state {
		return
	}
	// record the convert error
	_, _ = DPrintf("Term %d: Server %d convert from %v to %v\n", rf.currentTerm, rf.me, rf.state, s)
	rf.state = s
	switch s {
	case Follower:
		rf.heartbeatTimer.Stop() // convert to the follower , so stop to send the heartbeat and stop the heartbeatTimer
		//rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper)) // reset the electionTimer
		rf.votedFor = -1
	case Candidate:
		rf.startElection()
	case Leader:
		rf.electionTimer.Stop() // because convert to leader ,so don't need to elect
		rf.broadcastHeartbeat()
		rf.heartbeatTimer.Reset(HeartbeatInterval) // Reset the heartbeatTimer after send the heartbeat rpc
	}
}

// Get the random time between lower and upper
func randTimeDuration(lower, upper time.Duration) time.Duration {
	num := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}

// start the election in two conditions that
// one is heartbeat timeout that follower convert to the candidate
// another is the candidate elects timeout and restart a new election.
func (rf *Raft) startElection() {
	rf.currentTerm += 1 // term + 1

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me} // prepare the rpc args
	var voteCount int32                                               // record the voted number

	rf.votedFor = rf.me            // candidate vote for itself
	atomic.AddInt32(&voteCount, 1) // atomic add 1

	// whenever a new election start , reset the raft peer's electionTimer
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	for i := range rf.peers {
		// candidate will vote for itself
		if i == rf.me {
			continue
		}
		// use goroutine to send the RequestVote RPC to each raft peer except it self
		// (because it will takes some time to receive the reply)
		go func(server int) {
			var reply RequestVoteReply // record each reply from other peer
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock() // need to lock rf when deal with the reply

				// if VoteGranted is true , it's prove candidate's term is larger than follower
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&voteCount, 1)
					// if get the major vote , candidate convert to leader
					if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
						rf.convertTo(Leader)
					}
				} else {
					// VoteGranted is false, that has two conditions that
					// one is it has voted to another candidate
					// the other is follower's term is larger than the candidate's
					// so the candidate will update it's term and convert to the follower
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
					}
				}
				rf.mu.Unlock()
			} else {
				// send RequestVote failed,record the error
				_, _ = DPrintf("%v send RequestVote RPC to %v failed", rf, server)
			}
		}(i)
	}

}

// Leader send the heartbeat to each follower
func (rf *Raft) broadcastHeartbeat() {
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}

	for i := range rf.peers {
		// skip itself
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock() // need to lock rf when deal with the reply
				// if the reply's term is larger than the Leader's term
				// leader update its term and convert to the follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.convertTo(Follower)
				}
				rf.mu.Unlock()
			} else {
				// record the heartbeat error
				_, _ = DPrintf("%v send the heartbeat to %d failed", rf, server)
			}
		}(i)
	}
}

// End

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	// 2A
	currentTerm    int
	votedFor       int
	electionTimer  *time.Timer // election timeout
	heartbeatTimer *time.Timer // heartbeat timeout
	state          NodeState   // each node state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Lock the current rf
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term        int // 2A
	CandidateId int // 2A
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // 2A
	VoteGranted bool // 2A if the candidate get this vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// when candidate's term is smaller , or its term equal to candidate and has voted for another candidate
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1) {
		//_, _ = DPrintf("%v didn't vote for node at term %d\n", rf, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	reply.VoteGranted = true
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	rf.convertTo(Follower)
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

// Begin: from here it's defined by myself
// In the lab2A we just need to implement the heartbeat and heartbeat is also implemented
// by using AppendEntries rpc that just contains an empty log.

// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term     int // 2A
	LeaderId int // 2A
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // 2A
	Success bool // 2A
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the heartbeat term is smaller
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Success = true
	rf.currentTerm = args.Term
	// when follower receive the heartbeat
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	// it aims at the candidate, it proves that leader is still alive, stop the election and convert to follower
	rf.convertTo(Follower)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// End

//
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
	rf.currentTerm = 0 // initialization is 0
	rf.votedFor = -1   // not to vote
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	rf.state = Follower // init is follower

	// deal with the things about the timer
	go func(node *Raft) {
		for {
			select {
			// election timeout
			case <-rf.electionTimer.C:
				rf.mu.Lock() // add lock
				if rf.state == Follower {
					rf.convertTo(Candidate)
				} else {
					rf.startElection()
				}
				rf.mu.Unlock() // delete lock

			// deal with when will the leader send the AppendEntries RPC (within the heartbeatTimer)
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.broadcastHeartbeat()
					rf.heartbeatTimer.Reset(HeartbeatInterval) // reset the timer to prepare for next heartbeat
				}
				rf.mu.Unlock()
			}
		}
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
