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
	"sync"
	"labrpc"
	"time"
	"math/rand"
	// "fmt"
	)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2 ; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	command interface{}
	term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	elecTimer int64 // the start point of election timeout

	currentTerm int 
	isLeader bool // LY: may be changed to states: 0 for follower, 1 for candidate, 2 for leader
	voteFor int

	Logs []Log

	termLock sync.Mutex
	voteLock sync.Mutex
}

// --------------------------------------------------------------------
// Ancillary functions 
// --------------------------------------------------------------------

// random number in a range
func randIntRange(min, max int) int{
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max - min) + min
}

// out of range return initial value
func endLogTerm(Logs []Log, initTerm int) int {
	if len(Logs)-1 < 0 {
			return initTerm
		} else {
			return Logs[len(Logs)-1].term
		}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.

	term = rf.currentTerm
	isLeader = rf.isLeader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// --------------------------------------------------------------------
// Structs
// --------------------------------------------------------------------
//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {

}

type AppendEntriesReply struct {

}

// --------------------------------------------------------------------
// RPC handlers
// --------------------------------------------------------------------
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	earlyReturn := false
	rf.termLock.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		earlyReturn = true
	} 
	rf.termLock.Unlock()
	if earlyReturn { return }

	rf.currentTerm = args.Term
	reply.Term = args.Term 
	reply.VoteGranted = false

	rf.voteLock.Lock()
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId { // -1 for nil
		if endLogTerm(rf.Logs, -1) <= args.Term {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
		}
	}	
	rf.voteLock.Unlock()
}

func (rf *Raft) ReceiveHeartbeat(args AppendEntriesArgs, reply *AppendEntriesReply){
	rf.elecTimer = time.Now().UnixNano()
}

// --------------------------------------------------------------------
// RPC calls
// --------------------------------------------------------------------
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveHeartbeat", args, reply)
	return ok
}

// --------------------------------------------------------------------
// Main functions 
// --------------------------------------------------------------------

func (rf *Raft) broadcastHeartbeat() {
	for {
		time.Sleep( 10 * time.Millisecond) 
		for i := 0; i < len(rf.peers); i++ {
			go func(j int) {
				var args AppendEntriesArgs
				reply := &AppendEntriesReply{}
				rf.sendHeartbeat(j, args, reply)
				}(i)
		}
	}
}

func (rf *Raft) ElectionTimeout() {
	for {
		
		timeout := randIntRange(150, 300) // 150 ~ 300 ms
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		if (time.Now().UnixNano() - rf.elecTimer) > int64(timeout * 1e6) {

			rf.currentTerm ++ // change to candidate, term +1

			var args RequestVoteArgs
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.Logs)-1
			args.LastLogTerm = endLogTerm(rf.Logs, -1)

			reqVoteChann := make (chan *RequestVoteReply)
			for i := 0; i < len(rf.peers); i++ {
				go func(j int) {
					reply := &RequestVoteReply{}
					rf.sendRequestVote(j, args, reply)
					reqVoteChann <- reply
				}(i)
			}

			// count votes
			voteCount := 0
			stillCandidate := true
			for i := 0; i < len(rf.peers); i++ {
				reply := <- reqVoteChann

				rf.termLock.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					stillCandidate = false
				}
				rf.termLock.Unlock()

				if reply.VoteGranted {
					voteCount ++
				}
			} // barrier

			if stillCandidate && (2 * voteCount) > len(rf.peers) {
				rf.isLeader = true
				go rf.broadcastHeartbeat()
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail.
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
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.elecTimer = time.Now().UnixNano()

	rf.currentTerm = -1
	rf.voteFor = -1 // nil
	rf.isLeader = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ElectionTimeout()

	return rf
}
