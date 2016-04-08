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
	"bytes"
	"encoding/gob"
	// "fmt"
	)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

const appendEntriesTimeout = 50
const requestVoteTimeoutMin = 150
const requestVoteTimeoutMax = 300

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   
	Snapshot    []byte 
}

type Log struct {
	Command interface{}
	Term int
	Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	elecTimer int64 // the start point of election timeout

	currentTerm int 
	isLeader bool 
	voteFor int
	voteTerm int // which term the voteFor is

	Logs []Log
	applyCh chan ApplyMsg

	commitIndex int
	lastApplied int

	// for leaders, re-initialize after election
	nextIndex []int
	matchIndex []int

	// snapshots
	lastIncludedIndex int
	lastIncludedTerm int

	// close goroutine
	killIt chan bool
}

// --------------------------------------------------------------------
// Ancillary functions 
// --------------------------------------------------------------------

// min of two numbers 
func minOfTwo(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// max of two numbers
func maxOfTwo(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
// random number in a range
func randIntRange(min, max int) int{
	// rand.Seed(time.Now().UnixNano())
	return rand.Intn(max - min) + min
}

func lastLog(Logs []Log) Log {
	return Logs[len(Logs) - 1]
}

func (rf *Raft) backToFollower() { // has lock already
	if rf.isLeader {
			rf.isLeader = false // back to follower
			go rf.ElectionTimeout()
		}
}

func (rf *Raft) becomesLeader() { // has lock already
	rf.isLeader = true
	// reinitialize nextIndex
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i ++ {
		rf.nextIndex[i] = lastLog(rf.Logs).Index + 1 
	}
	// reinitialize matchIndex
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i ++ {
		rf.matchIndex[i] = 0
	}
	go rf.broadcastAppendEntries()
}

func (rf *Raft) updateCommitIndex() { // has lock already
	if rf.isLeader {
		for n := lastLog(rf.Logs).Index; n >= maxOfTwo(rf.commitIndex, rf.lastIncludedIndex) ; n -- {
			majorityMatch := false
			majorityCount := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					majorityCount += 1
				} else {
					if rf.matchIndex[i] >= n && rf.Logs[n - rf.Logs[0].Index].Term == rf.currentTerm {
						majorityCount += 1
					}
				}
				if majorityCount * 2 > len(rf.peers) {
					majorityMatch = true
					break
				}
			}
			if majorityMatch {
				rf.commitIndex = n
				break
			}
		}
	}
}

func (rf *Raft) applyStateMachine() { // has lock already
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1

		if rf.lastApplied > rf.lastIncludedIndex { // restart need to skip snapshot
			var applyMsg ApplyMsg
			applyMsg.Index = rf.lastApplied
			applyMsg.Command = rf.Logs[rf.lastApplied - rf.Logs[0].Index].Command

			rf.applyCh <- applyMsg
		}
	}
}

func (rf *Raft) previousTermIdx(PrevLogIndex int) int { // has lock already
	termToSkip := rf.Logs[PrevLogIndex - rf.Logs[0].Index].Term
	for i := PrevLogIndex - rf.Logs[0].Index - 1; i > 0; i -- {
		if rf.Logs[i].Term != termToSkip {
			return rf.Logs[i].Index
		}
	}
	return 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.Logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.Logs)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

//
// update snapshot sent from upper server
//
func (rf *Raft) SaveSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.lastIncludedIndex {
		// already in the snapshot
		return 
	}

	rf.persister.SaveSnapshot(snapshot)

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = rf.Logs[lastIncludedIndex - rf.Logs[0].Index].Term
	rf.Logs = rf.Logs[lastIncludedIndex - rf.Logs[0].Index : ]

	rf.persist()
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}
// --------------------------------------------------------------------
// Structs
// --------------------------------------------------------------------
//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int // start from 1
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	Ok bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextIdxToSend int
	Ok bool
}

type InstallSnapshotArgs struct {
	Term int 
	LeaderId int 
	LastIncludedIndex int 
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

// --------------------------------------------------------------------
// RPC handlers
// --------------------------------------------------------------------
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	} 

	if args.Term > rf.currentTerm {
		rf.backToFollower()
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term 
	reply.VoteGranted = false

	if (rf.voteTerm < args.Term) || 
	   ( rf.voteTerm == args.Term && rf.voteFor == args.CandidateId) {
		if (lastLog(rf.Logs).Term < args.LastLogTerm) ||
		   (lastLog(rf.Logs).Term == args.LastLogTerm && lastLog(rf.Logs).Index <= args.LastLogIndex) {
			rf.voteFor = args.CandidateId
			rf.voteTerm = args.Term
			reply.VoteGranted = true

			rf.elecTimer = time.Now().UnixNano() // reset timer
		} 
	}	

	rf.persist()

	rf.mu.Unlock()
}

func (rf *Raft) ReceiveAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply){

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.backToFollower()
	}

	reply.Term = rf.currentTerm
	if lastLog(rf.Logs).Index < args.PrevLogIndex { // leader has longer log
	   	reply.Success = false
	   	reply.NextIdxToSend = lastLog(rf.Logs).Index + 1
	} else if args.PrevLogIndex < rf.lastIncludedIndex { // in snapshot 
		reply.Success = true
		// things in snapshot is guaranteed to be committed
		// can roll back to the leader's latest nextIndex
	} else if rf.Logs[args.PrevLogIndex - rf.Logs[0].Index].Term != args.PrevLogTerm { // logs don't match
		reply.Success = false
		reply.NextIdxToSend = rf.previousTermIdx(args.PrevLogIndex) + 1
		// previousTermIdx will be 0 if hitting the snapshot
	} else if args.PrevLogIndex == 0 { // reach empty 
		reply.Success = true
		rf.Logs = append(rf.Logs[0:1], args.Entries...)
	} else {
		reply.Success = true
		rf.Logs = rf.Logs[:args.PrevLogIndex - rf.Logs[0].Index + 1] // remove all unmatched
		rf.Logs = append(rf.Logs, args.Entries...)
	}

	if reply.Success && 
	   args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = minOfTwo(args.LeaderCommit, lastLog(rf.Logs).Index)
	}

	rf.applyStateMachine()

	rf.persist()
	
	rf.elecTimer = time.Now().UnixNano() // reset timer

	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.backToFollower()
	}

	rf.persister.SaveSnapshot(args.Data)

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if lastLog(rf.Logs).Index > args.LastIncludedIndex &&
	   args.LastIncludedIndex >= rf.Logs[0].Index &&
	   args.LastIncludedTerm == (rf.Logs[args.LastIncludedIndex - rf.Logs[0].Index].Term) {
	   	
	   	rf.Logs = rf.Logs[ args.LastIncludedIndex - rf.Logs[0].Index : ]

	} else {
		
		rf.Logs = append([]Log{}, Log{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm})
	}

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	applyMsg := ApplyMsg{UseSnapshot: true, Snapshot:args.Data}

	rf.applyCh <- applyMsg

	rf.persist()
}

// --------------------------------------------------------------------
// RPC calls
// --------------------------------------------------------------------
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
// returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveAppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// --------------------------------------------------------------------
// Main functions 
// --------------------------------------------------------------------

func (rf *Raft) broadcastAppendEntries() {
	for {
		select {
			case <- rf.killIt:
				return
			default:

				time.Sleep( appendEntriesTimeout * time.Millisecond) 

				rf.mu.Lock()

				if !rf.isLeader {
					rf.mu.Unlock()			
					break
				}

				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me { // RPC other servers

						if rf.nextIndex[i] <= rf.lastIncludedIndex {
							// nextIndex is in snapshot, apply InstallSnapshotRPC

							args := InstallSnapshotArgs{Term: rf.currentTerm, 
														LeaderId: rf.me, 
														LastIncludedIndex: rf.lastIncludedIndex, 
														LastIncludedTerm: rf.lastIncludedTerm, 
														Data: rf.persister.ReadSnapshot()}

							go func(j int, args InstallSnapshotArgs) {
								reply := &InstallSnapshotReply{}
								ok := rf.sendInstallSnapshot(j, args, reply)

								if ok {
									rf.mu.Lock()
									defer rf.mu.Unlock()

									if reply.Term > rf.currentTerm {
										rf.currentTerm = reply.Term
										rf.backToFollower()
										rf.persist()
										return 
									} else {
										rf.nextIndex[j] = lastLog(rf.Logs).Index + 1 
									}

								}
							}(i, args)

						} else {

							var args AppendEntriesArgs
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.LeaderCommit = rf.commitIndex

							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.PrevLogTerm = rf.Logs[args.PrevLogIndex - rf.Logs[0].Index].Term

							if lastLog(rf.Logs).Index < rf.nextIndex[i] { // heartbeat
								args.Entries = []Log{}
							} else { // user command
								args.Entries = rf.Logs[rf.nextIndex[i] - rf.Logs[0].Index : ]
							}

							go func(j int, args AppendEntriesArgs) {
								reply := &AppendEntriesReply{}
								reply.Ok = rf.sendAppendEntries(j, args, reply)

								if reply.Ok {
									rf.mu.Lock()
									if reply.Term > rf.currentTerm { // someone has higher term
										rf.currentTerm = reply.Term // adapt to larger term
										rf.backToFollower()
										rf.persist()
										rf.mu.Unlock()
										return
									} else {
										if rf.currentTerm == args.Term { // no reordering of net pkt
											if reply.Success { 
												logLenSent := args.PrevLogIndex + len(args.Entries)
												rf.nextIndex[j] = logLenSent + 1 
												rf.matchIndex[j] = logLenSent
											} else { // reply unsuccessful
												// rf.nextIndex[j] -= 1 
												rf.nextIndex[j] = reply.NextIdxToSend
												// will retry in the next AppendEntries 
											}
										}
									}
									rf.mu.Unlock()
								} 
							}(i, args)
						}
					}
				}
				rf.updateCommitIndex()
				rf.persist()
				rf.applyStateMachine()
				rf.mu.Unlock()
			}
	}
}

func (rf *Raft) ElectionTimeout() {
	for {
		select {
			case <- rf.killIt:
				return
			default:
				timeout := randIntRange(requestVoteTimeoutMin, requestVoteTimeoutMax) // 150 ~ 300 ms
				time.Sleep(time.Duration(timeout) * time.Millisecond)

				rf.mu.Lock()

				if rf.isLeader{
					rf.mu.Unlock()
					break
				}

				if (time.Now().UnixNano() - rf.elecTimer) >= int64(timeout * 1e6) {	
					rf.currentTerm += 1 // change to candidate, term +1

					rf.elecTimer = time.Now().UnixNano() // reset timer
					rf.voteFor = rf.me // vote for itself
					rf.voteTerm = rf.currentTerm // because it votes for itself

					var args RequestVoteArgs
					args.Term = rf.currentTerm
					args.CandidateId = rf.me
					args.LastLogIndex = lastLog(rf.Logs).Index
					args.LastLogTerm = lastLog(rf.Logs).Term

					reqVoteChann := make (chan *RequestVoteReply, len(rf.peers)-1) // all other servers
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me { // RPC other servers
							go func(j int) {
								reply := &RequestVoteReply{}
								reply.Ok = rf.sendRequestVote(j, args, reply)
								reqVoteChann <- reply // reqVote channel in 
							}(i)
						}
					}

					go func() {
						// count votes
						voteCount := 1 // always vote for itself
						stillCandidate := true

						for i := 0; i < len(rf.peers)-1 ; i++ { // all other servers
							reply := <- reqVoteChann // reqVote channel out

							if reply.Ok {
								rf.mu.Lock()
								
								if rf.currentTerm > args.Term { // new election begins
									stillCandidate = false
									rf.mu.Unlock()
									break
								}

								if reply.Term > rf.currentTerm {
									rf.currentTerm = reply.Term // adapt to larger term
									rf.persist()
									stillCandidate = false
									rf.mu.Unlock()
									break
								}

								if reply.VoteGranted {
									voteCount += 1
								}

								if stillCandidate && (2 * voteCount) > len(rf.peers) {
									rf.becomesLeader()
									rf.mu.Unlock()
									break
								}
								rf.mu.Unlock()
							}
						} 
					}() 
					rf.persist()
				}
				rf.mu.Unlock()
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
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if !rf.isLeader {
		return -1, -1, false
	}

	log := Log{Command: command, Term: rf.currentTerm, Index: lastLog(rf.Logs).Index + 1}

	rf.Logs = append(rf.Logs, log)

	index := lastLog(rf.Logs).Index
	term := rf.currentTerm
	isLeader := rf.isLeader

	rf.persist()

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
	close(rf.killIt)
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

	// initialization from scratch

	rf.elecTimer = time.Now().UnixNano()

	rf.mu.Lock()

	rf.currentTerm = -1
	rf.voteFor = -1 // nil
	rf.voteTerm = -1
	rf.isLeader = false

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	rf.Logs = append([]Log{}, Log{Index: 0, Term: -1})

	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i ++ {
		rf.nextIndex[i] = lastLog(rf.Logs).Index + 1 
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i ++ {
		rf.matchIndex[i] = 0
	}

	rf.killIt = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Unlock()

	go rf.ElectionTimeout()

	return rf
}
