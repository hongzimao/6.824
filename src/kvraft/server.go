package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Request string  // "Put", "Append", "Get"
	Key     string  
	Value   string  // set to "" for Get request
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvdb    map[string]string
	rfidx   int 
}
func (kv *RaftKV) ApplyDb() {
	for{
		applymsg := <- kv.applyCh
		op := applymsg.Command.(Op)
		kv.rfidx = applymsg.Index

		if op.Request == "Put" {
			kv.kvdb[op.Key] = op.Value
		} else if op.Request == "Append" {
			kv.kvdb[op.Key] += op.Value
		} else if op.Request == "Get" {
			// dummy
		} 
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	op := Op{Request: "Get", Key: args.Key, Value: ""}
	
	cmtidx, _, isLeader := kv.rf.Start(op)

	reply.WrongLeader = false
	for{ // wait to store in raft log
		if !isLeader {
			reply.WrongLeader = true
			break
		} else if kv.rfidx >= cmtidx {
			reply.Value = kv.kvdb[args.Key]
			break // log already
		}
		time.Sleep( 50 * time.Millisecond) // appendEntries timeout
		_, isLeader = kv.rf.GetState()
	}

	kv.mu.Unlock()
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	op := Op{Request: args.Op, Key: args.Key, Value: args.Value}

	cmtidx, _, isLeader := kv.rf.Start(op)

	reply.WrongLeader = false
	for{ // wait to store in raft log
		if !isLeader {
			reply.WrongLeader = true
			break
		} else if kv.rfidx >= cmtidx {
			break // log already
		}
		time.Sleep( 50 * time.Millisecond) // appendEntries timeout
		_, isLeader = kv.rf.GetState()
	}
	
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvdb = make(map[string]string)

	go kv.ApplyDb()

	return kv
}
