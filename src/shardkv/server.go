package shardkv

import (
	"shardmaster"
	"labrpc"
	"raft"
	"sync"
	"encoding/gob"
	"time"
	"bytes"
	"reflect"
	// "fmt"
)

const MaxRaftFactor = 0.8

const ResChanSize = 1
const ResChanTimeout = 1000

const PollsConfigTimeout = 100

type ShardConfigOp struct {
	Request string  // "UpdateConfig"
	Config  shardmaster.Config
}

type Op struct {
	Request string  // "Put", "Append", "Get"
	Key     string  
	Value   string  // set to "" for Get request
	CltId   int64   // client unique identifier
	SeqNum  int64
}

type ReplyRes struct {
	Value   	 string 
	InOp    	 Op
	WrongGroup 	 bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft

	mck      *shardmaster.Clerk
	config   shardmaster.Config

	applyCh      chan raft.ApplyMsg

	make_end     func(string) *labrpc.ClientEnd
	
	gid          int
	
	masters      []*labrpc.ClientEnd

	maxraftstate int // snapshot if log grows this big

	kvdb    map[string]string
	rfidx   int 
	cltsqn  map[int64]int64  // sequence number log for each client

	chanMapMu  sync.Mutex
	resChanMap map [int] chan ReplyRes // communication from applyDb to clients

	pcTimer *time.Timer  // timer for polling the configuration

	// close goroutine
	killIt chan bool
}

func (kv *ShardKV) PollsConfig(){
	for {
		select{
			case <- kv.killIt:
				return
			case <- kv.pcTimer.C:

				nextConfigIdx := kv.config.Num + 1  // next config
				newConfig := kv.mck.Query(nextConfigIdx) 

				if newConfig.Num == nextConfigIdx {  // got new config
					op := ShardConfigOp{Request: "UpdateConfig", Config: newConfig}
					kv.rf.Start(op)
				}

				kv.pcTimer.Reset(time.Duration(PollsConfigTimeout)* time.Millisecond)
		}
	}
}

func (kv *ShardKV) ApplyDb() {
	for{
		select {
			case <- kv.killIt:
				return
			default:
				applymsg := <- kv.applyCh

				kv.mu.Lock()

				if applymsg.UseSnapshot {

					r := bytes.NewBuffer(applymsg.Snapshot)
					d := gob.NewDecoder(r)
					kv.kvdb = make(map[string]string)
					d.Decode(&kv.kvdb)
					d.Decode(&kv.rfidx)
					d.Decode(&kv.cltsqn)

				} else {

					kv.rfidx = applymsg.Index

					kv.createResChan(applymsg.Index)

					kv.chanMapMu.Lock()
					resCh := kv.resChanMap[applymsg.Index]
					kv.chanMapMu.Unlock()

					switch op := applymsg.Command.(type) { 

						case ShardConfigOp:  // update config

							if op.Config.Num > kv.config.Num { 

								kv.config = op.Config

							}

						case Op:  // user request

							// Check shard config
							shard := key2shard(op.Key)
							gid := kv.config.Shards[shard]

							if kv.gid == gid {

								if val, ok := kv.cltsqn[op.CltId]; !ok || op.SeqNum > val {

									kv.cltsqn[op.CltId] = op.SeqNum
									if op.Request == "Put" {
										kv.kvdb[op.Key] = op.Value
									} else if op.Request == "Append" {
										kv.kvdb[op.Key] += op.Value
									} else if op.Request == "Get" {
										// dummy
									}
								}

								select{
									case <- resCh:
										// flush the channel
									default:
										// no need to flush
								}

								resCh <- ReplyRes{Value:kv.kvdb[op.Key], InOp:op, WrongGroup: false}	
							
							} else {

								resCh <- ReplyRes{WrongGroup: true}	
							}
					
					}

				}

				kv.mu.Unlock()

				go kv.CheckSnapshot()
			}
	}
}

func (kv *ShardKV) CheckSnapshot() {
	if float64(kv.rf.GetStateSize()) / float64(kv.maxraftstate) > MaxRaftFactor {
		kv.SaveSnapshot()
	}
}

func (kv *ShardKV) SaveSnapshot() { 
	kv.mu.Lock()
	
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.kvdb)
	e.Encode(kv.rfidx)
	e.Encode(kv.cltsqn)
	data := w.Bytes()

	kvrfidx := kv.rfidx  // preserve this value outside the lock

	kv.mu.Unlock()  // has to unlock here, otherwise deadlock
	
	kv.rf.SaveSnapshot(data, kvrfidx)
}

func (kv *ShardKV) ReadSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.kvdb)
	d.Decode(&kv.rfidx)
	d.Decode(&kv.cltsqn)
}

func (kv *ShardKV) createResChan(cmtidx int) {
	kv.chanMapMu.Lock()
	if kv.resChanMap[cmtidx] == nil {
		kv.resChanMap[cmtidx] = make(chan ReplyRes, ResChanSize)
	}
	kv.chanMapMu.Unlock()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	
	op := Op{Request: "Get", Key: args.Key, Value: "", CltId:args.CltId, SeqNum: args.SeqNum}
	
	cmtidx, _, isLeader := kv.rf.Start(op)

	if !isLeader{
		reply.WrongLeader = true
		return
	}

	kv.createResChan(cmtidx)

	kv.chanMapMu.Lock()
	resCh := kv.resChanMap[cmtidx]
	kv.chanMapMu.Unlock()

	select{
		case res := <- resCh:
			if res.WrongGroup {
				reply.Err = ErrWrongGroup
			} else if reflect.DeepEqual(op, res.InOp) {
				reply.Value = res.Value
				reply.Err = OK
			} else{
				reply.WrongLeader = true
			}
		case <- time.After(ResChanTimeout * time.Millisecond): // RPC timeout
			reply.WrongLeader = true
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	
	op := Op{Request: args.Op, Key: args.Key, Value: args.Value, CltId:args.CltId, SeqNum: args.SeqNum}

	cmtidx, _, isLeader := kv.rf.Start(op)

	if !isLeader{
		reply.WrongLeader = true
		return
	}

	kv.createResChan(cmtidx)

	kv.chanMapMu.Lock()
	resCh := kv.resChanMap[cmtidx]
	kv.chanMapMu.Unlock()

	select{
		case res := <- resCh:
			if res.WrongGroup {
				reply.Err = ErrWrongGroup
			} else if reflect.DeepEqual(op, res.InOp) {
				reply.Err = OK
				// dummy
			} else{
				reply.WrongLeader = true
			}
		case <- time.After(ResChanTimeout * time.Millisecond): // RPC timeout
			reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.killIt)
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(ShardConfigOp{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvdb = make(map[string]string)
	kv.rfidx = 0
	kv.cltsqn = make(map[int64]int64)

	kv.resChanMap = make(map [int] chan ReplyRes)

	kv.killIt = make(chan bool)

	kv.ReadSnapshot(persister.ReadSnapshot())

	kv.pcTimer = time.NewTimer(time.Duration(PollsConfigTimeout)* time.Millisecond)

	kv.config = kv.mck.Query(0)  // get the initial config

	go kv.PollsConfig()

	go kv.ApplyDb()

	return kv
}
