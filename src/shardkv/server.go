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

const PollConfigTimeout = 100
const PollShardsTimeout = 100

// --------------------------------------------------------------------
// Op's
// --------------------------------------------------------------------

type ShardConfigOp struct {
	Config  shardmaster.Config
}

type PullShardOp struct {
	KvDb    map[string]string
	RfIdx   int 
	CltSqn  map[int64]int64  
	SV 		ShardVer
}

type Op struct {
	Request string  // "Put", "Append", "Get"
	Key     string  
	Value   string  // set to "" for Get request
	CltId   int64   // client unique identifier
	SeqNum  int64
}

// --------------------------------------------------------------------
// ShardKV struct
// --------------------------------------------------------------------

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft

	applyCh      chan raft.ApplyMsg

	mck          *shardmaster.Clerk
	config       shardmaster.Config

	shardsVerNum []int            	   // version number for each shard

	pullMap      map[ShardVer]ServerValid 

	make_end     func(string) *labrpc.ClientEnd
	
	gid          int
	
	masters      []*labrpc.ClientEnd

	maxraftstate int 				   // snapshot if log grows this big

	kvdb    map[string]string
	rfidx   int 
	cltsqn  map[int64]int64  		   // sequence number log for each client

	chanMapMu  sync.Mutex
	resChanMap map [int] chan ReplyRes // communication from applyDb to clients

	pcTimer *time.Timer  			   // timer for polling the configuration
	psTimer *time.Timer  			   // timer for sending PullShards requests

	killIt chan bool   				   // close goroutine
}

// --------------------------------------------------------------------
// Background Functions
// --------------------------------------------------------------------

func (kv *ShardKV) createResChan(cmtidx int) {
	kv.chanMapMu.Lock()
	if kv.resChanMap[cmtidx] == nil {
		kv.resChanMap[cmtidx] = make(chan ReplyRes, ResChanSize)
	}
	kv.chanMapMu.Unlock()
}

func (kv *ShardKV) PollConfig() {
	for {
		select{
			case <- kv.killIt:
				return
			case <- kv.pcTimer.C:

				kv.mu.Lock()
				nextConfigIdx := kv.config.Num + 1  // next config
				kv.mu.Unlock()

				newConfig := kv.mck.Query(nextConfigIdx) 

				if newConfig.Num == nextConfigIdx {  // got new config
					op := ShardConfigOp{Config: newConfig}
					kv.rf.Start(op)
				}

				kv.pcTimer.Reset(time.Duration(PollConfigTimeout)* time.Millisecond)
		}
	}
}

func (kv *ShardKV) PollShards() {
	for {
		select{
			case <- kv.killIt:
				return
			case <- kv.psTimer.C:

				kv.mu.Lock()

				for shardVer, serversValid := range kv.pullMap{

					if serversValid.Valid {  // needs shard from others	

						for si := 0; si < len(serversValid.Servers); si++ {

							srv := kv.make_end(serversValid.Servers[si])

							args := PullShardArgs{Shard:shardVer.Shard, VerNum:shardVer.VerNum}
							var reply PullShardReply
							
							ok := srv.Call("ShardKV.PullShard", &args, &reply)

							if ok && reply.Success {  // got the reply from intended shard group

								// if kv.pullMap[shardVer].Valid  // all lock here
								op := PullShardOp{KvDb: reply.KvDb, RfIdx: reply.RfIdx, CltSqn: reply.CltSqn, SV: shardVer}
								kv.rf.Start(op)

							}
						}
					}
				}

				kv.mu.Unlock()

				kv.psTimer.Reset(time.Duration(PollShardsTimeout)* time.Millisecond)
		}
	}
}

// --------------------------------------------------------------------
// ApplyCh from Raft
// --------------------------------------------------------------------

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
					kv.pullMap = make(map[ShardVer]ServerValid)
					d.Decode(&kv.kvdb)
					d.Decode(&kv.rfidx)
					d.Decode(&kv.cltsqn)
					d.Decode(&kv.config)
					d.Decode(&kv.shardsVerNum)
					d.Decode(&kv.pullMap)

				} else {

					kv.rfidx = applymsg.Index

					kv.createResChan(applymsg.Index)

					kv.chanMapMu.Lock()
					resCh := kv.resChanMap[applymsg.Index]
					kv.chanMapMu.Unlock()

					switch op := applymsg.Command.(type) { 

						// ------------------- update config op -------------------

						case ShardConfigOp:  // update config

							if op.Config.Num > kv.config.Num { 

								okToUpdate := true
								for s := 0; s < shardmaster.NShards; s ++ {  
									g := kv.config.Shards[s]
									if g == kv.gid {  // in charge of this shard in current config
										if kv.shardsVerNum[s] != kv.config.Num{  // config during transit
											okToUpdate = false
											break
										}
									}
								}
								if okToUpdate {
									for s := 0; s < shardmaster.NShards; s ++ {  
										g := op.Config.Shards[s]

										if g == kv.gid { // in charge of this shard in new config
											
											if kv.shardsVerNum[s] == kv.config.Num {  // in previous config

												kv.shardsVerNum[s] = op.Config.Num  // no need to pull
												
											} else {
												
												shardVer := ShardVer{Shard:s, VerNum:kv.config.Num}
												oldServer := kv.config.Groups[kv.config.Shards[s]]
												serversValid := ServerValid{Servers: oldServer, Valid: true}
												kv.pullMap[shardVer] = serversValid
											}
										}
									}

									kv.config = op.Config 
								}

							}

						// ------------------- pull shard op -------------------

						case PullShardOp:
							
							if kv.pullMap[op.SV].Valid {

								kv.rfidx = op.RfIdx

								kv.kvdb = make(map[string]string)
								kv.cltsqn = make(map[int64]int64)
								
								for k, v := range op.KvDb {
									kv.kvdb[k] = v
								}

								for k, v := range op.CltSqn {
									kv.cltsqn[k] = v
								}

								kv.pullMap[op.SV] = ServerValid{Servers: kv.pullMap[op.SV].Servers, 
																Valid: false}  // invalid pullMap

								kv.shardsVerNum[op.SV.Shard] = kv.config.Num  // update version number

							}

						// ------------------- client request op -------------------

						case Op:  // user request

							// Check shard config
							shard := key2shard(op.Key)
							gid := kv.config.Shards[shard]

							if kv.gid == gid {

								if kv.shardsVerNum[shard] != kv.config.Num {  // during transition
									// TODO: disable it here?
									resCh <- ReplyRes{InTransit: true}	

								} else {

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

									resCh <- ReplyRes{Value:kv.kvdb[op.Key], InOp:op, WrongGroup: false, InTransit: false}	

								}
							
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

// --------------------------------------------------------------------
// Snapshots
// --------------------------------------------------------------------

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
	e.Encode(kv.config)
	e.Encode(kv.shardsVerNum)
	e.Encode(kv.pullMap)
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
	d.Decode(&kv.config)
	d.Decode(&kv.shardsVerNum)
	d.Decode(&kv.pullMap)
}

// --------------------------------------------------------------------
// Shard Group RPC Functions
// --------------------------------------------------------------------

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {

	kv.mu.Lock()

	if kv.shardsVerNum[args.Shard] == args.VerNum {

		reply.Success = true

		reply.RfIdx = kv.rfidx

		reply.KvDb = make(map[string]string)
		reply.CltSqn = make(map[int64]int64)
		
		for k, v := range kv.kvdb {
			reply.KvDb[k] = v
		}

		for k, v := range kv.cltsqn {
			reply.CltSqn[k] = v
		}

	} else {

		reply.Success = false
	}

	kv.mu.Unlock()
}

// --------------------------------------------------------------------
// Response Functions
// --------------------------------------------------------------------

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
			} else if res.InTransit {
				reply.Err = ErrInTransit
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
			} else if res.InTransit {
				reply.Err = ErrInTransit
			} else if reflect.DeepEqual(op, res.InOp) {
				reply.Err = OK
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
	gob.Register(PullShardOp{})
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

	kv.config = kv.mck.Query(0)  // get the initial config

	kv.shardsVerNum = make([]int, shardmaster.NShards)

	kv.pullMap = make(map[ShardVer]ServerValid)

	kv.ReadSnapshot(persister.ReadSnapshot())

	kv.pcTimer = time.NewTimer(time.Duration(PollConfigTimeout)* time.Millisecond)
	kv.psTimer = time.NewTimer(time.Duration(PollShardsTimeout)* time.Millisecond)

	go kv.PollConfig()
	go kv.PollShards()

	go kv.ApplyDb()

	return kv
}
