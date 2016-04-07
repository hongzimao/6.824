package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"
// import "fmt"

const LogLenCheckerTimeout = 50 
const ClientRPCTimeout = 50
const MaxRaftFactor = 0.8

const VOIDGID = 0


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	rfidx   int 
	cltsqn  map[int64]int64  // sequence number log for each client

	configs []Config // indexed by config num
}

func (sm *ShardMaster) CloneLastConfig() {
	var shards [NShards]int
	group := make(map[int][]string)

	for i := 0; i < NShards; i ++ {
		shards[i] = sm.LastConfig().Shards[i]
	}

	for GID, servers := range sm.LastConfig().Groups {
		group[GID] = servers
	}

	config := Config{Num: len(sm.configs), 
					 Shards: shards, 
					 Groups: group}

	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) LastConfig() Config {
	return sm.configs[len(sm.configs) - 1]
}

func (sm *ShardMaster) LoadBalance() {

	// bear with my brute force..

	maLoad := make(map[int]int)  // each GID load
	maIdx := make(map[int][]int) // shards on each GID

	maLoad[VOIDGID] = 0
	maIdx[VOIDGID] = []int{}

	for GID, _ := range sm.LastConfig().Groups {
		maLoad[GID] = 0
		maIdx[GID] = []int{}
	}

	for i := 0; i < NShards; i++ {
		maLoad[sm.LastConfig().Shards[i]] += 1
		maIdx[sm.LastConfig().Shards[i]] = append(maIdx[sm.LastConfig().Shards[i]], i)
	}

	for {

		maxGID := getMaxGID(maLoad)
		minGID := getMinGID(maLoad)
		
		if maxGID != VOIDGID && abs(maLoad[maxGID] - maLoad[minGID]) <= 1 {
			break
		}

		shard := maIdx[maxGID][0]

		sm.configs[len(sm.configs)-1].Shards[shard] = minGID

		maLoad[maxGID] -= 1
		maLoad[minGID] += 1

		maIdx[maxGID] = maIdx[maxGID][1:]
		maIdx[minGID] = append(maIdx[minGID], shard)
	}
}

func (sm *ShardMaster) InvalidGroups(GIDs []int){
	for i := 0; i < NShards; i++ {
		for j := range GIDs {
			if sm.LastConfig().Shards[i] == GIDs[j] {
				sm.configs[len(sm.configs)-1].Shards[i] = VOIDGID
				break		
			}
		}
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	} else {
		return n
	}
}

func getMaxGID(maLoad map[int]int) int {

	if maLoad[VOIDGID] > 0 {
		return VOIDGID
	}

	maxLoad := 0
	maxGID := 0
	for GID, load := range maLoad {
		if load > maxLoad {
			maxGID = GID
			maxLoad = load
		}
	}
	return maxGID
}

func getMinGID(maLoad map[int]int) int {
	minLoad := NShards
	minGID := 0
	for GID, load := range maLoad {
		if GID == VOIDGID {
			continue
		} else if load <= minLoad {
			minGID = GID
			minLoad = load
		}
	}
	return minGID
}

func (sm *ShardMaster) ApplyDb() {
	for{
		applymsg := <- sm.applyCh
		
		sm.mu.Lock()

		op := applymsg.Command.(Op)

		sm.rfidx = applymsg.Index

		if val, ok := sm.cltsqn[op.CltId]; !ok || op.SeqNum > val {

			sm.cltsqn[op.CltId] = op.SeqNum

			if op.Request == "Join" {

				sm.CloneLastConfig()
				for GID, servers := range op.Servers{
					sm.configs[len(sm.configs)-1].Groups[GID] = servers
				}
				sm.LoadBalance()

			} else if op.Request == "Leave" {

				sm.CloneLastConfig()
				for i := range op.GIDs {
					delete(sm.configs[len(sm.configs)-1].Groups, op.GIDs[i])	
				}
				sm.InvalidGroups(op.GIDs)
				sm.LoadBalance()

			} else if op.Request == "Move" {

				sm.CloneLastConfig()
				sm.configs[len(sm.configs)-1].Shards[op.Shard] = op.GID

			} else if op.Request == "Query" {
				// dummy
			}
			
		}

		sm.mu.Unlock()
	}
}

type Op struct {
	Request string  // "Join", "Leave", "Move", "Query"
	GIDs    []int     
	GID     int
	Servers map[int][]string
	Shard   int
	Num     int
	Value   string  
	CltId   int64   
	SeqNum  int64
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{Request: "Join", Servers: args.Servers, CltId:args.CltId, SeqNum: args.SeqNum}
	
	cmtidx, _, isLeader := sm.rf.Start(op)

	reply.WrongLeader = false
	for{ // wait to store in raft log
		if !isLeader {
			reply.WrongLeader = true
			break
		} else if sm.rfidx >= cmtidx {
			// in log already
			break 
		}
		time.Sleep( ClientRPCTimeout * time.Millisecond) // appendEntries timeout
		_, isLeader = sm.rf.GetState()
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{Request: "Leave", GIDs: args.GIDs, CltId:args.CltId, SeqNum: args.SeqNum}
	
	cmtidx, _, isLeader := sm.rf.Start(op)

	reply.WrongLeader = false
	for{ // wait to store in raft log
		if !isLeader {
			reply.WrongLeader = true
			break
		} else if sm.rfidx >= cmtidx {
			// in log already
			break 
		}
		time.Sleep( ClientRPCTimeout * time.Millisecond) // appendEntries timeout
		_, isLeader = sm.rf.GetState()
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Request: "Move", GID: args.GID, Shard: args.Shard, CltId:args.CltId, SeqNum: args.SeqNum}
	
	cmtidx, _, isLeader := sm.rf.Start(op)

	reply.WrongLeader = false
	for{ // wait to store in raft log
		if !isLeader {
			reply.WrongLeader = true
			break
		} else if sm.rfidx >= cmtidx {
			// in log already
			break 
		}
		time.Sleep( ClientRPCTimeout * time.Millisecond) // appendEntries timeout
		_, isLeader = sm.rf.GetState()
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Request: "Query", Num: args.Num, CltId:args.CltId, SeqNum: args.SeqNum}
	
	cmtidx, _, isLeader := sm.rf.Start(op)

	reply.WrongLeader = false
	for{ // wait to store in raft log
		if !isLeader {
			reply.WrongLeader = true
			break
		} else if sm.rfidx >= cmtidx {

			if args.Num == -1 || args.Num >= len(sm.configs) {
				// config always non-empty
				reply.Config = sm.LastConfig()
			} else {
				reply.Config = sm.configs[args.Num]
			}
			break
		}
		time.Sleep( ClientRPCTimeout * time.Millisecond) // appendEntries timeout
		_, isLeader = sm.rf.GetState()
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(JoinArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveArgs{})
	gob.Register(LeaveReply{})
	gob.Register(MoveArgs{})
	gob.Register(MoveReply{})
	gob.Register(QueryArgs{})
	gob.Register(QueryReply{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.rfidx = 0
	sm.cltsqn = make(map[int64]int64)

	go sm.ApplyDb()

	return sm
}
