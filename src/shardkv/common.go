package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrInTransit  = "ErrInTransit"
)

type Err string

// ------------------------
// Basic Bundles
// ------------------------

type ShardVer struct {
	Shard   	 int
	VerNum  	 int
	ConfNum		 int
}

type ServerValid struct {
	Servers 	 []string
	Valid   	 bool
}

type ReplyRes struct {
	Value   	 string 
	InOp    	 Op
	WrongGroup 	 bool
	InTransit    bool
}

// ------------------------
// RPC's
// ------------------------

type PullShardArgs struct {
	Shard 	     int
	VerNum	     int
	ConfNum		 int
}

type PullShardReply struct {
	KvDb    	 map[string]string
	CltSqn  	 map[int64]int64  
	ShardVer 	 int
	Success      bool
}

type DeleteShardArgs struct {
	Shard 	     int
	VerNum	     int
	ConfNum		 int
}

type DeleteShardReply struct {
	Success 	 bool
}

// Put or Append
type PutAppendArgs struct {
	Key   		 string
	Value 		 string
	Op    		 string  // "Put" or "Append"
	CltId  		 int64
	SeqNum 		 int64
}

type PutAppendReply struct {
	WrongLeader  bool
	Err          Err
}

type GetArgs struct {
	Key 	     string
	CltId  	 	 int64
	SeqNum 		 int64
}

type GetReply struct {
	WrongLeader  bool
	Err          Err
	Value        string
}
