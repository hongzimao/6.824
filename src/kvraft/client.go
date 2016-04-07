package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
// import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	id 	   int64
	seqnum int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.seqnum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.seqnum += 1
	args := &GetArgs{Key:key, CltId:ck.id, SeqNum:ck.seqnum}
	for {
		for _, server := range ck.servers {
			var reply GetReply
			ok := server.Call("RaftKV.Get", args, &reply)

			if ok && !reply.WrongLeader {
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqnum += 1
	args := &PutAppendArgs{Key:key, Value:value, Op:op, CltId:ck.id, SeqNum:ck.seqnum}
	for {
		for _, server := range ck.servers {
			var reply PutAppendReply
			ok := server.Call("RaftKV.PutAppend", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}	
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
