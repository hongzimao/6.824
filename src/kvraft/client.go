package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
// import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key}
	for {
		for _, server := range ck.servers {
			reply := &GetReply{}
			ok := server.Call("RaftKV.Get", args, reply)

			if ok && !reply.WrongLeader {
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{Key: key, Value: value, Op: op}
	for {
		for _, server := range ck.servers {
			reply := &PutAppendReply{}
			ok := server.Call("RaftKV.PutAppend", args, reply)
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
