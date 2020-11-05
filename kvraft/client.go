package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

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
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{Key: key}
	ok := false
	for reply.Value == "" {
		DPrintf("Retrying for all")
		for i := range ck.servers {
			reply := GetReply{}
			ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok {
				DPrintf("got reply %+v", reply)
				if reply.Err != "" {
					DPrintf("%v", reply.Err)
					continue
				}
				break
			} else {
				DPrintf("failed rpc")
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok || (reply.Err != "") {
		DPrintf("Error fetching value for key %v - %+v", key, reply.Err)
	} else {
		DPrintf("Fetched value %v for key %v", reply.Value, key)
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	done := false
	for !done {
		for i := range ck.servers {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			DPrintf("%+v", reply)
			if ok {
				if reply.Err != "" {
					DPrintf("%v", reply.Err)
					continue
				}
				done = true
				break
			}
		}
	}
	DPrintf("Added KV pair %v:%v", key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
