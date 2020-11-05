package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	defer DPrintf("%+v", reply.Err)
	op := Op{Key: args.Key, Type: "get"}
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "Not the leader"
		return
	} else {
		reply.Err = ""
	}
	for msg := range kv.applyCh {
		DPrintf("got msg %+v on channel. expecting at index %d", msg, idx)
		if entry, ok := msg.Command.(Op); ok {
			if entry.Type == "put" {
				kv.store[entry.Key] = entry.Value
			} else if entry.Type == "append" {
				kv.store[entry.Key] += entry.Value
			}
			if msg.CommandIndex == idx {
				if entry == op {
					reply.Value = kv.store[args.Key]
					DPrintf(reply.Value)
					DPrintf("responding")
					return
				} else {
					reply.Err = "Mismatching command found in log entry"
					return
				}
			} else if msg.CommandIndex > idx {
				reply.Err = "Found higher index log entry"
				return
			}
		} else {
			reply.Err = "Mismatching command found in log entry"
			return
		}
	}
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Key: args.Key, Type: args.Op, Value: args.Value}
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "Not the leader"
		DPrintf("responding with %+v", reply)
		return
	}
	for msg := range kv.applyCh {
		DPrintf("got msg %+v on channel. expecting at index %d", msg, idx)
		if entry, ok := msg.Command.(Op); ok {
			if entry.Type == "put" {
				kv.store[entry.Key] = entry.Value
			} else if entry.Type == "append" {
				kv.store[entry.Key] += entry.Value
			}
			if msg.CommandIndex == idx {
				if entry != op {
					reply.Err = "Mismatching command found in log entry"
				} else {
					reply.Err = ""
				}
				DPrintf("responding with %+v", reply)
				return
			} else if msg.CommandIndex > idx {
				reply.Err = "Found higher index log entry"
				return
			}
		} else {
			reply.Err = "Mismatching command found in log entry"
			return
		}
	}
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) scout() {
	for {
		select {
		case msg, open := <-kv.applyCh:
			if !open {
				DPrintf("Channel closed")
				return
			}
			if op, ok := msg.Command.(Op); ok {
				if op.Type == "put" {
					kv.store[op.Key] = op.Value
				} else if op.Type == "append" {
					kv.store[op.Key] += op.Value
				}
			} else {
				DPrintf("Unkown command found in log entry")
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// go kv.scout()

	time.Sleep(100 * time.Millisecond)
	return kv
}
