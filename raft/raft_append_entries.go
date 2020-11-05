package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Xterm   int
	Xindex  int
	Xlen    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		DPrintf("[%d] received append entries from psuedo leader [%d]", rf.me, args.LeaderId)
		reply.Success = false
	} else if len(rf.Log)-1 < args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] received append entries with mismatching log from [%d]", rf.me, args.LeaderId)
		if len(rf.Log)-1 < args.PrevLogIndex {
			DPrintf("[%d] last index %d less than prev index in args %d", rf.me, len(rf.Log)-1, args.PrevLogIndex)
		} else {
			DPrintf("[%d] term of last index %d not same as prev log term in args %d", rf.me, rf.Log[args.PrevLogIndex].Term, args.PrevLogTerm)
		}
		reply.Success = false
		reply.Xterm = -1
		reply.Xlen = len(rf.Log)
		if len(rf.Log)-1 >= args.PrevLogIndex {
			reply.Xterm = rf.Log[args.PrevLogIndex].Term
			i := args.PrevLogIndex - 1
			for rf.Log[i].Term == reply.Xterm {
				i--
			}
			reply.Xindex = i + 1
		}
	} else {
		if rf.state != Follower {
			DPrintf("[%d] becoming follower on getting valid heartbeat from [%d]", rf.me, args.LeaderId)
		}
		rf.CurrentTerm = args.Term
		rf.state = Follower
		startIndex := args.PrevLogIndex + 1
		for i := startIndex; i-startIndex < len(args.Entries); i++ {
			if len(rf.Log)-1 < i {
				rf.Log = append(rf.Log, args.Entries[i-startIndex:]...)
				break
			} else if rf.Log[i].Term != args.Entries[i-startIndex].Term {
				rf.Log = rf.Log[:i]
				i--
			}
		}
		reply.Success = true
		if len(args.Entries) != 0 || args.PrevLogIndex >= args.LeaderCommit {
			rf.commitEntries(args.LeaderCommit)
		} else {
			rf.commitEntries(args.PrevLogIndex)
		}
		rf.lastHeartbeat = time.Now()
		rf.LeaderId = args.LeaderId
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return
}
