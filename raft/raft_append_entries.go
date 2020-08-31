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
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[%d] received append entries from psuedo leader [%d]", rf.me, args.LeaderId)
		reply.Success = false
	} else if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] received append entries with mismatching log from [%d]", rf.me, args.LeaderId)
		reply.Success = false
		if len(rf.log)-1 < args.PrevLogIndex {
			reply.Xlen = len(rf.log)
		} else {
			reply.Xterm = rf.log[args.PrevLogIndex].Term
			i := args.PrevLogIndex - 1
			for rf.log[i].Term == reply.Xterm {
				i--
			}
			reply.Xindex = i
		}
	} else {
		if rf.state != Follower {
			DPrintf("[%d] becoming follower on getting valid heartbeat from [%d]", rf.me, args.LeaderId)
		}
		rf.lastHeartbeat = time.Now()
		rf.currentTerm = args.Term
		rf.state = Follower
		startIndex := args.PrevLogIndex + 1
		for i := startIndex; i-startIndex < len(args.Entries); i++ {
			if len(rf.log)-1 < i {
				rf.log = append(rf.log, args.Entries[i-startIndex:]...)
				break
			} else if rf.log[i].Term != args.Entries[i-startIndex].Term {
				rf.log = rf.log[:i]
				i--
			}
		}
		reply.Success = true
		// rf.commitEntries(args.LeaderCommit)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return
}
