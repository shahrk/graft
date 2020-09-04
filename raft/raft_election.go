package raft

import (
	"math/rand"
	"sync"
	"time"
)

//
// This will have all the raft functionality
// related to elections
//

const (
	ElectionTimeoutMin = 500
	ElectionTimeoutMax = 800
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm > rf.Log[len(rf.Log)-1].Term ||
		(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term &&
			args.LastLogIndex >= len(rf.Log)-1) {
		if rf.VotedFor == nil {
			reply.VoteGranted = true
			rf.VotedFor = &args.CandidateId
		} else if args.Term > rf.CurrentTerm {
			reply.VoteGranted = true
			rf.VotedFor = &args.CandidateId
		} else {
			DPrintf("[%d] already voted for %d for term %d", rf.me, *rf.VotedFor, rf.CurrentTerm)
		}
	} else {
		DPrintf("[%d] denied vote to [%d] with outdated log", rf.me, args.CandidateId)
	}
	rf.CurrentTerm = args.Term
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, done chan bool) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	done <- ok
}

func (rf *Raft) startElectionTicker() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader && time.Since(rf.lastHeartbeat) > getElectionTimeout() {
			rf.CurrentTerm++
			rf.state = Candidate
			rf.VotedFor = &rf.me
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.Log) - 1,
				LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
			}
			rf.mu.Unlock()
			rf.conductElection(&args)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeoutMin+rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)) * time.Millisecond
}

func (rf *Raft) conductElection(args *RequestVoteArgs) {
	// assume leader heartbeat to avoid instant reelection in case we loose
	rf.lastHeartbeat = time.Now()
	DPrintf("[%d] conducting elections for term %d", rf.me, args.Term)
	cond := sync.NewCond(&rf.mu)
	votes := 1
	replies := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer cond.Broadcast()
			reply := RequestVoteReply{}
			rpcStart := time.Now()
			done := make(chan bool)
			rpcTimeout := time.NewTimer(ElectionTimeoutMax * time.Millisecond)
			defer rpcTimeout.Stop()
			defer func() {
				rf.mu.Lock()
				replies++
				rf.mu.Unlock()
			}()
			go rf.sendRequestVote(i, args, &reply, done)
			select {
			case ok := <-done:
				if !ok {
					DPrintf("[%d] RequestVote RPC for term %d to peer [%d] failed - %v", rf.me, args.Term, i, time.Since(rpcStart))
					return
				}
				break
			case <-rpcTimeout.C:
				DPrintf("[%d] RequestVote RPC for term %d to peer [%d] timed out - %v", rf.me, args.Term, i, time.Since(rpcStart))
				return
			}
			defer rf.mu.Unlock()
			rf.mu.Lock()
			if reply.VoteGranted {
				DPrintf("[%d] received vote for term %d from peer [%d]", rf.me, args.Term, i)
				votes++
			} else {
				DPrintf("[%d] did NOT receive vote for term %d from peer [%d] with term %d", rf.me, args.Term, i, reply.Term)
			}
		}(i)
	}
	rf.mu.Lock()
	for votes <= len(rf.peers)/2 && (replies-votes) < len(rf.peers)/2 && replies < len(rf.peers) {
		cond.Wait()
	}
	if rf.state == Candidate && votes > len(rf.peers)/2 {
		DPrintf("[%d] won the election for term %d by getting %d votes out of %d", rf.me, args.Term, votes, len(rf.peers))
		rf.state = Leader
		rf.initializeLeader()
		rf.mu.Unlock()
		go rf.sendPeriodicHeartbeats()
	} else {
		DPrintf("[%d] lost the election for term %d or already discovered another leader for a greater term", rf.me, args.Term)
		rf.mu.Unlock()
	}
}
