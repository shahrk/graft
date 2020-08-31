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
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 600
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
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
			args.LastLogIndex >= len(rf.log)-1) {
		if rf.votedFor == nil {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
		} else if args.Term > rf.currentTerm {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
		}
	} else {
		DPrintf("[%d] denied vote to [%d] with outdated log", rf.me, args.CandidateId)
	}
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
			rf.currentTerm++
			rf.state = Candidate
			rf.votedFor = &rf.me
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()
			rf.conductElection(&args)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeoutMin+rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)) * time.Millisecond
}

func (rf *Raft) conductElection(args *RequestVoteArgs) {
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
		rf.mu.Unlock()
		rf.initializeLeader()
		rf.sendPeriodicHeartbeats()
	} else {
		DPrintf("[%d] lost the election for term %d or already discovered another leader for a greater term", rf.me, args.Term)
		rf.mu.Unlock()
	}
}
