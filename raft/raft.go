package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command interface{}
	Term    int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

var States = [...]string{
	"Follower",
	"Candidate",
	"Leader",
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	lastHeartbeat time.Time           // time at which this peer received the last valid heartbeat
	state         State               // Whether the leader is a Leader, Follower or Candidate
	VotedFor      *int                // Index of peer this server has votedFor (null if none). Persisted
	CurrentTerm   int                 // Latest Term server has seen. Persisted
	commitIndex   int                 // Index of highest log entry known to be committed
	applyCh       chan ApplyMsg       // Channel to which raft instance will send committed msgs
	lastApplied   int                 // Index of last applied log entry
	Log           []*Log              // log entries. Persisted
	nextIndex     []int               // For each peer, the log index to be sent out
	matchIndex    []int               // For each peer, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	defer rf.mu.Unlock()
	rf.mu.Lock()
	DPrintf("[%d] Term: %d, State: %d, Heartbeat: %v", rf.me, rf.CurrentTerm, rf.state, rf.lastHeartbeat)

	term := rf.CurrentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) printLogs() {
	logs := ""
	for i := range rf.Log {
		logs += fmt.Sprintf("%+v ", rf.Log[i].Command)
	}
	DPrintf(logs)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		(rf.VotedFor != nil && e.Encode(rf.VotedFor) != nil) ||
		e.Encode(rf.Log) != nil {
		DPrintf("[%d] error while encoding state", rf.me)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		// bootstrap without any state
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor *int
	var log []*Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("[%d] Error while reading persisted state", rf.me)
	} else {
		if votedFor != nil {
			DPrintf("[%d] read currentTerm - %d, votedFor - %d", rf.me, currentTerm, *votedFor)
		} else {
			DPrintf("[%d] read currentTerm - %d, votedFor - Nil", rf.me, votedFor)
		}
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.printLogs()
	}
}

func (rf *Raft) beginConsensus(index int) {
	cond := sync.NewCond(&rf.mu)
	replies := 1
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer cond.Broadcast()
			for {
				rf.mu.Lock()
				if rf.state != Leader || rf.nextIndex[i] > index+1 {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{
					LeaderId:     rf.me,
					Term:         currentTerm,
					LeaderCommit: commitIndex,
					Entries:      rf.Log[rf.nextIndex[i] : index+1],
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.Log[rf.nextIndex[i]-1].Term,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				} else if reply.Success {
					defer rf.mu.Unlock()
					rf.mu.Lock()
					switch rf.Log[index].Command.(type) {
					default:
						DPrintf("[%d] Append Entries on server [%d] successful", rf.me, i)
					case int:
						for k := range args.Entries {
							DPrintf("[%d] sent command %+v to [%d]", rf.me, args.Entries[k].Command, i)
						}
					}
					replies++
					if rf.nextIndex[i] < index+1 {
						rf.nextIndex[i] = index + 1
					}
					return
				} else if reply.Term > currentTerm {
					defer rf.mu.Unlock()
					rf.mu.Lock()
					DPrintf("[%d] saw term %d on server [%d] which was greater than the term %d at sending", rf.me, reply.Term, i, currentTerm)
					if rf.CurrentTerm < reply.Term {
						DPrintf("[%d] switching to follower", rf.me)
						rf.CurrentTerm = reply.Term
						rf.state = Follower
						rf.lastHeartbeat = time.Now()
					}
					return
				} else {
					rf.mu.Lock()
					if reply.Xterm != -1 {
						// mismatching entry
						rf.nextIndex[i] = reply.Xindex
						for k := range rf.Log {
							if rf.Log[len(rf.Log)-k-1].Term == reply.Xterm {
								rf.nextIndex[i] = len(rf.Log) - k - 1
								break
							}
						}
					} else {
						// missing entry
						rf.nextIndex[i] = reply.Xlen
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}
	defer rf.mu.Unlock()
	rf.mu.Lock()
	for rf.state == Leader && replies <= len(rf.peers)/2 {
		cond.Wait()
	}
	if rf.state == Leader && currentTerm == rf.CurrentTerm {
		rf.commitEntries(index)
		rf.persist()
	} else {
		DPrintf("[%d] impeached before reaching consensus for index-%d in term-%d", rf.me, index, currentTerm)
	}
}

func (rf *Raft) commitEntries(index int) {
	prevCommitIndex := rf.commitIndex
	var updatedCommitIndex int
	if len(rf.Log)-1 < index {
		updatedCommitIndex = len(rf.Log) - 1
	} else {
		updatedCommitIndex = index
	}
	if updatedCommitIndex >= prevCommitIndex+1 {
		DPrintf("[%d] committing entries from %d to %d", rf.me, prevCommitIndex+1, updatedCommitIndex)
		rf.printLogs()
		rf.commitIndex = updatedCommitIndex
	} else {
		return
	}
	for i := prevCommitIndex + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{Command: rf.Log[i].Command, CommandIndex: i, CommandValid: true}
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	index := -1
	term := -1
	isLeader := rf.state == Leader
	if isLeader {
		index = len(rf.Log)
		switch command.(type) {
		default:
			DPrintf("[%d] processing command from client at index %d", rf.me, index)
		case int:
			DPrintf("[%d] processing command from client - %+v at index %d", rf.me, command, index)
		}
		term = rf.CurrentTerm
		logEntry := Log{Command: command, Term: term}
		rf.Log = append(rf.Log, &logEntry)
		go rf.beginConsensus(index)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) initializeLeader() {
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.Log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) sendPeriodicHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.sendHeartbeats(rf.CurrentTerm, rf.commitIndex)
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeats(currentTerm int, commitIndex int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: commitIndex,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.Log[rf.nextIndex[i]-1].Term,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				return
			}
			if !reply.Success {
				defer rf.mu.Unlock()
				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					DPrintf("[%d] saw term %d on server [%d] which was greater than current term %d", rf.me, reply.Term, i, rf.CurrentTerm)
					rf.CurrentTerm = reply.Term
					rf.state = Follower
					rf.lastHeartbeat = time.Now()
					return
				}
			}
		}(i)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.applyCh = applyCh
	log := Log{Term: 0}
	rf.Log = append(rf.Log, &log)
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	go rf.startElectionTicker()

	return rf
}
