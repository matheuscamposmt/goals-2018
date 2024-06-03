package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

var logger = log.New(os.Stdout, "Raft: ", log.LstdFlags|log.Lmicroseconds)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	electionTimeout  = 300 * time.Millisecond
	heartbeatTimeout = 100 * time.Millisecond
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	mu             sync.Mutex
	peers          []*labrpc.ClientEnd
	persister      *Persister
	me             int
	state          State
	currentTerm    int
	votedFor       int
	log            []LogEntry
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	applyCh        chan ApplyMsg
	votes_received int
	elected        chan bool
	rejected       chan bool
	vote           chan bool
	heartbeat      chan bool
	dead           int32
}
type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

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

func isUpToDate(lastLogIndex, lastLogTerm, candidateLastLogIndex, candidateLastLogTerm int) bool {
	if lastLogTerm != candidateLastLogTerm {
		return candidateLastLogTerm > lastLogTerm
	}
	return candidateLastLogIndex >= lastLogIndex
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm

	updated := isUpToDate(rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm)
	if updated && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.vote <- true
		rf.state = Follower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})

	// persist log entry here if needed

	return index, term, true
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dead = 1

}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.heartbeat = make(chan bool, 100)
	rf.elected = make(chan bool, 100)
	rf.rejected = make(chan bool, 100)
	rf.vote = make(chan bool, 100)

	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) run() {

	for {
		switch rf.state {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) runFollower() {
	select {
	case <-rf.heartbeat:
	case <-rf.vote:
	case <-time.After(randomElectionTimeout()):
		rf.state = Candidate

	}
}

func (rf *Raft) runCandidate() {
	go rf.startElection()

	select {
	case <-time.After(randomElectionTimeout()):
	case <-rf.heartbeat:
		rf.state = Follower
	case <-rf.elected:
		rf.mu.Lock()
		rf.state = Leader
		rf.mu.Unlock()
	}
}

func (rf *Raft) runLeader() {
	rf.sendHeartbeats()
	time.Sleep(heartbeatTimeout)
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votes_received = 1
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {
			go func(i int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(i, &args, &reply) {

					if rf.state != Candidate || rf.currentTerm != args.Term {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
					}

					if reply.VoteGranted {
						rf.votes_received++
						if rf.state == Candidate && rf.votes_received > (len(rf.peers)/2) {
							rf.state = Leader
							rf.elected <- true
						}
					}
				}

			}(i)
		}
	}

}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			go rf.sendHeartbeat(i)
		}
	}
}

// AppendEntriesArgs represents the arguments for an AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply represents the reply for an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getLastLogIndex(),
		PrevLogTerm:  rf.getLastLogTerm(),
		Entries:      []LogEntry{}, // Heartbeats não têm entradas de log
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		var reply AppendEntriesReply
		ok := rf.sendEntries(server, &args, &reply)
		if ok {
			rf.handleAppendEntriesReply(server, &args, &reply)
		}
	}()

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.heartbeat <- true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	reply.Term = args.Term

	// log consistency check
	if len(rf.log) > 0 && (args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persist()

	reply.Success = true
}

func (rf *Raft) sendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return 0
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
