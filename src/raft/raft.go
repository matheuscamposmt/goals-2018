package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

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
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) persist() {
	// Your code here (2C).
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.currentTerm <= args.Term {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
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
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.state = Follower
	rf.votedFor = -1

	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(heartbeatTimeout)

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
	rf.electionTimer.Reset(randomElectionTimeout())
	select {
	case <-rf.electionTimer.C:
		rf.mu.Lock()
		rf.state = Candidate
		rf.mu.Unlock()
	}
}

func (rf *Raft) runCandidate() {
	rf.startElection()
	select {
	case <-rf.electionTimer.C:
		rf.mu.Lock()
		rf.state = Candidate
		rf.mu.Unlock()
	}
}

func (rf *Raft) runLeader() {
	rf.sendHeartbeats()
	select {
	case <-rf.heartbeatTimer.C:
		rf.sendHeartbeats()
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()

	votes := 1
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				args := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				var reply RequestVoteReply
				if rf.sendRequestVote(i, args, &reply) && reply.VoteGranted {
					votes++
					if votes > len(rf.peers)/2 {
						rf.mu.Lock()
						rf.state = Leader
						rf.mu.Unlock()

					}
				}
			}(i)
		}
	}
	rf.electionTimer.Reset(randomElectionTimeout())
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHeartbeat(i)
		}
	}
	rf.heartbeatTimer.Reset(heartbeatTimeout)
}

// AppendEntriesArgs represents the arguments for an AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
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
		ok := rf.sendAppendEntries(server, &args, &reply)
		if ok {
			rf.handleAppendEntriesReply(server, &args, &reply)
		}
	}()
}

func (rf *Raft) AppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.electionTimer.Reset(randomElectionTimeout())

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	if args.PrevLogIndex > rf.getLastLogIndex() {
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}

	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		// Update commitIndex
		for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
			count := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
				rf.commitIndex = i
				go rf.applyLogEntries()
			}
		}
	} else {
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
	}

}

func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			Index:   rf.lastApplied,
			Command: rf.log[rf.lastApplied].Command,
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) > 0 {
		return len(rf.log) - 1
	}
	return 0
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
