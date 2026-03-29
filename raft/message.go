package raft

const (
	MessageTypeRequestVote        = "RequestVote"
	MessageTypeRequestVoteReply   = "RequestVoteReply"
	MessageTypeAppendEntries      = "AppendEntries"
	MessageTypeAppendEntriesReply = "AppendEntriesReply"
)

type Message struct {
	From string
	To   string
	Term int
	Data any
	Type string
}

type RequestVoteArgs struct {
	Term         int    // candidate's term
	CandidateID  string // candidate requesting vote
	LastLogIndex int    // index of candidate's last log entry
	LastLogTerm  int    // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     string     // so followers can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type LogEntry struct {
	Index   int
	Term    int
	Command any // the client command
}
