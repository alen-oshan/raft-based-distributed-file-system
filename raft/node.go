package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	heartbeatInterval  = 150 * time.Millisecond
	minElectionTimeout = 300 * time.Millisecond
	maxElectionTimeout = 600 * time.Millisecond
)

type RaftNode struct {
	id          string
	peers       []string
	state       string
	currentTerm int
	log         []LogEntry
	votedFor    string
	voteCount   int

	// Deadlines drive role-specific periodic work in the main loop.
	electionDeadline  time.Time
	heartbeatDeadline time.Time
	rng               *rand.Rand
	votesReceived     map[string]bool

	// Volatile state
	commitIndex int
	lastApplied int

	inbox        chan Message            // Channel for receiving messages
	nodeChannels map[string]chan Message // Map of peerID -> their inbox channel

	stopChan   chan bool // Channel to signal node to stop
	pauseChan  chan bool // Channel to signal node to pause
	resumeChan chan bool // Channel to signal node to resume
}

func NewRaftNode(id string, peers []string, nodeChannels map[string]chan Message, inbox chan Message) *RaftNode {
	r := &RaftNode{
		id:          id,
		peers:       peers,
		state:       "follower",
		currentTerm: 0,
		votedFor:    "",
		voteCount:   0,

		inbox:        inbox,
		nodeChannels: nodeChannels,
	}

	seed := time.Now().UnixNano()
	for _, ch := range id {
		seed += int64(ch)
	}
	// Mix node ID into seed so concurrently started nodes still diverge.
	r.rng = rand.New(rand.NewSource(seed))
	r.resetElectionTimer()
	// Leaders send heartbeat on this cadence; followers use electionDeadline.
	r.heartbeatDeadline = time.Now().Add(heartbeatInterval)
	r.stopChan = make(chan bool, 1)
	r.pauseChan = make(chan bool, 1)

	return r
}

func (n *RaftNode) Run() {
	pausedLogged := false

	for {
		var timeout time.Duration

		// Role decides which timer controls the next wake-up.
		if n.state == "leader" {
			timeout = time.Until(n.heartbeatDeadline)
		} else {
			timeout = time.Until(n.electionDeadline)
		}

		if timeout < 0 {
			timeout = 0
		}

		select {
		case <-n.stopChan:
			fmt.Printf("%s stopped\n", n.id)
			return

		case <-n.pauseChan:
			if !pausedLogged {
				fmt.Printf("%s paused\n", n.id)
				pausedLogged = true
			}

		case msg := <-n.inbox:
			pausedLogged = false
			if n.state != "leader" {
				n.resetElectionTimer()
			}

			// Network events are handled before waiting for the next timeout tick.
			n.handleMessage(msg)

		case <-time.After(timeout):
			pausedLogged = false

			if n.state == "leader" {
				// On heartbeat timeout, leader should broadcast AppendEntries.
				// n.sendHeartbeat()
				n.heartbeatDeadline = time.Now().Add(heartbeatInterval)
			} else {
				// Followers/candidates begin a new election after silence from leader.
				fmt.Printf("%s election timeout in term %d; starting election\n", n.id, n.currentTerm)
				n.resetElectionTimer()
				// n.startElection()
			}
		}
	}
}

func (n *RaftNode) resetElectionTimer() {
	window := maxElectionTimeout - minElectionTimeout
	randPart := time.Duration(0)
	if window > 0 {
		randPart = time.Duration(n.rng.Int63n(int64(window)))
	}
	n.electionDeadline = time.Now().Add(minElectionTimeout + randPart)
}

func (n *RaftNode) Send(msg Message) {
	// In this simulation, "sending" means pushing directly into peer inbox.
	ch := n.nodeChannels[msg.To]
	ch <- msg
}

func (n *RaftNode) handleMessage(msg Message) {
	switch msg.Type {
	// case MessageTypeRequestVote:
	// 	n.handleRequestVote(msg)
	// case MessageTypeRequestVoteReply:
	// 	n.handleRequestVoteReply(msg)
	case MessageTypeAppendEntries:
		n.handleAppendEntries(msg)
	case MessageTypeAppendEntriesReply:
		n.handleAppendEntriesReply(msg)
	default:
		fmt.Printf("%s received unknown message type %q from %s\n", n.id, msg.Type, msg.From)
	}
}
