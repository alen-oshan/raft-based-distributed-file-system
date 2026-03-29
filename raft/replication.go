package raft

import "fmt"

func (n *RaftNode) sendHeartbeat() {
	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: n.commitIndex,
	}

	if len(n.log) > 0 {
		args.PrevLogIndex = len(n.log) - 1
		args.PrevLogTerm = n.log[len(n.log)-1].Term
	}

	for _, peerID := range n.peers {
		n.Send(Message{
			Type: MessageTypeAppendEntries,
			From: n.id,
			To:   peerID,
			Term: n.currentTerm,
			Data: args,
		})
	}
}

func (n *RaftNode) handleAppendEntries(msg Message) {
	args, ok := msg.Data.(AppendEntriesArgs)
	if !ok {
		fmt.Printf("%s received invalid AppendEntries payload from %s\n", n.id, msg.From)
		return
	}

	reply := AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	// Reset election timer on any AppendEntries from a leader/candidate.
	n.resetElectionTimer()

	// Reply false if term < currentTerm.
	if args.Term < n.currentTerm {
		response := Message{
			Type: MessageTypeAppendEntriesReply,
			From: n.id,
			To:   msg.From,
			Term: n.currentTerm,
			Data: reply,
		}
		n.Send(response)
		return
	}

	// Update term if needed.
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = "follower"
		n.votedFor = ""
		reply.Term = n.currentTerm
	}

	// Check log matching property.
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(n.log) {
			response := Message{
				Type: MessageTypeAppendEntriesReply,
				From: n.id,
				To:   msg.From,
				Term: n.currentTerm,
				Data: reply,
			}
			n.Send(response)
			return
		}

		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			response := Message{
				Type: MessageTypeAppendEntriesReply,
				From: n.id,
				To:   msg.From,
				Term: n.currentTerm,
				Data: reply,
			}
			n.Send(response)
			return
		}
	}

	// Success path: entries application is placeholder for now.
	reply.Success = true

	if args.LeaderCommit > n.commitIndex {
		lastLogIndex := len(n.log) - 1
		if args.LeaderCommit < lastLogIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastLogIndex
		}
	}

	response := Message{
		Type: MessageTypeAppendEntriesReply,
		From: n.id,
		To:   msg.From,
		Term: n.currentTerm,
		Data: reply,
	}

	// fmt.Printf("%s handling AppendEntries from %s: term=%d entries=%d success=%t\n", n.id, args.LeaderID, args.Term, len(args.Entries), reply.Success)
	n.Send(response)
}

func (n *RaftNode) handleAppendEntriesReply(msg Message) {
	// reply, ok := msg.Data.(AppendEntriesReply)
	// if !ok {
	// 	fmt.Printf("%s received invalid AppendEntriesReply payload from %s\n", n.id, msg.From)
	// 	return
	// }

	// fmt.Printf("%s handling AppendEntriesReply from %s: success=%t term=%d\n", n.id, msg.From, reply.Success, reply.Term)
}
