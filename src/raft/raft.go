package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "labgob"

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

//定义使用到的常量
const (
	STOPPED      = "stopped"
	INITIALIZED  = "initialized"
	FOLLOWER     = "follower"
	CANDIDATE    = "candidate"
	LEADER       = "leader"
	SNAPSHOTTING = "snapshotting"
)

//相关事件常量
const (
	HeartbeatCycle  = time.Millisecond * 50
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

//状态机日志项结构
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//服务器粗腰持久化到硬盘的数据
	currentTerm int
	voteFor     int
	log         []LogEntry

	//所有服务器都应该实时维护的状态信息
	commitIndex int //已知提交日志中的最大索引id值，小于等于该index的日志都应该提交，初始化为0，单调递增
	lastApplied int //已经应用到状态机中的日志的最大索引id值，该index之前的都已经应用到服务器，初始化为0，单调递增

	//作为leader服务器时，应该维护的状态信息
	nextIndex  []int //对每个follower，下一个应该要发送的日志index信息
	matchIndex []int //已知复制到各follower的最大的日志index

	grantedvotescount int //投票计数

	state   string //节点状态
	applyCh chan ApplyMsg

	timer *time.Timer //节点计时器，主要选举超时以及变成候选人超时
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cur int
	var vot int
	var log []LogEntry
	if d.Decode(&cur) != nil || d.Decode(&vot) != nil || d.Decode(&log) != nil {
		panic("read persist error")
	} else {
		rf.currentTerm = cur
		rf.voteFor = vot
		rf.log = log
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidateid  int
	Lastlogindex int
	Lastlogterm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) handleVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.persist()
		rf.resetTimer()
		return
	}

	if rf.state == CANDIDATE && reply.VoteGranted {
		rf.grantedvotescount += 1
		if rf.grantedvotescount >= (len(rf.peers)/2 + 1) {
			//fmt.Println("服务器",rf.me,"成为主！！！！！任期号",rf.currentTerm)
			rf.state = LEADER
			//更新作为leader需要维护的状态
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = -1
			}
			rf.resetTimer()
		}
		return
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//任期小于本身直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	may_grant := true

	if len(rf.log) > 0 {
		if rf.log[len(rf.log)-1].Term > args.Lastlogterm ||
			(rf.log[len(rf.log)-1].Term == args.Lastlogterm && len(rf.log)-1 > args.Lastlogindex) {
			may_grant = false
		}
	}

	//任期等于本身，则看是否日志最新
	if rf.currentTerm == args.Term {
		if rf.voteFor == -1 && may_grant {
			rf.voteFor = args.Candidateid
			rf.persist()
		}
		reply.Term = args.Term
		reply.VoteGranted = (rf.voteFor == args.Candidateid)
		return
	}

	//任期大于自身，自身变成追随者
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteFor = -1
		if may_grant {
			rf.voteFor = args.Candidateid
			rf.persist()
		}

		reply.Term = args.Term
		reply.VoteGranted = (rf.voteFor == args.Candidateid)
		rf.resetTimer()
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term         int
	Leaderid     int
	Prevlogindex int
	Prevlogterm  int
	Entries      []LogEntry
	Leadercommit int
}

type AppendEntryReply struct {
	Term        int
	Success     bool
	Commitindex int
}

func (rf *Raft) handleAppendEntries(server int, rep AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	if rep.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.voteFor = -1
		rf.currentTerm = rep.Term
		rf.persist()
		rf.resetTimer()
		return
	}
	if rep.Success == false {
		rf.nextIndex[server] = rep.Commitindex + 1
		rf.sendAppendToFollower()
	} else {
		rf.nextIndex[server] = rep.Commitindex + 1
		rf.matchIndex[server] = rep.Commitindex
		reply_count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				reply_count++
			}
		}
		if reply_count >= (len(rf.peers)/2+1) && rf.commitIndex < rf.matchIndex[server] &&
			rf.log[rf.matchIndex[server]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[server]
			fmt.Println("服务器", rf.me, "提交日志", rf.matchIndex[server])
			go rf.commitlogs()
		}
	}
	rf.persist()
}

func (rf *Raft) commitlogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.log)-1 {
		rf.commitIndex = len(rf.log) - 1
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		fmt.Println("服务器", rf.me, "应用日志", rf.commitIndex)
		rf.applyCh <- ApplyMsg{
			CommandIndex: i,
			Command:      rf.log[i].Command,
			CommandValid: false,
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntries(args AppendEntryArgs, rep *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果日志小于当前任期号，直接拒绝
	if args.Term < rf.currentTerm {
		rep.Term = rf.currentTerm
		rep.Commitindex = -1
		rep.Success = false
		return
	} else {
		rf.state = FOLLOWER
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rep.Term = rf.currentTerm

		//如果先前的日志不匹配也需要拒绝
		if args.Prevlogindex >= 0 && (args.Prevlogindex > (len(rf.log)-1) || rf.log[args.Prevlogindex].Term != args.Prevlogterm) {
			rep.Commitindex = len(rf.log) - 1
			if rep.Commitindex > args.Prevlogindex {
				rep.Commitindex = args.Prevlogindex
			}
			for rep.Commitindex >= 0 {
				if args.Prevlogterm == rf.log[rep.Commitindex].Term {
					break
				}
				rep.Commitindex--
			}
			rep.Success = false
		} else if args.Entries != nil {
			// 前项日志匹配了，添加日志
			rf.log = rf.log[:args.Prevlogindex+1]
			rf.log = append(rf.log, args.Entries...)
			if len(rf.log)-1 >= args.Leadercommit {
				rf.commitIndex = args.Leadercommit
				go rf.commitlogs()
			}
			rep.Commitindex = len(rf.log) - 1
			rep.Success = true
		} else {
			//心跳
			if len(rf.log)-1 >= args.Leadercommit {
				rf.commitIndex = args.Leadercommit
				//fmt.Println("从服务器提交日志",rf.commitIndex)
				go rf.commitlogs()
			}
			rep.Commitindex = args.Prevlogindex
			rep.Success = true
		}
	}
	rf.persist()
	rf.resetTimer()
}

func (rf *Raft) SendAppendEntryToFollower(server int, args AppendEntryArgs, rep *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, rep)
	return ok
}

func (rf *Raft) sendAppendToFollower() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var entryargs AppendEntryArgs
		entryargs.Term = rf.currentTerm
		entryargs.Leaderid = rf.me
		entryargs.Prevlogindex = rf.nextIndex[i] - 1
		if entryargs.Prevlogindex >= 0 {
			entryargs.Prevlogterm = rf.log[entryargs.Prevlogindex].Term
		}
		if rf.nextIndex[i] < len(rf.log) {
			entryargs.Entries = rf.log[rf.nextIndex[i]:]
		}
		entryargs.Leadercommit = rf.commitIndex

		go func(server int, args AppendEntryArgs) {
			var reply AppendEntryReply
			fmt.Println("主服务器", rf.me, "发起应用日志", args, "到服务器", server)
			ok := rf.SendAppendEntryToFollower(server, args, &reply)
			if ok {
				fmt.Println("主服务器", rf.me, "收到到服务器", server, "响应", reply)
				rf.handleAppendEntries(server, reply)
			}
		}(i, entryargs)
	}
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nlog := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	if rf.state != LEADER {
		isLeader = (rf.state == LEADER)
		return index, term, isLeader
	}

	isLeader = (rf.state == LEADER)
	rf.log = append(rf.log, nlog)
	term = rf.currentTerm
	index = len(rf.log) - 1
	rf.persist()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//重置定时器函数
//leader 50ms
//follower 150ms - 300ms
func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * 2000)
		go func() {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}
	newtime := HeartbeatCycle
	if rf.state != LEADER {
		newtime = time.Millisecond * time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime))
	}
	rf.timer.Reset(newtime)
}

//定时器处理函数
//根据是否为主进行不同操作
func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		rf.sendAppendToFollower()
	} else {
		//切换位候选人，更新相关信息准备参与选举
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.grantedvotescount = 1
		rf.persist()
		//构建选举参数
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			Candidateid:  rf.voteFor,
			Lastlogindex: len(rf.log) - 1,
		}
		if len(rf.log) > 0 {
			args.Lastlogterm = rf.log[len(rf.log)-1].Term
		}
		//对于每台机器发送选举
		for num := 0; num < len(rf.peers); num++ {
			if num == rf.me {
				continue
			}
			go func(server int, arg RequestVoteArgs) {
				var votereply RequestVoteReply
				//fmt.Println("服务器：",rf.me,"发起投票","，任期号",rf.currentTerm)
				ok := rf.sendRequestVote(server, arg, &votereply)
				if ok {
					//fmt.Println("服务器：",rf.me,"任期号",rf.currentTerm,"收到服务器：",server,"投票",votereply)
					rf.handleVoteResult(votereply)
				}
			}(num, args)
		}
	}
	rf.resetTimer()
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

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = FOLLOWER
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist()
	rf.resetTimer()

	return rf
}
