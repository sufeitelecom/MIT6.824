package shardmaster

import "raft"
import "labrpc"
import "sync"
import (
	"labgob"
	"log"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const Timeout = time.Second * 3

type WaitingOp struct {
	waitchan chan bool
	op       *Op
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs      []Config             // indexed by config num
	waitingforOp map[int][]*WaitingOp //异步等待相应操作完成
	dupremove    map[int64]int64      // 去重
}

type Op struct {
	// Your data here.
	Type    string
	Command interface{}
}

func (sm *ShardMaster) ExecOp(op Op) Err {
	index, _, isleader := sm.rf.Start(op)
	if isleader == false {
		DPrintf("This server is not leader , server number is %v", sm.me)
		return ErrNotLeader
	}

	waiting := make(chan bool, 1)

	sm.mu.Lock()
	sm.waitingforOp[index] = append(sm.waitingforOp[index], &WaitingOp{waitchan: waiting, op: &op})
	sm.mu.Unlock()

	timer := time.NewTimer(Timeout)
	var ok bool
	var res Err
	select {
	case ok = <-waiting:
		if ok {
			res = OK
		} else {
			res = ErrNotLeader
		}
	case <-timer.C:
		DPrintf("KV execute timeout!")
		res = ErrTimeout
	}
	sm.mu.Lock()
	delete(sm.waitingforOp, index)
	sm.mu.Unlock()
	return res
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
