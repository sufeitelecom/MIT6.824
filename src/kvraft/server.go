package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

const (
	OpGet    = 0
	OpPut    = 1
	OpAppend = 2
)

const Timeout = time.Second * 3

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     int
	Key      string
	Value    string
	Clientid int64
	Opid     int64
}

type WaitingOp struct {
	waitchan chan bool
	op       *Op
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data         map[string]string    //最终kv数据存储位置
	waitingforOp map[int][]*WaitingOp //异步等待相应操作完成
	Opseq        map[int64]int64      //主要是去重，防止多次进行相同操作

}

func (kv *KVServer) Opexec(op Op) bool {
	index, _, isleader := kv.rf.Start(op)
	if isleader == false {
		DPrintf("this is not leader!!")
		return false
	}

	waiting := make(chan bool, 1)

	kv.mu.Lock()
	kv.waitingforOp[index] = append(kv.waitingforOp[index], &WaitingOp{op: &op, waitchan: waiting})
	kv.mu.Unlock()

	timer := time.NewTimer(Timeout)
	var ok bool
	select {
	case ok = <-waiting:
	case <-timer.C:
		DPrintf("KV execute timeout!")
		ok = false
	}
	delete(kv.waitingforOp, index)
	return ok

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Type = OpGet
	op.Clientid = args.Clientid
	op.Opid = args.Opid
	op.Key = args.Key

	ok := kv.Opexec(op)
	if ok == false {
		reply.WrongLeader = true
		return
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if value, ok := kv.data[args.Key]; ok {
			reply.Value = value
			reply.WrongLeader = false
			reply.Err = OK
			return
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
