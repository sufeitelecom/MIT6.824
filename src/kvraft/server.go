package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	FOLLOWER     = "follower"
	CANDIDATE    = "candidate"
	LEADER       = "leader"
	LOADSNAPSHOT = "loadsnapshotting"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

const Timeout = time.Second * 3

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
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
	term         int                  //kv服务器已经应用到的日志任期号和index （初始化都为0，kv的指令从1开始计数，index比raft中大一）
	index        int
}

func (kv *KVServer) Opexec(op Op) Err {

	index, _, isleader := kv.rf.Start(op)
	if isleader == false {
		DPrintf("server ", kv.me, "this is not leader!!")
		return ErrNotLeader
	}

	waiting := make(chan bool, 1)

	kv.mu.Lock()
	kv.waitingforOp[index] = append(kv.waitingforOp[index], &WaitingOp{op: &op, waitchan: waiting})
	kv.mu.Unlock()

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
	kv.mu.Lock()
	delete(kv.waitingforOp, index)
	kv.mu.Unlock()
	return res

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Type = OpGet
	op.Clientid = args.Clientid
	op.Opid = args.Opid
	op.Key = args.Key

	reply.Err = kv.Opexec(op)
	if reply.Err != OK {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if value, ok := kv.data[args.Key]; ok {
			reply.Value = value
			return
		} else {
			reply.Err = ErrNoKey
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Clientid = args.Clientid
	op.Opid = args.Opid
	op.Type = args.Op
	op.Key = args.Key
	op.Value = args.Value

	reply.Err = kv.Opexec(op)
	if reply.Err != OK {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		return
	}
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
	kv.term = 0
	kv.index = 0

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.waitingforOp = make(map[int][]*WaitingOp)
	kv.Opseq = make(map[int64]int64)

	kv.loaddata(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh
			kv.ApplyMsg(msg)
		}
	}()
	return kv
}

func (kv *KVServer) ApplyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.CommandValid == false {
		if msg.Command == LOADSNAPSHOT {
			kv.loaddata(msg.SnapShot)
		}
		return
	} else {
		op := msg.Command.(Op)
		//检测命令是否可以执行，clienid下opid为空或者大于相应opid（去重）才能执行
		if index, ok := kv.Opseq[op.Clientid]; !ok || index < op.Opid {
			switch op.Type {
			case OpPut:
				kv.data[op.Key] = op.Value
			case OpAppend:
				kv.data[op.Key] = kv.data[op.Key] + op.Value
			default:
			}
			DPrintf("Now {Clientid is %d seqid is %d oriid is %d} ,the data[%s],value is : %s", op.Clientid, op.Opid, kv.Opseq[op.Clientid], op.Key, kv.data[op.Key])
			kv.Opseq[op.Clientid] = op.Opid
			kv.term = msg.CommandTerm
			kv.index = msg.CommandIndex
		} else {
			DPrintf("Duplicate operation!!")
		}
		if waiting, ok := kv.waitingforOp[msg.CommandIndex]; ok {
			for _, waiter := range waiting {
				if waiter.op.Clientid == op.Clientid && waiter.op.Opid == op.Opid {
					waiter.waitchan <- true
				} else {
					waiter.waitchan <- false
				}
			}
		}
	}

	if kv.maxraftstate != -1 && kv.rf.Getpersister().RaftStateSize() > kv.maxraftstate {
		data := kv.persistdata()
		go kv.rf.SaveSnapShotAndState(data, kv.index-1, kv.term)
	}
}

func (kv *KVServer) loaddata(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	var res map[string]string
	var term int
	var index int
	var seq map[int64]int64
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&res) != nil || d.Decode(&term) != nil || d.Decode(&index) != nil || d.Decode(&seq) != nil {
		panic("read data error")
	} else {
		kv.data = res
		kv.term = term
		kv.index = index
		kv.Opseq = seq
	}
}

func (kv *KVServer) persistdata() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.term)
	e.Encode(kv.index)
	e.Encode(kv.Opseq)
	data := w.Bytes()
	return data
}
