package shardkv

// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"bytes"
	"encoding/gob"
	"labgob"
	"log"
	"shardmaster"
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
const PullingInterval = time.Millisecond * time.Duration(150)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	LastQueryId int64
	Key         string
	Value       string
	Type        string
}

type WaitingOp struct {
	waitchan chan bool
	op       *Op
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm           *shardmaster.Clerk
	config       shardmaster.Config
	data         map[string]string
	waitingforOp map[int][]*WaitingOp //异步等待相应操作完成
	dupremove    map[int64]int64      // 去重
	term         int                  //kv服务器已经应用到的日志任期号和index （初始化都为0，kv的指令从1开始计数，index比raft中大一）
	index        int
}

func (kv *ShardKV) Opexec(op Op) Err {

	index, _, isleader := kv.rf.Start(op)
	if isleader == false {
		DPrintf("server ", kv.me, "this is not leader!!")
		return ErrWrongLeader
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
			res = ErrWrongLeader
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if args.ConfigNum != kv.config.Num {
		DPrintf("Get:ConfigNum is different,args is %v ,kv.config.Num is %v", args, kv.config.Num)
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{}
	op.ClientId = args.ClientId
	op.LastQueryId = args.LastQueryId
	op.Key = args.Key
	op.Type = OpGet

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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.ConfigNum != kv.config.Num {
		DPrintf("PutAppend:ConfigNum is different,args is %v ,kv.config.Num is %v", args, kv.config.Num)
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{ClientId: args.ClientId, LastQueryId: args.LastQueryId, Key: args.Key, Value: args.Value, Type: args.Op}
	reply.Err = kv.Opexec(op)
	if reply.Err == OK {
		reply.WrongLeader = false
		return
	} else {
		reply.WrongLeader = true
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.dupremove = make(map[int64]int64)
	kv.term = 0
	kv.index = 0
	kv.waitingforOp = make(map[int][]*WaitingOp)
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.sm.Query(-1)
	kv.loaddata(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh
			kv.ApplyMsg(msg)
		}
	}()

	go func() {
		pollingTimer := time.NewTimer(PullingInterval)
		for {
			select {
			case <-pollingTimer.C:
				pollingTimer.Reset(PullingInterval)
				kv.mu.Lock()
				nextConfigNum := kv.config.Num + 1
				kv.mu.Unlock()
				newconf := kv.sm.Query(nextConfigNum)
				kv.mu.Lock()
				if newconf.Num > kv.config.Num {
					kv.config = newconf
				}
				kv.mu.Unlock()
			}
		}
	}()
	return kv
}

func (kv *ShardKV) ApplyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.CommandValid == false {
		if msg.Command == LOADSNAPSHOT {
			if msg.SnapShotTerm > kv.term || (msg.SnapShotTerm == kv.term && msg.SnapShotIndex > (kv.index-1)) {
				DPrintf("\n sever ", kv.me, "\nMSG: TERM,INDEX", msg.SnapShotTerm, msg.SnapShotIndex, "\n kv: term,index", kv.term, kv.index)
				kv.loaddata(msg.SnapShot)
			}
		}
		return
	} else if op, ok := msg.Command.(Op); ok {
		//检测命令是否可以执行，clienid下opid为空或者大于相应opid（去重）才能执行
		if index, ok := kv.dupremove[op.ClientId]; !ok || index < op.LastQueryId {
			switch op.Type {
			case OpPut:
				kv.data[op.Key] = op.Value
			case OpAppend:
				kv.data[op.Key] = kv.data[op.Key] + op.Value
			default:
			}
			DPrintf("gid is %v,server number is %v,op type is %v,data is %v", kv.gid, kv.me, op.Type, kv.data)
			kv.dupremove[op.ClientId] = op.LastQueryId
			kv.term = msg.CommandTerm
			kv.index = msg.CommandIndex
		} else {
			DPrintf("Duplicate operation!!")
		}
		if waiting, ok := kv.waitingforOp[msg.CommandIndex]; ok {
			for _, waiter := range waiting {
				if waiter.op.ClientId == op.ClientId && waiter.op.LastQueryId == op.LastQueryId {
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

//调用者持mu锁
func (kv *ShardKV) persistdata() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.gid)
	e.Encode(kv.term)
	e.Encode(kv.index)
	e.Encode(kv.dupremove)
	e.Encode(kv.data)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) loaddata(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	var gid map[string]string
	var term int
	var index int
	var dup map[int64]int64
	var dat map[string]string
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&gid) != nil || d.Decode(&term) != nil || d.Decode(&index) != nil || d.Decode(&dup) != nil || d.Decode(&dat) != nil {
		panic("read data error")
	} else {
		kv.data = gid
		kv.term = term
		kv.index = index
		kv.dupremove = dup
		kv.data = dat
	}
	return
}
