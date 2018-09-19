package shardmaster

import "raft"
import "labrpc"
import "sync"
import (
	"labgob"
	"log"
	"time"
)

const Debug = 1

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
	Type        RequestType
	Clientid    int64
	Queryid     int64
	JoinServers map[int][]string //join
	LeaveGIDs   []int            //leave
	MoveShard   int              //move
	MoveGID     int              //move
	QueryNum    int              //query
}

func (sm *ShardMaster) ExecOp(op Op) Err {
	index, _, isleader := sm.rf.Start(op)
	if isleader == false {
		DPrintf("This server is not leader , server number is %v", sm.me)
		return ErrNotLeader
	}
	DPrintf("This server is leader , server number is %v", sm.me)
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
		DPrintf("SM execute timeout!")
		res = ErrTimeout
	}
	sm.mu.Lock()
	delete(sm.waitingforOp, index)
	sm.mu.Unlock()
	return res
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	servers := make(map[int][]string)
	for gid, server := range args.Servers {
		servers[gid] = append([]string{}, server...)
	}
	op := Op{Type: JOIN, Clientid: args.Clientid, Queryid: args.Queryid, JoinServers: servers}
	reply.Err = sm.ExecOp(op)
	if reply.Err != OK {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Type: Leave, Clientid: args.Clientid, Queryid: args.Queryid, LeaveGIDs: args.GIDs}
	reply.Err = sm.ExecOp(op)
	if reply.Err != OK {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Type: MOVE, Clientid: args.Clientid, Queryid: args.Queryid, MoveGID: args.GID, MoveShard: args.Shard}
	reply.Err = sm.ExecOp(op)
	if reply.Err != OK {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Type: QUERY, Clientid: args.Clientid, Queryid: args.Queryid, QueryNum: args.Num}
	reply.Err = sm.ExecOp(op)
	if reply.Err != OK {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if len(sm.configs) > args.Num {
			reply.Config = sm.getconfig(args.Num)
			return
		} else {
			reply.Err = ErrNoNum
			return
		}
	}
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
	sm.waitingforOp = make(map[int][]*WaitingOp)
	sm.dupremove = make(map[int64]int64) // 去重

	go func() {
		for {
			msg := <-sm.applyCh
			sm.Apply(msg)
		}
	}()

	return sm
}

func (sm *ShardMaster) Apply(msg raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if msg.CommandValid == false {
		return
	} else {
		DPrintf("op is %v, server number is %d", msg, sm.me)
		op := msg.Command.(Op)
		if index, ok := sm.dupremove[op.Clientid]; !ok || index < op.Queryid {
			switch op.Type {
			case JOIN:
				jion := op.JoinServers
				newconf := sm.getconfig(-1)
				NewShards := make([]int, 0) //记录新加入的gid
				//初始化Groups
				for gid, servers := range jion {
					if _, ok := newconf.Groups[gid]; ok {
						newconf.Groups[gid] = append(newconf.Groups[gid], servers...)
					} else {
						newconf.Groups[gid] = append([]string{}, servers...)
						NewShards = append(NewShards, gid)
					}
				}
				//下面进行分片重新分配，也就是初始化Shards [NShards]int结构: shard->group
				if len(newconf.Groups) == 0 {
					newconf.Shards = [NShards]int{}
				} else {
					var minShardNum, maxShardNum, maxShardNumCount int
					ShardNum := make(map[int]int) //记录每个gid的shard数（分片数）
					minShardNum = NShards / len(newconf.Groups)
					if maxShardNumCount = NShards % len(newconf.Groups); maxShardNumCount != 0 {
						maxShardNum = minShardNum + 1
					} else {
						maxShardNum = minShardNum
					}
					for i, j := 0, 0; i < NShards; i++ {
						gid := newconf.Shards[i]
						//为了尽量少移动各分片，所以只移动超过的部分
						//1、没有分配的直接放入新的gid中
						//2、超过最大分片数的需要重新分配
						if gid == 0 ||
							(minShardNum == maxShardNum && ShardNum[gid] == minShardNum) ||
							(minShardNum < maxShardNum && ShardNum[gid] == minShardNum && maxShardNumCount <= 0) {
							newgid := NewShards[j]
							newconf.Shards[i] = newgid
							ShardNum[newgid] += 1
							j = (j + 1) % len(NewShards) //这里主要是为了循环分配给每个新加入的group
						} else { //其他情况保持原来的分配
							ShardNum[gid] += 1
							if minShardNum < maxShardNum && ShardNum[gid] == maxShardNum {
								maxShardNumCount -= 1
							}
						}
					}
				}
				//最后将新的conf配置添加到配置中
				sm.appendNewconf(newconf)
			case Leave:
				leave := op.LeaveGIDs
				newconf := sm.getconfig(-1)
				keepgid := make([]int, 0) //记录剩下来的gid
				leavegid := make(map[int]struct{}, 0)
				for gid := range leave {
					delete(newconf.Groups, gid)
					leavegid[gid] = struct{}{}
				}
				for gid := range newconf.Groups {
					keepgid = append(keepgid, gid)
				}

				if len(newconf.Groups) == 0 {
					newconf.Shards = [NShards]int{}
				} else {
					for i, j := 0, 0; i < NShards; i++ {
						gid := newconf.Shards[i]
						if _, ok := leavegid[gid]; ok {
							newgid := keepgid[j]
							newconf.Shards[i] = newgid
							j = (j + 1) % len(keepgid)
						}
					}
				}
				sm.appendNewconf(newconf)
			case MOVE:
				moveshard := op.MoveShard
				movegid := op.MoveGID
				newconf := sm.getconfig(-1)
				if moveshard > 0 && moveshard < NShards {
					newconf.Shards[moveshard] = movegid
				}
				sm.appendNewconf(newconf)
			default:
			}
			sm.dupremove[op.Clientid] = op.Queryid
		} else {
			DPrintf("Duplicate operation!!")
		}
		if waiting, ok := sm.waitingforOp[msg.CommandIndex]; ok {
			for _, waiter := range waiting {
				if waiter.op.Clientid == op.Clientid && waiter.op.Queryid == op.Queryid {
					waiter.waitchan <- true
				} else {
					waiter.waitchan <- false
				}
			}
		}
	}
}

// 调用者持锁
func (sm *ShardMaster) getconfig(num int) Config {
	len := len(sm.configs)
	var src Config
	if num < 0 || num >= len {
		src = sm.configs[len-1]
	} else {
		src = sm.configs[num]
	}
	dst := Config{Num: src.Num, Shards: src.Shards, Groups: make(map[int][]string)}
	for git, servers := range src.Groups {
		dst.Groups[git] = append([]string{}, servers...)
	}
	return dst
}

func (sm *ShardMaster) appendNewconf(conf Config) {
	conf.Num = len(sm.configs)
	sm.configs = append(sm.configs, conf)
}
