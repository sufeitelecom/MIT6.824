package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Clientid int64 //客户端id
	Opid     int64 //客户端上次操作id，由于raft没有持久化applyid，并不保证去重，只保证有序，所以服务器需要保证相同操作不能执行两次，Clientid+Opid确保操作唯一性
	Leaderid int   //上次通信的主服务id，主要是为了避免每次都去查找
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.Clientid = nrand()
	ck.Opid = 0
	ck.Leaderid = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Clientid = ck.Clientid
	args.Key = key
	args.Opid = atomic.AddInt64(&ck.Opid, 1)

	n := len(ck.servers)
	start := ck.Leaderid
	for {
		reply := GetReply{}
		ok := ck.servers[start].Call("KVServer.Get", &args, &reply)
		if ok { //只有当成功返回，并且是确定是主库，错误类型为（没有错误或者没有相应键值对）
			if (reply.Err == OK || reply.Err == ErrNoKey) && reply.WrongLeader == false {
				DPrintf("ClerK Get %s success, value is %s", args.Key, reply.Value)
				ck.Leaderid = start
				return reply.Value
			}
		}
		start = (start + 1) % n
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Op = op
	args.Key = key
	args.Value = value
	args.Clientid = ck.Clientid
	args.Opid = atomic.AddInt64(&ck.Opid, 1)

	n := len(ck.servers)
	start := ck.Leaderid
	for {
		reply := PutAppendReply{}
		ok := ck.servers[start].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK && reply.WrongLeader == false {
				DPrintf("Clerk PutAppend success! type:%s,id:%d-%d  %s:%s", args.Op, args.Clientid, args.Opid, args.Key, args.Value)
				ck.Leaderid = start
				return
			}
		}
		start = (start + 1) % n
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
