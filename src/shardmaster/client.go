package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId    int64
	LastQueryId int64
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
	// Your code here.
	ck.ClientId = nrand()
	ck.LastQueryId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Clientid: ck.ClientId, Num: num}
	args.Queryid = atomic.AddInt64(&ck.LastQueryId, 1)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Clientid: ck.ClientId, Servers: servers}
	// Your code here.
	args.Queryid = atomic.AddInt64(&ck.LastQueryId, 1)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{Clientid: ck.ClientId, GIDs: gids}
	// Your code here.
	args.Queryid = atomic.AddInt64(&ck.LastQueryId, 1)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Clientid: ck.ClientId, Shard: shard, GID: gid}
	// Your code here.
	args.Queryid = atomic.AddInt64(&ck.LastQueryId, 1)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
