package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeOut"
)

const (
	OpPut        = "Put"
	OpAppend     = "Append"
	OpGet        = "get"
	OpMigration  = "migration"
	OpClean      = "clean"
	LOADSNAPSHOT = "loadsnapshotting"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClientId    int64
	LastQueryId int64
	ConfigNum   int
	Key         string
	Value       string
	Op          string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	ClientId    int64
	LastQueryId int64
	ConfigNum   int
	Key         string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type MigrationArgs struct {
	Shard     int
	ConfigNum int
}

type MigrationData struct {
	Data map[string]string
}

type MigrationReply struct {
	Err           Err
	Shard         int
	ConfigNum     int
	MigrationData MigrationData
}
