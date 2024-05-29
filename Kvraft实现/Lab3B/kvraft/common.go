package kvraft

const (
	OK             = "OK" // success
	ErrNoKey       = "ErrNoKey" // 表示没有这个key，直接退出
	ErrWrongLeader = "ErrWrongLeader" // 表示不是leader，需要重试
	ErrTimeOut     = "ErrTimeOut" // 表示超时，需要重试
	ErrServer      = "ErrServer" // 表示服务端出错，需要重试
)

type Err string // 错误类型，内容为上面的五种

// Put or Append
type PutAppendArgs struct {
	Key   string 
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64 // 客户端id
	CommandId int64 // 客户端命令id
}

type PutAppendReply struct {
	Err Err // 对于Put和Append来说，只有成功和失败两种情况，因此不需要value
}

type GetArgs struct {
	Key       string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
}

type GetReply struct {
	Err   Err
	Value string // 对于Get命令来说，需要返回value
}
