package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
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
	ck.clientId = nrand()
	return ck
}

// Query()发送一个Query RPC到一个shardctrler服务器，等待回复，如果回复是一个错误的leader，或者没有回复，那么等待一段时间后重试另一个服务器。
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num // 要查询的config的序号，类似于raft的term
	args.ClientId = ck.clientId // 客户端的id，用于区分不同的客户端
	args.CommandId = nrand() // 客户端的命令id，用于区分不同的命令
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply) // 向服务器发送请求
			if ok && reply.WrongLeader == false { // 如果成功，且是leader，则返回config
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
 // Join()发送一个Join RPC到一个shardctrler服务器，等待回复，如果回复是一个错误的leader，或者没有回复，那么等待一段时间后重试另一个服务器。
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers // 要加入的服务器ID和地址，用map表示，key是ID，value是地址
	args.ClientId = ck.clientId
	args.CommandId = nrand()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false { // 如果成功，且是leader，则返回
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Leave()发送一个Leave RPC到一个shardctrler服务器，等待回复，如果回复是一个错误的leader，或者没有回复，那么等待一段时间后重试另一个服务器。
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids // 要离开的服务器ID，数据类型为[]int，因为可能有多个
	args.ClientId = ck.clientId
	args.CommandId = nrand()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard // 要移动的shard
	args.GID = gid // 要移动到的gid
	args.ClientId = ck.clientId
	args.CommandId = nrand()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false { // 如果成功，且是leader，则返回
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
