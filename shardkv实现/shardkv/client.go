package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.

// 判断key属于哪个shard
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 { // 根据key的第一个字符来判断属于哪个shard
		shard = int(key[0]) // key的首字母的ASCII码
	}
	shard %= shardctrler.NShards // 对shard的数量取模，防止越界
	return shard
}

// 生成随机数
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 客户端结构体
type Clerk struct {
	sm       *shardctrler.Clerk // 生成一个分片控制器的客户端
	config   shardctrler.Config // 分片控制器的配置
	make_end func(string) *labrpc.ClientEnd // 这里的string是server的名字，返回的是一个labrpc.ClientEnd，可以用来发送RPC请求
	// You will have to modify this struct.
	clientId int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
// 生成一个客户端，每个客户端都有一个分片控制器的客户端，用来获取分片控制器的配置
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk) // 生成一个客户端
	ck.sm = shardctrler.MakeClerk(ctrlers) // 生成一个分片控制器的客户端
	ck.make_end = make_end // 这里的string是server的名字，返回的是一个labrpc.ClientEnd，可以用来发送RPC请求
	// You'll have to add code here.
	ck.clientId = nrand() // 生成一个随机数作为客户端的id
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
// 获取key对应的value
func (ck *Clerk) Get(key string) string {
	args := GetArgs{} // 获取参数
	args.Key = key
	args.ClientId = ck.clientId
	args.CommandId = nrand()

	for {
		args.ConfigNum = ck.config.Num // 获取当前的配置
		shard := key2shard(key) // 获取key属于哪个shard
		gid := ck.config.Shards[shard] // 获取shard属于哪个group
		if servers, ok := ck.config.Groups[gid]; ok { // 获取group中的server
			// try each server for the shard.
			for si := 0; si < len(servers); si++ { // 遍历group中的server
				srv := ck.make_end(servers[si]) // 生成一个labrpc.ClientEnd，可以用来发送RPC请求
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply) // 发送RPC请求
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) { // 如果成功获取到value或者key不存在
					return reply.Value // 返回value
				}
				if ok && (reply.Err == ErrWrongGroup) { // 如果key不属于这个group
					break // 跳出循环
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1) // 获取最新的配置
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.CommandId = nrand()

	for { // 一直尝试直到成功
		args.ConfigNum = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
