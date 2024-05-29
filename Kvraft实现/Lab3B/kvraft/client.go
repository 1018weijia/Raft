package kvraft
// 导入必要的包
import (
	"6.824/labrpc" //用于处理 RPC 通信的 labrpc
	"log" //记录日志的 log
	"time"
)
import "crypto/rand" // 用于生成随机数的 crypto/rand
import "math/big" // 用于任意精度算术的 math/big


// 定义一个时间常量，在 RPC 失败时尝试更改领导者之间的间隔时间
// 这里的时间间隔不宜过长，否则会导致客户端请求超时
// 为了处理网络延迟和服务器处理延迟。当客户端向服务器发送请求后，可能需要一段时间才能收到服务器的响应。
// 如果在这段时间内客户端继续发送新的请求，可能会导致服务器的负载过大，甚至可能会导致之前的请求还没有处理完就被新的请求覆盖。
const (
	ChangeLeaderInterval = time.Millisecond * 20
)

//客户端
type Clerk struct {
	servers []*labrpc.ClientEnd // 用于客户端与所有的服务器通信连接，相当于一个桥梁
	// You will have to modify this struct.
	clientId int64 //客户端的id
	leaderId int //当前leader的id
}

//用于生成一个随机数，可以生成clientId和commandId
func nrand() int64 {
	max := big.NewInt(int64(1) << 62) // 生成一个最大值为 2^62 的大整数，表示随机数的最大范围
	bigx, _ := rand.Int(rand.Reader, max) // 生成一个随机数，范围为 [0, 2^62)
	x := bigx.Int64() // 将随机数转换为 int64 类型
	return x
}

//生成一个客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk) 
	ck.servers = servers
	ck.clientId = nrand() // 生成一个随机的客户端 id,这里不使用固定值，是为了防止多个客户端同时运行时，出现相同的客户端 id，发生冲突
	// You'll have to add code here.
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

//根据key获取value
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	//DPrintf("%v client get key：%s.", ck.clientId, key)
	// 读取 key 对应的 value
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: nrand(), // 生成一个随机的 commandId，这里不用固定值，是为了防止多个客户端同时运行时，出现相同的 commandId，发生冲突
	}
	leaderId := ck.leaderId //当前leader的id

	for {
		reply := GetReply{}  
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply) // 向 leader 发送 Get RPC 请求
		if !ok {
			//如果请求失败，等一段时间再请求,换一个节点再请求
			DPrintf("%v client get key %v from server %v,not ok.", ck.clientId, key, leaderId)
			time.Sleep(ChangeLeaderInterval) // 等待一段时间再请求
			// 这里等一会是为了避免server过载
			leaderId = (leaderId + 1) % len(ck.servers) // 换一个节点再请求
			continue
		} else if reply.Err != OK { // 如果请求成功，但是返回的 reply.Err 不为 OK，说明请求失败
			DPrintf("%v client get key %v from server %v,reply err = %v!", ck.clientId, key, leaderId, reply.Err)
		}
		// 根据 reply.Err 的值，判断请求是否成功
		switch reply.Err {

			//请求成功
		case OK:
			DPrintf("%v client get key %v from server %v,value: %v,OK.", ck.clientId, key, leaderId, reply.Value, leaderId)
			ck.leaderId = leaderId
			return reply.Value


			// 请求失败，key 不存在
		case ErrNoKey:
			DPrintf("%v client get key %v from server %v,NO KEY!", ck.clientId, key, leaderId)
			ck.leaderId = leaderId
			return "" // 返回空字符串


			// 请求超时
		case ErrTimeOut:
			continue // 继续请求


			// 请求失败，leader 错误
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers) // 换一个节点再请求
			continue
		}

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

// PutAppendArgs 用于 PutAppend RPC 请求的参数
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("%v client PutAppend,key:%v,value:%v,op:%v", ck.clientId, key, value, op)
	// You will have to modify this function.

	// 参数
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op, // PutAppend 的类型，Put 或者 Append
		ClientId:  ck.clientId,
		CommandId: nrand(),
	}

	leaderId := ck.leaderId


	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			//可能当前请求的server不是leader，换一个server再访问
			DPrintf("%v client set key %v to %v to server %v,not ok.", ck.clientId, key, value, leaderId)
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		} else if reply.Err != OK {
			DPrintf("%v client set key %v to %v to server %v,reply err = %v!", ck.clientId, key, value, leaderId, reply.Err)
		}

		switch reply.Err {

			// 请求成功
		case OK:
			DPrintf("%v client set key %v to %v to server %v,OK.", ck.clientId, key, value, leaderId)
			ck.leaderId = leaderId
			return

			// 请求失败，key 不存在
		case ErrNoKey:
			DPrintf("%v client set key %v to %v to server %v,NOKEY!", ck.clientId, key, value, leaderId)
			return

			// 请求超时,继续请求
		case ErrTimeOut:
			continue

			// 请求失败，leader 错误
		case ErrWrongLeader:
			//换一个节点继续请求
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue


			// 请求失败，server 错误
		case ErrServer:
			//换一个节点继续请求
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue

			// 请求失败，未知错误
		default:
			log.Fatal("client rev unknown err", reply.Err) // 未知错误，直接退出
		}
	}
}

// Put 用于向 kv 服务器写入数据
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append 用于向 kv 服务器追加数据
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
