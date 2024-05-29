package kvraft

// 导入必要的包
import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes" 
	"log" 
	"sync" 
	"sync/atomic"
	"time"
)


// 定义一个时间常量，在 RPC 失败时尝试更改领导者之间的间隔时间
const WaitCmdTimeOut = time.Millisecond * 500   // cmd执行超过这个时间，就返回timeout 
const MaxLockTime = time.Millisecond * 10  // debug 用，锁超过这个时间，就打印日志


type Op struct { // 定义一个结构体，用于存储客户端的命令
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId     int64 // 用来标识commandNotify,区分不同的请求，避免同一个请求被多次执行
	CommandId int64 // 用来标识命令的唯一性，避免重复执行,与上面的ReqId一起使用可以避免重复执行相同命令
	ClientId  int64
	Key       string
	Value     string
	Method    string // Get/Put/Append 三种类型
}

type CommandResult struct {
	Err   Err // OK/ErrNoKey/ErrWrongLeader/ErrTimeOut/ErrServer 五种类型
	Value string // Get 时有效
}

type KVServer struct { // 定义一个结构体，用于存储 KV 服务器的状态
	mu      sync.Mutex
	me      int // 当前服务器的编号
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	stopCh  chan struct{} // 用于通知其他协程停止

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandNotifyCh map[int64]chan CommandResult // 建立通道和请求id的映射，用于通知请求结果，这里使用map是因为一个请求可能被多次执行
	//将 ClientId（客户端标识）与最近应用的 CommandId 关联起来。这个映射用于检测和防止重复执行相同的命令。
	//当客户端发出一个命令请求时，服务器会检查 lastApplies 中是否已经记录了相同 ClientId 的 CommandId，如果有，则说明这个命令已经被执行过，可以避免重复执行。
	lastApplies     map[int64]int64 

	//将键值对的键与值关联起来，用于存储键值存储系统的数据，相当于一个数据库
	data            map[string]string
	//持久化
	persister *raft.Persister

	//用于互斥锁
	lockStartTime time.Time
	lockEndTime   time.Time
	lockMsg       string
}

// 自定义锁
func (kv *KVServer) lock(msg string) {
	kv.mu.Lock()
	kv.lockStartTime = time.Now()
	kv.lockMsg = msg // 当发生死锁时，可以通过这个字段来定位
}

// 自定义解锁
func (kv *KVServer) unlock(msg string) {
	kv.lockEndTime = time.Now()
	duration := kv.lockEndTime.Sub(kv.lockStartTime)
	kv.lockMsg = "" // 释放锁后，清空这个字段
	kv.mu.Unlock()
	if duration > MaxLockTime { // 如果锁的时间超过了 MaxLockTime，就打印日志
		DPrintf("lock too long:%s:%s\n", msg, duration) // 打印日志
	}
}

// 移除命令通道， 根据请求的id来进行该请求的通道的移除
func (kv *KVServer) removeCh(reqId int64) {
	kv.lock("removeCh")
	defer kv.unlock("removeCh")
	delete(kv.commandNotifyCh, reqId)
}

//调用start向raft请求命令
func (kv *KVServer) waitCmd(op Op) (res CommandResult) {
	DPrintf("server %v wait cmd start,Op: %+v.\n", kv.me, op)

	//提交命令,其实这里的start要改，一个kv数据库get命令可以发生在所有节点上
	index, term, isLeader := kv.rf.Start(op) // 这里的start是raft的start，用于向raft提交命令
	if !isLeader {
		res.Err = ErrWrongLeader // 如果不是leader，就返回 ErrWrongLeader
		return
	}

	kv.lock("waitCmd") // 加锁
	ch := make(chan CommandResult, 1) // 创建一个通道，用于接收命令执行结果，后面的参数1表示通道的容量为1
	kv.commandNotifyCh[op.ReqId] = ch // 将通道和请求id进行映射，用于通知请求结果
	kv.unlock("waitCmd") // 解锁

	DPrintf("start cmd: index:%d, term:%d, op:%+v", index, term, op)

	t := time.NewTimer(WaitCmdTimeOut) // 设置一个定时器，用于超时处理
	defer t.Stop() // 在函数返回时，关闭定时器


	select {
	case <-kv.stopCh:// 如果收到stopCh，就返回 ErrServer
		DPrintf("stop ch waitCmd")
		kv.removeCh(op.ReqId)
		res.Err = ErrServer
		return

	case res = <-ch: // 如果收到通道的消息，就给res赋值，并且执行其内容。这里的res是一个CommandResult，里面包含了Err和Value
		kv.removeCh(op.ReqId) // 移除命令通道
		return

	case <-t.C: // 如果超时，就返回 ErrTimeOut
		kv.removeCh(op.ReqId)
		res.Err = ErrTimeOut
		return

	}
}

//处理Get rpc
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server %v in rpc Get,args: %+v", kv.me, args)

	_, isLeader := kv.rf.GetState() // 判断是否是leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 构造一个命令
	op := Op{
		ReqId:     nrand(),
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Key:       args.Key,
		Method:    "Get",
	}
	//等待命令执行
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value

	DPrintf("server %v in rpc Get,args：%+v,reply：%+v", kv.me, args, reply)
}

//处理Put rpc
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server %v in rpc PutAppend,args: %+v", kv.me, args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		ReqId:     nrand(),
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Key:       args.Key,
		Value:     args.Value,
		Method:    args.Op,
	}
	//等待命令执行
	res := kv.waitCmd(op)
	reply.Err = res.Err

	DPrintf("server %v in rpc PutAppend,args：%+v,reply：%+v", kv.me, args, reply)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//保存快照
func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return // 如果存储的最大容量超过设定容量，返回
	}

	//生成快照数据
	w := new(bytes.Buffer) 
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.data); err != nil {
		panic(err) 
	}
	if err := e.Encode(kv.lastApplies); err != nil {
		panic(err)
	}
	data := w.Bytes() 
	kv.rf.Snapshot(logIndex, data) // 调用Raft层的函数生成快照
}

//读取快照
//两处调用：初始化阶段；收到Snapshot命令，即接收了leader的Snapshot
func (kv *KVServer) readPersist(isInit bool, snapshotTerm, snapshotIndex int, data []byte) {
	if data == nil || len(data) < 1 { 
		return
	}
	//只要不是初始化调用，即如果收到一个Snapshot命令，就要执行该函数
	if !isInit {  
		res := kv.rf.CondInstallSnapshot(snapshotTerm, snapshotIndex, data) // 读取快照
		if !res { // 
			log.Panicln("kv read persist err in CondInstallSnapshot!") // 如果读取失败，就打印日志
			return
		}
	}
	//对数据进行同步
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var lastApplies map[int64]int64

	if d.Decode(&kvData) != nil || // 如果解码不为空或者最后一次应用的命令不为空，就打印日志
		d.Decode(&lastApplies) != nil { 
		log.Fatal("kv read persist err!") //fatal函数会打印日志，然后调用os.Exit(1)退出程序
	} else {
		kv.data = kvData //将快照中的数据同步到kv服务器中
		kv.lastApplies = lastApplies // 将快照中的最后一次应用的命令同步到kv服务器中
	}
}

func (kv *KVServer) getValueByKey(key string) (err Err, value string) {
	if v, ok := kv.data[key]; ok { // 如果存在该键，就返回OK和值
		err = OK
		value = v
	} else {
		err = ErrNoKey // 如果不存在该键，就返回ErrNoKey
	}
	return
}

func (kv *KVServer) notifyWaitCommand(reqId int64, err Err, value string) {
	if ch, ok := kv.commandNotifyCh[reqId]; ok { // 如果存在该请求id，就通知该请求
		ch <- CommandResult{ // 调用commandResult通道，将结果发送给客户端
			Err:   err, 
			Value: value,
		}
	}
}

//应用每一条命令
func (kv *KVServer) handleApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			DPrintf("get from stopCh,server-%v stop!", kv.me)
			return
		case cmd := <-kv.applyCh: // 从applyCh中读取命令
			//处理快照命令，读取快照的内容
			if cmd.SnapshotValid { // 如果快照有效，就读取快照
				DPrintf("%v get install sn,%v %v", kv.me, cmd.SnapshotIndex, cmd.SnapshotTerm)
				kv.lock("waitApplyCh_sn")
				kv.readPersist(false, cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot)
				kv.unlock("waitApplyCh_sn")
				continue
			}
			//处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex // 获取命令的索引
			DPrintf("server %v start apply command %v：%+v", kv.me, cmdIdx, cmd.Command)
			op := cmd.Command.(Op) // 获取命令
			kv.lock("handleApplyCh")

			if op.Method == "Get" {
				//处理读
				e, v := kv.getValueByKey(op.Key) // 获取键值对
				kv.notifyWaitCommand(op.ReqId, e, v) // 将结果封装成CommandResult，然后通知客户端
			} else if op.Method == "Put" || op.Method == "Append" {
				//处理写
				//判断命令是否重复
				isRepeated := false // 判断命令是否重复，默认不重复
				if v, ok := kv.lastApplies[op.ClientId]; ok { // 如果存在该客户端id，就判断该客户端的命令id是否与当前命令id相同
					if v == op.CommandId { // 如果相同，就说明该命令已经被执行过了，就不需要再执行了
						isRepeated = true
					}
				}

				if !isRepeated { // 如果不重复，就执行命令
					switch op.Method {
					case "Put":
						kv.data[op.Key] = op.Value // 将键值对存储到kv服务器中
						kv.lastApplies[op.ClientId] = op.CommandId // 将客户端id和命令id进行映射，用于判断命令是否重复
					case "Append":
						e, v := kv.getValueByKey(op.Key) // 获取键值对
						if e == ErrNoKey { // 如果不存在该键，就按put处理
							//按put处理
							kv.data[op.Key] = op.Value
							kv.lastApplies[op.ClientId] = op.CommandId
						} else { // 如果存在该键，就将值进行追加
							//追加
							kv.data[op.Key] = v + op.Value
							kv.lastApplies[op.ClientId] = op.CommandId
						}
					default:
						kv.unlock("handleApplyCh") // 如果命令不是Put或者Append，就打印日志
						panic("unknown method " + op.Method)
					}

				}
				//命令处理成功
				kv.notifyWaitCommand(op.ReqId, OK, "") // 将结果封装成CommandResult，然后通知客户端
			} else {
				kv.unlock("handleApplyCh")
				panic("unknown method " + op.Method)
			}

			DPrintf("apply op: cmdId:%d, op: %+v, data:%v", cmdIdx, op, kv.data[op.Key])
			//每应用一条命令，就判断是否进行持久化
			kv.saveSnapshot(cmdIdx) // 保存快照

			kv.unlock("handleApplyCh")
		}

	}

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
// 服务器数组包含通过Raft合作的服务器的端口，以形成容错的键/值服务。
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{}) // 定义Op结构体，用于编码和解码

	kv := new(KVServer) // 创建一个KVServer
	kv.me = me // 设置服务器编号
	kv.maxraftstate = maxraftstate // 设置最大存储容量
	kv.persister = persister // 设置持久化

	// You may need initialization code here.
	kv.lastApplies = make(map[int64]int64) // 创建一个map，用于存储客户端id和命令id的映射
	kv.data = make(map[string]string) // 创建一个map，用于存储键值对

	kv.stopCh = make(chan struct{}) //	创建一个通道，用于通知其他协程停止
	//读取快照
	kv.readPersist(true, 0, 0, kv.persister.ReadSnapshot()) // 读取快照，其中各个参数含义为：是否是初始化，快照的term，快照的索引，快照的数据

	kv.commandNotifyCh = make(map[int64]chan CommandResult) // 创建一个map，用于存储请求id和通道的映射
	kv.applyCh = make(chan raft.ApplyMsg) // 创建一个通道，用于存储命令
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // 创建一个raft，用于进行命令的应用

	go kv.handleApplyCh() // 启动一个协程，用于处理命令

	return kv
}
