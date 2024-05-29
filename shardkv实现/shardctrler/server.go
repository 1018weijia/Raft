package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"

const WaitCmdTimeOut = time.Millisecond * 500 // cmd执行超过这个时间，就返回timeout
const MaxLockTime = time.Millisecond * 10     // debug

// 定义结构体
type ShardCtrler struct {
	mu      sync.Mutex // 互斥锁
	me      int // 当前server的id
	rf      *raft.Raft // raft实例
	applyCh chan raft.ApplyMsg // raft的applyCh

	// Your data here.
	stopCh          chan struct{} // 用于停止server
	commandNotifyCh map[int64]chan CommandResult //k-v：ReqId-CommandResult
	lastApplies     map[int64]int64 //k-v：ClientId-CommandId，用于判断是否重复执行命令

	configs []Config // indexed by config num // 保存历史配置

	//用于互斥锁
	lockStartTime time.Time
	lockEndTime   time.Time
	lockMsg       string
}

// 执行命令的返回结果
type CommandResult struct {
	Err    Err // 错误码
	Config Config // 当前配置
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId     int64 //用来标识commandNotify
	CommandId int64
	ClientId  int64
	Args      interface{} //JoinArgs,LeaveArgs,MoveArgs,QueryArgs四种类型
	Method    string //Join,Leave,Move,Query四种类型
}

//自定义锁
func (sc *ShardCtrler) lock(msg string) {
	sc.mu.Lock()
	sc.lockStartTime = time.Now()
	sc.lockMsg = msg
}

func (sc *ShardCtrler) unlock(msg string) {
	sc.lockEndTime = time.Now()
	duration := sc.lockEndTime.Sub(sc.lockStartTime)
	sc.lockMsg = ""
	sc.mu.Unlock() // 释放锁
	if duration > MaxLockTime {
		DPrintf("lock too long:%s:%s\n", msg, duration) // 用于debug，打印锁的持续时间
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	close(sc.stopCh)
	// Your code here, if desired.
}

func (sc *ShardCtrler) removeCh(reqId int64) {//删除reqId对应的channel，防止内存泄漏
	sc.lock("removeCh")
	defer sc.unlock("removeCh")
	delete(sc.commandNotifyCh, reqId)
}

func (sc *ShardCtrler) getConfigByIndex(idx int) Config { // 根据idx获取config配置
	if idx < 0 || idx >= len(sc.configs) { //若idx为-1或者超出当前最新config，返回最新的config
		//因为会在config的基础上进行修改形成新的config，又涉及到map，需要深拷贝
		return sc.configs[len(sc.configs)-1].Copy() // 减一是因为configs是从0开始的
	}
	return sc.configs[idx].Copy() // 返回idx对应的config
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {//返回raft实例
	return sc.rf
}

/*
rpc
*/

// 处理RPC
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sc.waitCommand(args.ClientId, args.CommandId, "Join", *args)// 调用waitCommand等待命令执行
	if res.Err == ErrWrongLeader { // 如果不是leader，返回错误
		reply.WrongLeader = true
	}
	reply.Err = res.Err // 返回错误码
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	res := sc.waitCommand(args.ClientId, args.CommandId, "Leave", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	res := sc.waitCommand(args.ClientId, args.CommandId, "Move", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("server %v query:args %+v", sc.me, args)

	//如果是查询已经存在的配置可以直接返回，因为存在的配置是不会改变的；
	//如果是-1，则必须在handleApplyCh中进行处理，按照命令顺序执行，不然不准确。
	sc.lock("query")
	if args.Num >= 0 && args.Num < len(sc.configs) { // 查询已经存在的配置
		reply.Err = OK
		reply.WrongLeader = false // 不需要在leader上执行
		reply.Config = sc.getConfigByIndex(args.Num)
		sc.unlock("query")
		return
	}
	sc.unlock("query") // 释放锁
	// 查询最新配置，需要调用waitCommand在leader上执行
	res := sc.waitCommand(args.ClientId, args.CommandId, "Query", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
	reply.Config = res.Config // 返回最新配置
}

func (sc *ShardCtrler) waitCommand(clientId int64, commandId int64, method string, args interface{}) (res CommandResult) {
	DPrintf("server %v wait cmd start,clientId：%v,commandId: %v,method: %s,args: %+v", sc.me, clientId, commandId, method, args)
	op := Op{
		ReqId:     nrand(),
		ClientId:  clientId,
		CommandId: commandId,
		Method:    method,
		Args:      args,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader { // 如果不是leader，返回错误
		res.Err = ErrWrongLeader
		DPrintf("server %v wait cmd NOT LEADER.", sc.me)
		return
	}
	sc.lock("waitCommand")
	ch := make(chan CommandResult, 1) // 创建channel，用于通知命令执行结果
	sc.commandNotifyCh[op.ReqId] = ch // 将通道和reqId绑定
	sc.unlock("waitCommand")
	DPrintf("server %v wait cmd notify,index: %v,term: %v,op: %+v", sc.me, index, term, op)

	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select { // 根据执行结果进行判断
	case <-t.C: // 超时
		res.Err = ErrTimeout
	case res = <-ch: // 执行成功
	case <-sc.stopCh: // server停止
		res.Err = ErrServer
	}

	sc.removeCh(op.ReqId) // 移除channel
	DPrintf("server %v wait cmd end,Op: %+v.", sc.me, op)
	return

}

/*
配置调整代码
*/

//配置的调整
//我们的策略是尽量不改变当前的配置
func (sc *ShardCtrler) adjustConfig(conf *Config) {
	//针对三种情况分别进行调整
	if len(conf.Groups) == 0 { //没有group，所有shard都设置为0
		conf.Shards = [NShards]int{} // NShards是shard的数量，这里初始化为0，即长度为NShards的int数组
	} else if len(conf.Groups) == 1 { // 只有一个group，所有shard都设置为该group
		for gid, _ := range conf.Groups { // 对Group中的每一个gid,进行Shard的分配
			for i, _ := range conf.Shards { // 对所有shard都分配给该gid
				conf.Shards[i] = gid
			}
		}
	} else if len(conf.Groups) <= NShards {
		//group数小于shard数，因此某些group可能会分配多一个或多个shard
		avgShardsCount := NShards / len(conf.Groups) // 每个group平均分配的shard数
		otherShardsCount := NShards - avgShardsCount*len(conf.Groups) // 多出来的shard数
		isTryAgain := true // 是否需要再次调整

		for isTryAgain {
			isTryAgain = false
			DPrintf("adjust config,%+v", conf)
			//获取所有的gid
			var gids []int
			for gid, _ := range conf.Groups {
				gids = append(gids, gid)
			}
			sort.Ints(gids)
			//遍历每一个server
			for _, gid := range gids {
				count := 0
				for _, val := range conf.Shards { // 统计该server分配的shard数
					if val == gid {
						count++
					}
				}

				//判断是否要改变配置
				if count == avgShardsCount {
					//不需要改变配置
					continue
				} else if count > avgShardsCount && otherShardsCount == 0 { // 如果多出来的shard数为0，不需要改变配置
					//多出来的设置为0
					temp := 0
					for k, v := range conf.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else {
								conf.Shards[k] = 0
							}
						}
					}
				} else if count > avgShardsCount && otherShardsCount > 0 {
					//此时看看多出的shard能否全部分配给该server
					//如果没有全部分配完，下一次循环再看
					//如果全部分配完还不够，则需要将多出的部分设置为0
					temp := 0
					for k, v := range conf.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else if temp == avgShardsCount && otherShardsCount > 0 {
								otherShardsCount -= 1
							} else {
								conf.Shards[k] = 0
							}
						}
					}

				} else {
					//count < arg
					for k, v := range conf.Shards {
						if v == 0 && count < avgShardsCount {
							conf.Shards[k] = gid
							count += 1
						}
						if count == avgShardsCount {
							break
						}
					}
					//因为调整的顺序问题，可能前面调整的server没有足够的shard进行分配，需要再进行一次调整
					if count < avgShardsCount {
						DPrintf("adjust config try again.")
						isTryAgain = true
						continue
					}
				}
			}

			//调整完成后，可能会有所有group都打到平均的shard数，但是多出来的shard没有进行分配
			//此时可以采用轮询的方法
			cur := 0
			for k, v := range conf.Shards {
				//需要进行分配的
				if v == 0 {
					conf.Shards[k] = gids[cur]
					cur += 1
					cur %= len(conf.Groups)
				}
			}

		}
	} else {
		//group数大于shard数，每一个group最多一个shard，会有group没有shard

		gidsFlag := make(map[int]int) // 用于标记gid是否已经分配过shard
		emptyShards := make([]int, 0, NShards) // 用于记录没有分配shard的shard的index
		for k, gid := range conf.Shards { // 遍历所有shard
			if gid == 0 { // 如果没有分配shard，记录下来
				emptyShards = append(emptyShards, k) // 记录没有分配shard的shard的index到emptyShards中
				continue
			}
			if _, ok := gidsFlag[gid]; ok { // 如果已经分配过shard，将该shard设置为0
				conf.Shards[k] = 0 // 表示可以再把这个shard分配给其他的gid
				emptyShards = append(emptyShards, k)
			} else { // 如果标记这个gidsFlag中没有该gid，说明是第一次进入，将该gid标记为已经分配过shard
				gidsFlag[gid] = 1
			}
		}
		if len(emptyShards) > 0 { // 如果有没有分配shard的shard
			var gids []int
			for k, _ := range conf.Groups { // 遍历所有的gid，加入到gids中
				gids = append(gids, k)
			}
			sort.Ints(gids) // 对gids进行排序
			temp := 0 // 表示当前已经分配了多少个shard
			for _, gid := range gids { // 遍历所有的gid
				if _, ok := gidsFlag[gid]; !ok { // 如果该gid没有分配过shard
					conf.Shards[emptyShards[temp]] = gid // 将该gid分配给一个没有分配shard的shard
					temp += 1 // 已经分配的shard数加一
				}
				if temp >= len(emptyShards) { // 已经分配的shard数大于等于没有分配shard的shard的数量，跳出循环
					break
				}
			}

		}
	}
}

/*
applych处理代码
*/

func (sc *ShardCtrler) handleJoinCommand(args JoinArgs) { // 处理Join命令
	conf := sc.getConfigByIndex(-1) // 获取最新的配置
	conf.Num += 1 // 配置的序号加一，因为有新的命令，需要生成新的配置

	//加入组
	for k, v := range args.Servers {
		conf.Groups[k] = v // 将新的server加入到Groups中，其中k是gid，v是server的地址
	}

	sc.adjustConfig(&conf) // 调整配置
	sc.configs = append(sc.configs, conf) // 将新的配置加入到configs中
}

func (sc *ShardCtrler) handleLeaveCommand(args LeaveArgs) {
	conf := sc.getConfigByIndex(-1)
	conf.Num += 1

	//删掉server，并重置分配的shard
	for _, gid := range args.GIDs { // 遍历所有要移除的server（要删除的不止一个），其中gid是要移除的server的id
		delete(conf.Groups, gid) // 将该server从Groups中删除
		for i, v := range conf.Shards { // 遍历所有的shard
			if v == gid { // 如果该shard分配给了该移除的gid，将该shard设置为0
				conf.Shards[i] = 0
			}
		}
	}

	sc.adjustConfig(&conf)
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleMoveCommand(args MoveArgs) {
	conf := sc.getConfigByIndex(-1) // 获取最新的配置
	conf.Num += 1 // 配置的序号加一，因为有新的命令，需要生成新的配置

	//移动shard
	conf.Shards[args.Shard] = args.GID // 将shard移动到指定的gid
	// 这里不需要调整配置，因为只是移动shard，不会改变配置
	sc.configs = append(sc.configs, conf) // 将新的配置加入到configs中
}

// 返回给客户端命令执行结果
func (sc *ShardCtrler) notifyWaitCommand(reqId int64, err Err, conf Config) {
	if ch, ok := sc.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:    err,
			Config: conf,
		}
	}
}

//处理applych
func (sc *ShardCtrler) handleApplyCh() {
	for {
		select {
		case <-sc.stopCh:
			DPrintf("get from stopCh,server-%v stop!", sc.me)
			return
		case cmd := <-sc.applyCh:
			//处理快照命令，读取快照的内容
			if cmd.SnapshotValid {
				continue
			}
			//处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			DPrintf("server %v start apply command %v：%+v", sc.me, cmdIdx, cmd.Command)
			op := cmd.Command.(Op)
			sc.lock("handleApplyCh")

			if op.Method == "Query" {
				//处理读
				conf := sc.getConfigByIndex(op.Args.(QueryArgs).Num) // 获取指定配置,当Num为-1时，获取最新的配置
				sc.notifyWaitCommand(op.ReqId, OK, conf) // 通知命令执行结果
			} else {
				//处理其他命令
				//判断命令是否重复
				isRepeated := false // 默认不重复
				if v, ok := sc.lastApplies[op.ClientId]; ok {
					if v == op.CommandId { // 检查最后一次执行的命令是否和当前命令相同，相同则重复
						isRepeated = true
					}
				}
				if !isRepeated { // 如果不重复，执行命令
					switch op.Method {
					case "Join":
						sc.handleJoinCommand(op.Args.(JoinArgs))
					case "Leave":
						sc.handleLeaveCommand(op.Args.(LeaveArgs))
					case "Move":
						sc.handleMoveCommand(op.Args.(MoveArgs))
					default:
						panic("unknown method")
					}
				}
				sc.lastApplies[op.ClientId] = op.CommandId // 更新最后一次执行的命令
				sc.notifyWaitCommand(op.ReqId, OK, Config{}) // 通知命令执行结果
			}

			DPrintf("apply op: cmdId:%d, op: %+v", cmdIdx, op)
			sc.unlock("handleApplyCh")
		}
	}
}

/*
初始化代码
*/

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.stopCh = make(chan struct{})
	sc.commandNotifyCh = make(map[int64]chan CommandResult)
	sc.lastApplies = make(map[int64]int64)

	go sc.handleApplyCh()

	return sc
}
