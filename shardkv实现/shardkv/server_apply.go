package shardkv

import (
	"6.824/shardctrler"
)

// 封装结果并发到通知通道中
func (kv *ShardKV) notifyWaitCommand(reqId int64, err Err, value string) {
	if ch, ok := kv.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:   err,
			Value: value,
		}
	}
}

// 根据Key查询对应的value
func (kv *ShardKV) getValueByKey(key string) (err Err, value string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK // 如果成功查询到，则返回错误码为OK
		value = v // 赋值
	} else {
		err = ErrNoKey // 否则表示无该键值，返回空字符串以及不存在key的错误码
		value = ""
	}
	return
}

//判断能否执行客户端发来的命令
func (kv *ShardKV) ProcessKeyReady(configNum int, key string) Err {
	//config不对
	if configNum == 0 || configNum != kv.config.Num {
		kv.log("process key ready err config.")
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	//没有分配该shard
	if _, ok := kv.meShards[shardId]; !ok {
		kv.log("process key ready err shard.")
		return ErrWrongGroup
	}
	//正在迁移，这里有优化的空间，如果没有迁移完成，可以直接请求目标节点完成操作并返回，但是这样就太复杂了，这里简略了
	if _, ok := kv.inputShards[shardId]; ok {
		kv.log("process key ready err waitShard.")
		return ErrWrongGroup
	}
	return OK
}

//应用每一条命令
func (kv *ShardKV) handleApplyCh() {
	for {
		select { 
		case <-kv.stopCh: // 当有停止消息时，返回
			kv.log("get from stopCh,server-%v stop!", kv.me)
			return
		case cmd := <-kv.applyCh: // 当应用通道中有内容发来时，进行处理
			//处理快照命令，读取快照的内容
			if cmd.SnapshotValid {
				kv.log("%v get install sn,%v %v", kv.me, cmd.SnapshotIndex, cmd.SnapshotTerm)
				kv.lock("waitApplyCh_sn")
				kv.readPersist(false, cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot) // 读取持久化内容
				kv.unlock("waitApplyCh_sn")
				continue
			}
			//处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			//处理不同的命令
			if op, ok := cmd.Command.(Op); ok {
				kv.handleOpCommand(cmdIdx, op)
			} else if config, ok := cmd.Command.(shardctrler.Config); ok {
				kv.handleConfigCommand(cmdIdx, config)
			} else if mergeData, ok := cmd.Command.(MergeShardData); ok {
				kv.handleMergeShardDataCommand(cmdIdx, mergeData)
			} else if cleanData, ok := cmd.Command.(CleanShardDataArgs); ok {
				kv.handleCleanShardDataCommand(cmdIdx, cleanData)
			} else {
				panic("apply command,NOT FOUND COMMDN！")
			}

		}

	}

}

//处理get、put、append命令
func (kv *ShardKV) handleOpCommand(cmdIdx int, op Op) {
	kv.log("start apply command %v：%+v", cmdIdx, op)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	shardId := key2shard(op.Key) // 获取这个key对应在哪个shard上
	// 调用ProcessKeyReady函数，判断能否处理这个命令，如果不能处理，把这个err直接赋值给错误码，封装好直接返回
	if err := kv.ProcessKeyReady(op.ConfigNum, op.Key); err != OK {
		kv.notifyWaitCommand(op.ReqId, err, "")
		return
	}
	if op.Method == "Get" { // 若能处理，如果类型为Get
		//处理读
		e, v := kv.getValueByKey(op.Key) // 调用根据键获得值的方法
		kv.notifyWaitCommand(op.ReqId, e, v) // 封装好直接返回
	} else if op.Method == "Put" || op.Method == "Append" { 
		//处理写
		//判断命令是否重复
		isRepeated := false // 默认不重复
		if v, ok := kv.lastApplies[shardId][op.ClientId]; ok {
			if v == op.CommandId { // 根据lastapplied判断是否重复
				isRepeated = true
			}
		}

		if !isRepeated { // 不重复就根据是Put还是append来进行分别操作
			switch op.Method {
			case "Put":
				kv.data[shardId][op.Key] = op.Value // 更新data数据
				kv.lastApplies[shardId][op.ClientId] = op.CommandId // 更新lastapplied
			case "Append":
				e, v := kv.getValueByKey(op.Key) // 先看看这个键是否存在，不存在就按照put处理
				if e == ErrNoKey {
					//按put处理
					kv.data[shardId][op.Key] = op.Value
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				} else {
					kv.data[shardId][op.Key] = v + op.Value // 存在就是在原来基础上进行追加
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				}
			default:
				panic("unknown method " + op.Method)
			}

		}
		//命令处理成功
		kv.notifyWaitCommand(op.ReqId, OK, "") // put和append函数处理完，封装结果放入通知通道
	} else {
		panic("unknown method " + op.Method)
	}

	kv.log("apply op: cmdId:%d, op: %+v, data:%v", cmdIdx, op, kv.data[shardId][op.Key])
	//每应用一条命令，就判断是否进行持久化
	kv.saveSnapshot(cmdIdx)
}

//处理config命令，即更新config
//主要是处理meshard、inputshard、outputshard
func (kv *ShardKV) handleConfigCommand(cmdIdx int, config shardctrler.Config) {
	kv.log("start handle config %v：%+v", cmdIdx, config)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	// 如果config的num小于等于当前的config的num，则不处理
	if config.Num <= kv.config.Num {
		kv.saveSnapshot(cmdIdx)
		return
	}
	// 如果config的读取有跳跃，则不处理
	if config.Num != kv.config.Num+1 {
		panic("applyConfig err")
	}

	oldConfig := kv.config.Copy() // 保存旧的config
	outputShards := make([]int, 0, shardctrler.NShards) // 保存当前config中移出的shard
	inputShards := make([]int, 0, shardctrler.NShards) // 保存当前config中移入的shard
	meShards := make([]int, 0, shardctrler.NShards)	// 保存当前config中属于自己的shard

	for i := 0; i < shardctrler.NShards; i++ { // 遍历所有的shard
		if config.Shards[i] == kv.gid { // 如果当前shard属于自己
			meShards = append(meShards, i) // 将其加入到meShards中
			if oldConfig.Shards[i] != kv.gid { // 如果旧的config中该shard不属于自己，则表示是新分配的shard
				inputShards = append(inputShards, i) // 将其加入到inputShards中
			}
		} else {
			if oldConfig.Shards[i] == kv.gid { // 如果旧的config中该shard属于自己，则表示是移出的shard
				outputShards = append(outputShards, i) // 将其加入到outputShards中
			}
		}
	}

	//处理当前的shard
	kv.meShards = make(map[int]bool) // 新建一个map，保存当前属于自己的shard
	for _, shardId := range meShards { // 遍历meShards，将其中的shard加入到meShards中
		kv.meShards[shardId] = true
	}

	//处理移出的shard
	//保存当前所处配置的所有移除的shard数据
	d := make(map[int]MergeShardData) // 新建一个map，保存当前config的所有移出的shard数据
	for _, shardId := range outputShards { // 遍历outputShards
		mergeShardData := MergeShardData{ // 封装数据
			ConfigNum:      oldConfig.Num, // 旧的config的num
			ShardNum:       shardId, // shard的id
			Data:           kv.data[shardId], // shard的数据
			CommandIndexes: kv.lastApplies[shardId], // shard的lastapplied
		}
		d[shardId] = mergeShardData // 将数据保存到d中
		// 因为shard已经移出，所以需要将其数据清空
		kv.data[shardId] = make(map[string]string) // 将shard的数据清空
		kv.lastApplies[shardId] = make(map[int64]int64) // 将shard的lastapplied清空
	}
	kv.outputShards[oldConfig.Num] = d // 将当前config的所有移出的shard数据保存到outputShards中

	//处理移入的shard
	kv.inputShards = make(map[int]bool) // 新建一个map，保存当前移入的shard
	if oldConfig.Num != 0 { // 如果旧的config的num不为0
		for _, shardId := range inputShards { // 遍历inputShards
			kv.inputShards[shardId] = true // 设置移入的shard为自身管理的shard
			// 这里的inputShards是当前config中移入的shard，但是这些shard的数据还没有迁移过来，所以需要向其他节点发送请求，让其将数据迁移过来
		}
	}

	kv.config = config // 更新配置
	kv.oldConfig = oldConfig
	kv.log("apply op: cmdId:%d, config:%+v", cmdIdx, config)
	//每应用一条命令，就判断是否进行持久化
	kv.saveSnapshot(cmdIdx)
}

//处理新的shard数据，即input shard，这里的cmdIdx指的是config命令的cmdIdx
func (kv *ShardKV) handleMergeShardDataCommand(cmdIdx int, data MergeShardData) {
	kv.log("start merge Shard Data %v：%+v", cmdIdx, data)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	// 如果config的num不是当前数据的config的num+1，即该data更新的config不是当前的config，则不处理
	if kv.config.Num != data.ConfigNum+1 {
		return
	}
	// 如果当前的shard不是inputShard，则不处理
	if _, ok := kv.inputShards[data.ShardNum]; !ok {
		return
	}
	// 给新加进来的shard分配空间
	kv.data[data.ShardNum] = make(map[string]string)
	kv.lastApplies[data.ShardNum] = make(map[int64]int64)
	// 将数据保存到新加进来的shard中
	for k, v := range data.Data {
		kv.data[data.ShardNum][k] = v
	}
	// 将lastapplied保存到新加进来的shard中
	for k, v := range data.CommandIndexes {
		kv.lastApplies[data.ShardNum][k] = v
	}
	// 将新加进来的shard从inputShard中删除
	delete(kv.inputShards, data.ShardNum)

	kv.log("apply op: cmdId:%d, mergeShardData:%+v", cmdIdx, data)
	// 每应用一条命令，就判断是否进行持久化
	kv.saveSnapshot(cmdIdx) // 这里的cmdIdx指的是config命令的cmdIdx，所以这里的saveSnapshot是保存config命令的快照
	// 调用callPeerCleanShardData函数，通知其他节点清除该shard的数据
	go kv.callPeerCleanShardData(kv.oldConfig, data.ShardNum)
}

//处理已经迁移走的shard，即output shard
func (kv *ShardKV) handleCleanShardDataCommand(cmdIdx int, data CleanShardDataArgs) {
	kv.log("start clean shard data %v：%+v", cmdIdx, data)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	//如果要清除的shard确实是在outputShard中，且没有被清除，则需要清除
	if kv.OutputDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.outputShards[data.ConfigNum], data.ShardNum)
	}

	//通知等待协程
	//if ch, ok := kv.cleanOutputDataNotifyCh[fmt.Sprintf("%d%d", data.ConfigNum, data.ShardNum)]; ok {
	//	ch <- struct{}{}
	//}

	kv.saveSnapshot(cmdIdx)
}
