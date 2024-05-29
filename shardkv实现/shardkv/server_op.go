package shardkv

import (
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId     int64 //用来标识commandNotify
	CommandId int64
	ClientId  int64
	Key       string
	Value     string
	Method    string
	ConfigNum int
}

type CommandResult struct {
	Err   Err
	Value string
}

// 用于移除commandNotifyCh中的channel，防止内存泄漏
func (kv *ShardKV) removeCh(reqId int64) {
	kv.lock("removeCh")
	if _, ok := kv.commandNotifyCh[reqId]; ok {
		delete(kv.commandNotifyCh, reqId)
	}
	kv.unlock("removeCh")
}

/*
Get和PutAppend RPC的处理
*/

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	res := kv.waitCommand(args.ClientId, args.CommandId, "Get", args.Key, "", args.ConfigNum)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	res := kv.waitCommand(args.ClientId, args.CommandId, args.Op, args.Key, args.Value, args.ConfigNum)
	reply.Err = res.Err
}

// 用于等待commandNotifyCh中的channel返回结果
func (kv *ShardKV) waitCommand(clientId int64, commandId int64, method, key, value string, configNum int) (res CommandResult) {
	kv.log("wait cmd start,clientId：%d,commandId: %d,method: %s,key-value:%s %s,configNum %d", clientId, commandId, method, key, value, configNum)
	op := Op{ // 封装一个Op结构体
		ReqId:     nrand(),
		ClientId:  clientId,
		CommandId: commandId,
		Method:    method,
		Key:       key,
		ConfigNum: configNum,
		Value:     value,
	}
	index, term, isLeader := kv.rf.Start(op) // 将op发送给raft,等到raft节点中的状态机均复制了该op后，会在applyCh中返回该op,状态机会执行该op，然后返回结果到commandNotifyCh的结果通知channel中
	if !isLeader { // 如果不是leader，则返回非leader错误
		res.Err = ErrWrongLeader
		kv.log("wait cmd NOT LEADER.")
		return
	}
	// 如果是leader，则等待commandNotifyCh中的channel返回结果
	kv.lock("waitCommand")
	ch := make(chan CommandResult, 1) // 创建一个结果通知channel
	kv.commandNotifyCh[op.ReqId] = ch // 将channel与op.ReqId绑定
	kv.unlock("waitCommand")
	kv.log("wait cmd notify,index: %v,term: %v,op: %+v", index, term, op)
	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select {
	case <-t.C: // 如果超时,返回超时错误
		res.Err = ErrTimeOut
	case res = <-ch: // 如果成功返回结果，将结果赋值给res
	case <-kv.stopCh:
		res.Err = ErrServer // 如果服务停止，则返回服务错误
	}

	kv.removeCh(op.ReqId) // 移除commandNotifyCh中的channel，防止内存泄漏
	kv.log("wait cmd end,Op: %+v.res：%+v", op, res)
	return

}
