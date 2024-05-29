package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

func init() {
	rand.Seed(150)
}

type Role int

const (
	Role_Follower  = 0 //对角色进行编号
	Role_Candidate = 1
	Role_Leader    = 2
)

const (
	ElectionTimeout   = time.Millisecond * 300 // 选举超时时间/心跳超时时间,300毫秒
	HeartBeatInterval = time.Millisecond * 150 // leader 发送心跳的时间间隔
	ApplyInterval     = time.Millisecond * 100 // apply log 日志应用间隔,每隔一段时间应用一次日志,将已提交但未应用的日志项应用到状态机中
	RPCTimeout        = time.Millisecond * 100 // 在进行远程过程调用(RPC)时等待对方响应的最大时间。
	MaxLockTime       = time.Millisecond * 10 // debug
)

type ApplyMsg struct {
	CommandValid bool // 判断该追加日志条目是否已提交
	Command      interface{}
	CommandIndex int

	// For 2D:接收到leader发来的快照,就要发送一条快照命令
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int // 日志的任期
	Command interface{} // 要追加的命令
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers,每一个clientEnd对应了向该peer通信的端点
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role // 状态编号,0为Follower,1为Candidate,2为Leader
	currentTerm int // 当前任期
	votedFor    int // 选票给了谁
	logs        []LogEntry // 日志
	commitIndex int // 大多数结点都已提交的日志index
	lastApplied int // 已经被运用到状态机的日志index,和上面都是取最高的index
	nextIndex   []int // 用于leader结点维护的与每个follower结点相关的索引,用于日志复制
	matchIndex  []int

	electionTimer       *time.Timer // 选举定时器,用于触发节点发起选举。
	appendEntriesTimers []*time.Timer // 附加日志定时器,用于 Leader 发送心跳和附加日志的定时任务。
	applyTimer          *time.Timer // 应用定时器,用于触发应用日志到状态机。
	applyCh             chan ApplyMsg //这个chan是用来提交应用的日志,具体的处理在config.go文件中
	notifyApplyCh       chan struct{} //用于通知应用层有新的已提交的日志可供应用。
	stopCh              chan struct{} // 用于通知 Raft 节点停止的通道

	lastSnapshotIndex int // 快照中最后一条日志的index,是真正的index,不是存储在logs中的index
	lastSnapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.


// 获取结点状态信息,判断该结点是不是leader,并且得到其任期
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	flag := false
	if rf.role == Role_Leader {
		flag = true
	}
	return rf.currentTerm, flag
}

// 获取一个随机的超时时间,范围是一倍到2倍的基础超时时间。
func (rf *Raft) getElectionTimeout() time.Duration {
	t := ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
	return t
}

//重置计时器,设置时间为上面函数得到的超时时间
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.getElectionTimeout())
}

// 返回当前状态机的最后一条日志的任期和索引
// 返回值是最后一条日志的任期和索引。注意,索引是由当前日志队列长度和最后快照的索引计算而来。
// 索引是一直会增大的,但是我们的日志队列却不可能无限增大,在队列中下标0存储快照
func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	return rf.logs[len(rf.logs)-1].Term, rf.lastSnapshotIndex + len(rf.logs) - 1
}


//结点身份改变时需要做的事情
func (rf *Raft) changeRole(newRole Role) {
	if newRole < 0 || newRole > 3 {
		panic("unknown role")
	}
	rf.role = newRole
	switch newRole {
// 跟随者不需要做什么,大多数结点都是跟随者,操作量都很低,减小工作量
	case Role_Follower:
// 当变为候选人时,自身term+1,给自己投票,重置倒计时
	case Role_Candidate:
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetElectionTimer()
// 当变为领导者后,得到当前的最新的index
// 对其他结点的要发送的日志index更新为leader自身最新命令的index+1,已匹配的index也改为当前的自己最高index
// 重新开始计时
	case Role_Leader:
		//leader有两个特殊的数据结构:nextIndex和matchIndex
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = lastLogIndex
		}
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}

}

//获取持久化的数据
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}


// vote部分

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// 请求投票的候选结点的信息(结点的简历,用于给别的结点判断是否投票给它让它当leader)
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
// example RequestVote RPC reply structure. field names must start with capital letters!

// 响应,包含被请求结点的term,以及是否投票 返回term是让该候选结点知道其现在的任期是否过时,若过时就要从候选结点变为跟随者
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.

// 处理来自其它结点的投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock() // 在函数返回前一步执行,避免死锁发生

// 获取结点的信息,用于和发送请求的结点进行比较,判断能不能给它投票
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false // 默认不投票

	if rf.currentTerm > args.Term { // 任期比请求结点高,不予投票
		return
	} else if rf.currentTerm == args.Term { // 任期一致,继续判断
		if rf.role == Role_Leader { // 如果自己是Leader,也不投票(自己继续当Leader)
			return
		}
		if args.CandidateId == rf.votedFor { // 已经投过票给它了,可能它没收到,继续投票
			reply.Term = args.Term
			reply.VoteGranted = true
			return
		}
		if rf.votedFor != -1 && args.CandidateId != rf.votedFor { // 投过票给别人,就不能再投了
			return
		}
		//还有一种情况,没有投过票,下面再判断
	}
	if rf.currentTerm < args.Term { // 自己的term低,修改任期,转变身份,修改投票记录,投出票
		rf.currentTerm = args.Term
		rf.changeRole(Role_Follower)
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		rf.persist()
	}
	//判断日志完整性,如果候选结点的任期或者日志的index比自己低,也不投
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}
	rf.votedFor = args.CandidateId // 给请求结点投票
	reply.VoteGranted = true
	rf.changeRole(Role_Follower)
	rf.resetElectionTimer()
	rf.persist()
	//Dprintf("%v, role:%v,voteFor: %v", rf.me, rf.role, rf.votedFor)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

//向其它结点发送请求投票消息
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
//如果该结点不在要发的结点范围内,或者是要发给自己的话,就会出错
	if server < 0 || server > len(rf.peers) || server == rf.me {
		panic("server invalid in sendRequestVote!")
	}
// 创建定时器
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()
	ch := make(chan bool, 1)
// 启动go rountine,进行RPC调用,成功则已,否则循环最多十次
	go func() {
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()
// 若在上面的循环中,ch收到消息,则表示成功,直接返回,对应下面的第二种情况
// 否则就是RPC调用超时,对应第一种情况
	select {
	case <-rpcTimer.C:
		//Dprintf("%v role: %v, send request vote to peer %v TIME OUT!!!", rf.me, rf.role, server)
		return
	case <-ch:
		return
	}
}


// 用于启动新一轮选举
func (rf *Raft) startElection() {
	rf.mu.Lock() // 加锁,保证互斥访问,不会发生冲突
	rf.resetElectionTimer() // 重置选举计时器,确保在选举过程中不会发生冲突
	if rf.role == Role_Leader {// 检查自身状态,如果自己本身就是leader,就没必要推翻自己,重新开始选举了
		rf.mu.Unlock()
		return
	}
	rf.changeRole(Role_Candidate)// 否则,把自己的角色修改成候选人
	//Dprintf("%v role %v,start election,term: %v", rf.me, rf.role, rf.currentTerm)

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()// 获取自身信息,要写简历啦
	args := RequestVoteArgs{ // 写简历,用于发送投票请求
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	rf.persist()
	rf.mu.Unlock()
	allCount := len(rf.peers) // 开始给投票的结点发简历
	grantedCount := 1 // 自己给自己投一票,提振信心
	resCount := 1 // 记录已经收到的投票数,包括同意和不同意
	grantedChan := make(chan bool, len(rf.peers)-1)
	for i := 0; i < allCount; i++ {//对每一个其它节点都要发送rpc
		if i == rf.me {
			continue
		}	
		go func(gch chan bool, index int) {
			reply := RequestVoteReply{} // 创建响应变量,用于接收投票响应
			rf.sendRequestVote(index, &args, &reply)
			gch <- reply.VoteGranted
			if reply.Term > args.Term { // 当其它结点的任期比自己高,自觉放弃选举,改变状态为followe
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {//放弃选举
					rf.currentTerm = reply.Term
					rf.changeRole(Role_Follower)
					rf.votedFor = -1
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(grantedChan, i)
	}
	for rf.role == Role_Candidate {
		flag := <-grantedChan
		resCount++ // 收到票
		if flag { // 收到同意票 grantedCount++ 
			grantedCount++ 
		}
		//Dprintf("vote: %v, allCount: %v, resCount: %v, grantedCount: %v", flag, allCount, resCount, grantedCount)
		if grantedCount > allCount/2 { //收到同意票数超一半,竞选成功
			rf.mu.Lock()
			//Dprintf("before try change to leader,count:%d, args:%+v, currentTerm: %v, argsTerm: %v", grantedCount, args, rf.currentTerm, args.Term)
			if rf.role == Role_Candidate && rf.currentTerm == args.Term {//改变状态为leader
				rf.changeRole(Role_Leader)
			}
			if rf.role == Role_Leader {//立即触发心跳
				rf.resetAppendEntriesTimersZero()
			}
			rf.persist()
			rf.mu.Unlock()
			//Dprintf("%v current role: %v", rf.me, rf.role)
		} else if resCount == allCount || resCount-grantedCount > allCount/2 {// 竞选失败
			//Dprintf("grant fail! grantedCount <= len/2:count:%d", grantedCount)
			return
		}
	}
}



// appendentries部分

// 2B部分需要用到的结构体,用于日志复制
type AppendEntriesArgs struct {
	Term         int // 发送方当前任期号
	LeaderId     int // 当前leader的ID
	PrevLogIndex int // leader的前一条日志索引(即要追加的日志的前一条日志索引)
	PrevLogTerm  int // 与上面同理,前一条目的任期
	Entries      []LogEntry // 追加的日志
	LeaderCommit int // leader的提交日志索引
}

type AppendEntriesReply struct {
	Term         int // 接收方当前任期
	Success      bool // 接收方是否接受这一追加日志请求
	NextLogTerm  int // 接收方期望下一条日志的任期号
	NextLogIndex int // 与上面一样,但是索引
}

//立马发送,leader收到新command,重置对所有的结点的追加日志定时器,立马向各结点发送追加日志命令
func (rf *Raft) resetAppendEntriesTimersZero() {
	for _, timer := range rf.appendEntriesTimers {
		timer.Stop()
		timer.Reset(0)
	}
}

// leader发送完后,若某个结点并没有接收log的日志,需要对其再一次发送
func (rf *Raft) resetAppendEntriesTimerZero(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(0)
}

//重置单个timer,当发送完一个次RPC后,调用该函数,重置对该结点的下一次RPC发送间隔时间
func (rf *Raft) resetAppendEntriesTimer(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(HeartBeatInterval)
}

//判断当前raft的最后一条日志记录是否超过leader发送过来的最后一条日志记录
func (rf *Raft) isOutOfArgsAppendEntries(args *AppendEntriesArgs) bool {
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if lastLogTerm == args.Term && argsLastLogIndex < lastLogIndex {
		return true
	}
	return false
}

//获取当前存储位置的索引
func (rf *Raft) getStoreIndexByLogIndex(logIndex int) int {
	storeIndex := logIndex - rf.lastSnapshotIndex
	if storeIndex < 0 {
		return -1
	}
	return storeIndex
}

//接收端处理rpc,在lab2A中,仅需要判断term是否合格
//主要进行三个处理：
// 	1. 判断任期
// 	2. 判断是否接收数据,success:数据全部接受,或者没有数据
//	3. 判断是否提交数据
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	//Dprintf("%v receive a appendEntries: %+v", rf.me, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // 僭越term等级,不接收该追加
		rf.mu.Unlock()
		return
	}
// 否则更新自身的term,转变状态,重新开始选举倒计时(心跳)
	rf.currentTerm = args.Term
	rf.changeRole(Role_Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.getLastLogTermAndIndex()
	//先判断两边,再判断刚好从快照开始,再判断中间的情况
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		//1.要插入的前一个index小于快照index,几乎不会发生
		reply.Success = false
		reply.NextLogIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		//2. 要插入的前一个index大于最后一个log的index,说明中间还有log
		reply.Success = false
		reply.NextLogIndex = lastLogIndex + 1
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		//3. 要插入的前一个index刚好等于快照的index,说明可以全覆盖,但要判断是否是全覆盖
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0 //=0代表着插入会导致乱序
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else if args.PrevLogTerm == rf.logs[rf.getStoreIndexByLogIndex(args.PrevLogIndex)].Term {
		//4. 中间的情况：索引处的两个term相同,判断是否follower结点的长度比leader大,大则插入会导致乱序,拒绝插入
		if rf.isOutOfArgsAppendEntries(args) {
			reply.Success = false
			reply.NextLogIndex = 0
		} else {
			reply.Success = true
			rf.logs = append(rf.logs[:rf.getStoreIndexByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			_, currentLogIndex := rf.getLastLogTermAndIndex()
			reply.NextLogIndex = currentLogIndex + 1
		}
	} else {
		//5. 中间的情况：索引处的两个term不相同,跳过一个term,不断回退到到floower已有的该任期的最初的一个index,避免一步一步回退,浪费时间
		term := rf.logs[rf.getStoreIndexByLogIndex(args.PrevLogIndex)].Term
		index := args.PrevLogIndex
		for index > rf.commitIndex && index > rf.lastSnapshotIndex && rf.logs[rf.getStoreIndexByLogIndex(index)].Term == term {
			index--
		}
		reply.Success = false
		reply.NextLogIndex = index + 1
	}
// 追加成功,更新已提交日志索引,并发送通知进行应用
	if reply.Success {
		//Dprintf("%v current commit: %v, try to commit %v", rf.me, rf.commitIndex, args.LeaderCommit)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	//Dprintf("%v role: %v, get appendentries finish,args = %v,reply = %+v", rf.me, rf.role, *args, *reply)
	rf.mu.Unlock()

}

//获取要向指定节点发送的日志
func (rf *Raft) getAppendLogs(peerId int) (prevLogIndex int, prevLogTerm int, logEntries []LogEntry) {
	nextIndex := rf.nextIndex[peerId]
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
// 如果下一条信息的index小于快照所包含的最后一条日志index,或者大于最后一条日志index,则说明没有要发送的日志
	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastLogIndex {
		//没有要发送的log
		prevLogTerm = lastLogTerm
		prevLogIndex = lastLogIndex
		return
	}
//这里一定要进行深拷贝,不然会和Snapshot()发生数据上的冲突
	//logEntries = rf.logs[nextIndex-rf.lastSnapshotIndex:]
	logEntries = make([]LogEntry, lastLogIndex-nextIndex+1)
	copy(logEntries, rf.logs[nextIndex-rf.lastSnapshotIndex:])
	prevLogIndex = nextIndex - 1
// 之前已经排除了prevlogterm小于leader快照的term的情况,现在判断等于和大于的情况
// 大于的情况则按照对应长度在log中匹配即可
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[prevLogIndex-rf.lastSnapshotIndex].Term
	}
	return
}

//尝试去提交日志 会依次判断,可以提交多个,但不能有间断
func (rf *Raft) tryCommitLog() {
	_, lastLogIndex := rf.getLastLogTermAndIndex()
	hasCommit := false

	for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {
		count := 0
		for _, m := range rf.matchIndex {
// 一个节点的matchIndex大于等于当前日志的index,表明该节点已经接收了该日志
			if m >= i {
				count += 1
				//若提交数达到多数派,可以提交
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					//Dprintf("%v role: %v,commit index %v", rf.me, rf.role, i)
					break
				}
			}
		}
// 遍历的过程中,只要有一条日志没有达到多数派,后面的日志也可以不用计算了,节省时间成本
		if rf.commitIndex != i {
			break
		}
	}
// 若成功提交,则会发一个空结构体给应用提交通道,使其应用到状态机中
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

//发送端发送数据,用于日志追加,与之前的请求投票类似
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		//尝试发送日志追加10次
		for i := 0; i < 10 && !rf.killed(); i++ {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()
	select { // 若发送成功,则返回,否则RPC超时,也返回
	case <-rpcTimer.C:
		//Dprintf("%v role: %v, send append entries to peer %v TIME OUT!!!", rf.me, rf.role, server)
		return
	case <-ch:
		return
	}
}
// leader结点向follower结点发送追加日志请求,在lab2A部分中则是发送心跳
func (rf *Raft) sendAppendEntriesToPeer(peerId int) {
	if rf.killed() { // 若结点死亡,返回
		return
	}
	rf.mu.Lock()
// 判断是不是leader,若不是,则重置追加日志计时器,返回
	if rf.role != Role_Leader { // 只有leader才有资格给其他结点发送日志追加请求
		rf.resetAppendEntriesTimer(peerId)
		rf.mu.Unlock()
		return
	}
	//Dprintf("%v send append entries to peer %v", rf.me, peerId)
// 获取要发送给follower的信息,包括前一日志的信息和待追加的日志条目
	prevLogIndex, prevLogTerm, logEntries := rf.getAppendLogs(peerId)
// 封装信息准备发送
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{} // 接收响应
	rf.resetAppendEntriesTimer(peerId)
	rf.mu.Unlock()
// 发送消息
	rf.sendAppendEntries(peerId, &args, &reply)
	//Dprintf("%v role: %v, send append entries to peer finish,%v,args = %+v,reply = %+v", rf.me, rf.role, peerId, args, reply)
	rf.mu.Lock()
	if reply.Term > rf.currentTerm { // 如果收到的响应中其他结点的任期比自己大,修改身份,返回
		rf.changeRole(Role_Follower)
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		rf.mu.Unlock()
		return
	}
// 确保在这个过程中,leader身份没有改变并且还处于当前任期,否则返回
	if rf.role != Role_Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	//响应：成功了,即：发送的数据全部接收了,或者根本没有数据
	if reply.Success {
		if reply.NextLogIndex > rf.nextIndex[peerId] {// 根据返回期望的index更新自身下次发送和已匹配的index
			rf.nextIndex[peerId] = reply.NextLogIndex
			rf.matchIndex[peerId] = reply.NextLogIndex - 1
		}
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
			//每个leader只能提交自己任期的日志
			rf.tryCommitLog()
		}
		rf.persist()
		rf.mu.Unlock()
		return
	}
	//响应：失败了,此时要修改nextIndex或者不做处理
	if reply.NextLogIndex != 0 {
		if reply.NextLogIndex > rf.lastSnapshotIndex {
			rf.nextIndex[peerId] = reply.NextLogIndex
			//为了一致性,立马发送
			rf.resetAppendEntriesTimerZero(peerId)
		} else {
			//发送快照
			go rf.sendInstallSnapshotToPeer(peerId)
		}
		rf.mu.Unlock()
		return
	} else {
		//reply.NextLogIndex == 0,此时如果插入会导致乱序,不进行处理
	}
	rf.mu.Unlock()
	return
}


// 快照部分

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm || rf.role != Role_Follower {
		rf.changeRole(Role_Follower)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.resetElectionTimer()
		rf.persist()
	}

	//如果自身快照包含的最后一个日志>=leader快照包含的最后一个日志,就没必要接受了
	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		return
	}


	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	_, lastIndex := rf.getLastLogTermAndIndex()
	if lastIncludedIndex > lastIndex {
		rf.logs = make([]LogEntry, 1)
	} else {
		installLen := lastIncludedIndex - rf.lastSnapshotIndex
		rf.logs = rf.logs[installLen:]
		rf.logs[0].Command = nil
	}
	//0处是空日志，代表了快照日志的标记
	rf.logs[0].Term = lastIncludedTerm

	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	//保存快照和状态
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)

	/***********************************/

	//接收发来的快照，并提交一个命令处理
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

}

//向指定节点发送快照
func (rf *Raft) sendInstallSnapshotToPeer(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	timer := time.NewTimer(RPCTimeout)
	defer timer.Stop()
	//Dprintf("%v role: %v, send snapshot  to peer,%v,args = %+v,reply = %+v", rf.me, rf.role, server, args)

	for {
		timer.Stop()
		timer.Reset(RPCTimeout)

		ch := make(chan bool, 1)
		reply := &InstallSnapshotReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-rf.stopCh:
			return
		case <-timer.C:
			//Dprintf("%v role: %v, send snapshot to peer %v TIME OUT!!!", rf.me, rf.role, server)
			continue
		case ok := <-ch:
			if !ok {
				continue
			}
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != Role_Leader || args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.changeRole(Role_Follower)
			rf.currentTerm = reply.Term
			rf.resetElectionTimer()
			rf.persist()
			return
		}

		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[server] {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		return
	}
}



// save Raft's persistent state to stable storage,where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 保存持久化状态
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}
// restore previously persisted state.
//读取持久化数据
func (rf *Raft) readPersist(data []byte) {
// 如果未保存有数据,或者长度小于1,说明此结点可能是第一次开机
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm                                      int
		votedFor                                         int
		logs                                             []LogEntry
		commitIndex, lastSnapshotTerm, lastSnapshotIndex int
	)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil || d.Decode(&lastSnapshotIndex) != nil || d.Decode(&logs) != nil {
		log.Fatal("rf read persist err!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.logs = logs
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
//其实CondInstallSnapshot中的逻辑可以直接在InstallSnapshot中来完成,让CondInstallSnapshot成为一个空函数,这样可以减少锁的获取和释放
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	//installLen := lastIncludedIndex - rf.lastSnapshotIndex
	//if installLen >= len(rf.logs)-1 {
	//	rf.logs = make([]LogEntry, 1)
	//	rf.logs[0].Term = lastIncludedTerm
	//} else {
	//	rf.logs = rf.logs[installLen:]
	//}
	_, lastIndex := rf.getLastLogTermAndIndex()
	if lastIncludedIndex > lastIndex {
		rf.logs = make([]LogEntry, 1)
	} else {
		installLen := lastIncludedIndex - rf.lastSnapshotIndex
		rf.logs = rf.logs[installLen:]
		rf.logs[0].Command = nil
	}
	//0处是空日志,代表了快照日志的标记
	rf.logs[0].Term = lastIncludedTerm

	//其实接下来可以读入快照的数据进行同步,这里可以不写

	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	//保存快照和状态
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//生成一次快照,实现很简单,删除掉对应已经被压缩的 raft log 即可
//index是当前要压缩到的index,snapshot是已经帮我们压缩好的数据
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.lastSnapshotIndex
	if snapshotIndex >= index {
		//Dprintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	oldLastSnapshotIndex := rf.lastSnapshotIndex
	rf.lastSnapshotTerm = rf.logs[rf.getStoreIndexByLogIndex(index)].Term
	rf.lastSnapshotIndex = index
	//删掉index前的所有日志
	rf.logs = rf.logs[index-oldLastSnapshotIndex:]
	//0位置就是快照命令
	rf.logs[0].Term = rf.lastSnapshotTerm
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	//Dprintf("{Node %v}'s state is {role %v,term %v,commitIndex %v,lastApplied %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, snapshotIndex)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

//客户端请求,leader添加日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
// 如果该结点不是leader,就直接返回并且告知其不是leader
	if rf.role != Role_Leader {
		return index, term, isLeader
	}
// 在原本日志后添加条目
	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
//更新lastIndex,得到该日志条目的index并返回
	_, lastIndex := rf.getLastLogTermAndIndex()
	index = lastIndex
//更新自己的lastIndex
	rf.matchIndex[rf.me] = lastIndex
	rf.nextIndex[rf.me] = lastIndex + 1

	term = rf.currentTerm
	isLeader = true
	//Dprintf("%v start command%v %v:%+v", rf.me, rf.lastSnapshotIndex, lastIndex, command)
// 重置日志条目添加倒计时
	rf.resetAppendEntriesTimersZero()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.


// 使用 atomic.StoreInt32 将 rf.dead 标志位设置为1,表示 Raft 实例已经被终止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// 检查结点是否死亡,查看上面杀死命令的设置的值是否为1
func (rf *Raft) killed() bool {
	live_or_die := atomic.LoadInt32(&rf.dead)
	return live_or_die == 1
}



//处理要应用的日志,快照的命令比较特殊,不在这里提交
func (rf *Raft) startApplyLogs() {
	defer rf.applyTimer.Reset(ApplyInterval)
// 通过 defer 语句确保 applyTimer 定时器在函数退出时被重置,以确保定时任务得以继续。
	rf.mu.Lock()
	var msgs []ApplyMsg
	if rf.lastApplied < rf.lastSnapshotIndex {
		//此时要安装快照,命令在接收到快照时就发布过了,等待处理
		msgs = make([]ApplyMsg, 0)
		rf.mu.Unlock()
		rf.CondInstallSnapshot(rf.lastSnapshotTerm, rf.lastSnapshotIndex, rf.persister.snapshot)
		return
	} else if rf.commitIndex <= rf.lastApplied { //已经应用的日志索引超过最后的快照索引,不需要进行日志应用
		// snapShot 没有更新 commitidx
		msgs = make([]ApplyMsg, 0)
	} else {// 表明有新日志需要应用,构造ApplyMsg切片
		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.getStoreIndexByLogIndex(i)].Command,
				CommandIndex: i,
			})
		}
	}
	rf.mu.Unlock()
// 将新的日志逐个发给上层应用,同时更新状态机已经应用的最后一条日志的索引
	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.lastApplied = msg.CommandIndex
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.


func (rf *Raft) ticker() {
// 启动一个goroutine处理应用计时器和应用通知管道,
// 当应用计时器触发时,通过通知管道 notifyApplyCh 发送空结构体来通知 Raft 实例提交了新的日志记录。
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.applyTimer.C:
				rf.notifyApplyCh <- struct{}{}
			case <-rf.notifyApplyCh: //当有日志记录提交了,要进行应用
				rf.startApplyLogs()
			}
		}
	}()
//选举定时 启动一个 goroutine 处理选举计时器。当选举计时器触发时,通过 startElection 方法开始新一轮的领导者选举。
	go func() {
		for rf.killed() == false {

			// Your code here to check if a leader election should be started and to randomize sleeping time using time.Sleep().
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()
// leader发送日志定时 为每个非自身的节点创建一个 goroutine 处理 Leader 发送日志定时器。
// 当定时器触发时,通过 sendAppendEntriesToPeer 方法向对应的节点发送附加日志项请求
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(cur int) {
			for rf.killed() == false {
				select {
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[cur].C:
					rf.sendAppendEntriesToPeer(cur)
				}
			}
		}(i)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// make 函数用于Raft结构体的构造函数,用于创建并初始化Raft实例
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//Dprintf("make a raft,me: %v", me)
	rf := &Raft{} // 创建raft实例
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 初始化各个结点的基本属性
	rf.role = Role_Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1) //下标为0存储快照
	// initialize from state persisted before a crash
	rf.commitIndex = 0
	rf.lastApplied = 0
	//初始化用于追踪每个服务器的下一个日志索引和已匹配的最高日志索引
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//读取持久化数据
	rf.readPersist(persister.ReadRaftState())
	// 定义多个计时器,调用ticker函数
	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatInterval)
	}
	rf.applyTimer = time.NewTimer(ApplyInterval)
	//设置应用管道,应用通知管道和停止管道
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.stopCh = make(chan struct{})

	// start ticker goroutine to start elections
	rf.ticker()

	return rf
}











