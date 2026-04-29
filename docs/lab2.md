# lab2 Raft


- [思路](#思路)
- [实现](#实现)
    - [结构体](#结构体)
    - [lab2A 领导者选举](#lab2A-领导者选举)
        - [服务器状态](#服务器状态)
        - [启动](#启动)
        - [选举与投票](#选举与投票)
    - [lab2B 日志复制](#lab2B-日志复制)
        - [handler](#handler)
        - [sender](#sender)
        - [日志同步](#日志同步)
        - [复制模型](#复制模型)
        - [完善选举](#完善选举)
    - [lab2C 持久化](#lab2C-持久化)
    - [lab2D 日志压缩](#lab2D-日志压缩)
        - [服务端触发的日志压缩](#服务端触发的日志压缩)
        - [leader 发送来的 InstallSnapshot](#leader-发送来的-installsnapshot)
        - [异步 applier 的 exactly once](#异步-applier-的-exactly-once)

      
## 思路

raft的结构体按照论文的图2进行设计。
![](https://user-images.githubusercontent.com/32640567/116203223-0bbb5680-a76e-11eb-8ccd-4ef3f1006fb3.png)

raft的功能实现参考lab2的指导手册 [guidance](https://thesquareplanet.com/blog/students-guide-to-raft/)。

## 实现

### 结构体

raft的结构体设计如下：

```go
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2A
	state          NodeState   //节点状态
	currentTerm    int         //当前任期
	votedFor       int         //给谁投过票
	electionTimer  *time.Timer //选举时间
	heartbeatTimer *time.Timer //心跳时间

	//2B
	logs        []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		//2A
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		//2B
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
	}
	//rf.peers = peers
	//rf.persister = persister
	//rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// After restoring persisted logs/snapshot dummy entry, align volatile
	// indexes so applier/replicator won't use stale lastApplied/commitIndex
	// that refer to compacted entries. Use the first log's Index (the
	// dummy entry) as the base (this may be the lastIncludedIndex).
	first := rf.getFirstLog().Index
	if rf.commitIndex < first {
		rf.commitIndex = first
	}
	if rf.lastApplied < first {
		rf.lastApplied = first
	}

	//2A
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})

			go rf.replicator(i)  // start replicator goroutine to replicate logs to each follower, 共len(peer)-1个，用来向每个 follower 复制日志
		}
	}
	// start ticker goroutine to start elections， 用来触发 heartbeat timeout 和 election timeout
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once， ，用来往 applyCh 中 push 提交的日志并保证 exactly once
	go rf.applier()
	return rf
}
```

### lab2A 领导者选举

#### 服务器状态
- **Leader**：处理所有客户端请求、日志同步、心跳维持领导权。同一时刻最多只能有一个可行的 Leader
- **Follower**：所有服务器的初始状态，功能为：追随领导者，接收领导者日志并实时同步，特性：完全被动的（不发送 RPC，只响应收到的 RPC）
- **Candidate**：用来选举新的 Leader，处于 Leader 和 Follower 之间的暂时状态，如Follower 一定时间内未收到来自Leader的心跳包，Follower会自动切换为Candidate，并开始选举操作，向集群中的其它节点发送投票请求，待收到半数以上的选票时，协调者升级成为领导者。

系统正常运行时，只有一个 Leader，其余都是 Followers。Leader拥有绝对的领导力，不断向Followers同步日志且发送心跳状态。

如果 Leader 想要保持权威，必须向集群中的其它节点发送心跳包（空的 `AppendEntries` RPC）。

#### 启动

1. 集群所有节点初始状态均为Follower
2. Follower 被动地接受 Leader 或 Candidate 的 RPC；
3. 所以，如果 Leader 想要保持权威，必须向集群中的其它节点发送心跳包（空的 AppendEntries RPC）；
4. 等待选举超时(electionTimeout，一般在 100~500ms)后，Follower 没有收到任何 RPC：
   * Follower 认为集群中没有 Leader
   * 开始新的一轮选举

集群开始的时候，所有节点均为Follower， 它们依靠ticker()成为Candidate。`ticker` 协程会定期收到两个 timer 的到期事件，如果是 `election timer` 到期，则发起一轮选举；如果是 `heartbeat timer` 到期且节点是 leader，则发起一轮心跳。

#### 选举与投票
基本参照图 2 的描述实现。需要注意的是只有在 grant 投票时才重置选举超时时间，这样有助于网络不稳定条件下选主的 liveness 问题。

当一个节点开始竞选：
1. 增加自己的 `currentTerm`
2. 转为 Candidate 状态，其目标是获取超过半数节点的选票
3. 让自己成为 Leader,先给自己投一票
4. 并行地向集群中其它节点发送 `RequestVote` RPC 索要选票，如果没有收到指定节点的响应，它会反复尝试，直到发生以下三种情况之一：
   * 获得超过半数的选票：成为 Leader，并向其它节点发送 `AppendEntries` 心跳；
   * 收到来自 Leader 的 RPC：转为 Follower；
   * 其它两种情况都没发生，没人能够获胜(`electionTimeout` 已过)：增加 `currentTerm`，开始新一轮选举；



领导者选举主要工作可总结如下：

* 三个状态，三个状态之间的转换。
* 1个loop——ticker。
* 1个RPC请求和处理，用于投票。

需要注意：
* 并行异步投票：发起投票时要异步并行去发起投票，从而不阻塞 `ticker` 协程，这样 candidator 再次 `election timeout` 之后才能自增 term 继续发起新一轮选举。
* 投票统计：在函数内定义一个变量`grantedVotes`并利用 go 的闭包来实现.
* 抛弃过期请求的回复：对于过期请求的回复，直接抛弃就行，不要做任何处理。
* 节点随机选择超时时间，通常在 [T, 2T] 之间（T = electionTimeout）
  这样，节点不太可能再同时开始竞选，先竞选的节点有足够的时间来索要其他节点的选票
  T >> broadcast time(T 远大于广播时间)时效果更佳

### lab2B 日志复制

#### handler
按照图 2 中的描述实现，主要是根据请求中的 term 来判断是否过期，如果过期直接回复当前 term；如果不过期则重置选举超时时间，并且根据日志的一致性检查来判断是否接受该日志。

#### sender
对于 follower 的复制，有 `InstallSnapshot` 和 `AppendEntries` 两种方式，需要根据该 peer 的 nextIndex 来判断。

需要注意:
* 锁的使用：发送 rpc，接收 rpc，push channel，receive channel 的时候一定不要持锁，否则很可能产生死锁 对于仅读的代码块可以短暂的用`RLock`。
* 抛弃过期请求的回复：对于过期请求的回复，直接抛弃就行，不要做任何处理。
* commit 日志：图 2 中规定，raft leader 只能提交当前 term 的日志，不能提交旧 term 的日志。因此 leader 根据 matchIndex[] 来 commit 日志时需要判断该日志的 term 是否等于 leader 当前的 term，即是否为当前 leader 任期新产生的日志，若是才可以提交。此外，follower 对 leader
  的 leaderCommit 就不需要判断了，无条件服从即可。

#### 日志同步
运行流程
1. 客户端向 Leader 发送命令，希望该命令被所有状态机执行；
2. Leader 先将该命令追加到自己的日志中；
3. Leader 并行地向其它节点发送 AppendEntries RPC，等待响应；
4. 收到超过半数节点的响应，则认为新的日志记录是被提交的：
5. * Leader 将命令传给自己的状态机，然后向客户端返回响应
   * 此外，一旦 Leader 知道一条记录被提交了，将在后续的 AppendEntries RPC 中通知已经提交记录的 Followers
   * Follower 将已提交的命令传给自己的状态机
6. 如果 Follower 宕机/超时：Leader 将反复尝试发送 RPC；
7. 性能优化：Leader 不必等待每个 Follower 做出响应，只需要超过半数的成功响应（确保日志记录已经存储在超过半数的节点上）——一个很慢的节点不会使系统变慢，因为 Leader 不必等他；

需要注意：

改造AppendEntries以及其请求体和返回体，添加`ConflictIndex`和`ConflictTerm`字段来实现快速失败和日志回退的功能。
1. 若 follower 没有 `prevLogIndex` 处的日志，则直接置 `conflictIndex` = len(log)，`conflictTerm` = None；
     * leader 收到返回体后，肯定找不到对应的 term，则设置`nextIndex` = `conflictIndex`；
     * 其实就是 leader 对应的 nextIndex 直接回退到该 follower 的日志条目末尾处，因为 `prevLogIndex` 超前了
2. 若 follower 有 `prevLogIndex` 处的日志，但是 term 不匹配；则设置 `conlictTerm`为 `prevLogIndex` 处的 term，且肯定可以找到日志中该 term出现的第一个日志条目的下标，并置`conflictIndex = firstIndexWithTerm`；
     * leader 收到返回体后，有可能找不到对应的 term，即 leader 和 follower 在`conflictIndex`处以及之后的日志都有冲突，都不能要了，直接置`nextIndex = conflictIndex`
     * 若找到了对应的term，则找到对应term出现的最后一个日志条目的下一个日志条目，即置`nextIndex` = `lastIndexWithTerm+1`；这里其实是默认了若 leader 和 follower 同时拥有该 term 的日志，则不会有冲突，直接取下一个 term 作为日志发起就好，是源自于 safety 的安全性保证。

如果还有冲突，leader 和 follower 会一直根据以上规则回溯 `nextIndex`。


#### 复制模型
对于复制模型，参考了 `sofajraft` 的[日志复制实现](https://mp.weixin.qq.com/s/jzqhLptmgcNix6xYWYL01Q) 。

每个 peer 在启动时会为除自己之外的每个 peer 都分配一个 replicator 协程。对于 follower 节点，该协程利用条件变量执行 wait 来避免耗费 cpu，并等待变成 leader 时再被唤醒；对于 leader 节点，该协程负责尽最大地努力去向对应 follower 发送日志使其同步，直到该节点不再是 leader 或者该 follower 节点的 matchIndex 大于等于本地的 lastIndex。

这样的实现方式能够将日志同步的触发和上层服务提交新指令解耦，能够大幅度减少传输的数据量，rpc 次数和系统调用次数。当然，这样的实现也只是实现了粗粒度的 batching，并没有流量控制，而且也没有实现 pipeline。

此外，虽然 leader 对于每一个节点都有一个 replicator 协程去同步日志，但其目前同时最多只能发送一个 rpc，而这个 rpc 很可能超时或丢失从而触发集群换主。因此对于 heartbeat timeout 触发的 BroadcastHeartbeat，我们需要立即发出日志同步请求而不是让 replicator 去发。

#### 完善选举
无论谁赢得选举，可以确保 Leader 和超过半数投票给它的节点中拥有最完整的日志——最完整的意思就是 index 和 term 这对唯一标识是最大的。
为了保证日志「更加完善」的节点能够当选领导者，因此选票会向日志完善的节点倾斜，这被称为isLogUpToDate条件。

### lab2C 持久化
* raft 的持久化主要是为了在节点 crash 之后能够恢复之前的状态。raft 需要持久化的状态包括`currentTerm`、已经投过票的 candidate 的 id的`voteFor`、日志条目 `logs`等。

* `currentTerm`, `voteFor` 和 `logs` 这三个变量一旦发生变化就一定要在被其他协程感知到之前（释放锁之前，发送 rpc 之前）持久化，这样才能保证原子性。

### lab2D 日志压缩

#### 服务端触发的日志压缩
当服务端的日志过多时，leader 会触发日志压缩。leader 会将当前的日志快照化，并且将快照发送给 follower 来进行日志压缩。

#### leader 发送来的 InstallSnapshot
* 当 follower 收到 leader 发送来的 `InstallSnapshot` rpc 时，需要判断 term 是否正确，如果无误则 follower 把请求上报给上层应用，然后上层应用调用rf.CondInstallSnapshot()来决定是否安装快照。

* 此外如果快照中的 `lastIncludedIndex` 比本地的 `commitIndex` 还要小，说明该快照已经过时了，直接回复当前 term 就行；如果快照中的 `lastIncludedIndex` 比本地的 `commitIndex` 大，说明该快照是有效的，应该安装该快照，并且重置选举超时时间。

* 对于更新的 snapshot，这里通过异步的方式将其 push 到 applyCh 中。

#### 异步 applier 的 exactly once
* 对于日志的 apply，我们通过一个单独的 applier 协程来实现。applier 协程会定期检查是否有新的日志被 commit，如果有则将其 push 到 applyCh 中。

* 为了保证 exactly once，我们在 push 之前会先检查该日志是否已经被 apply 过了，如果已经被 apply 过了就不再 push 了。简而言之 applier 在发送每个 entry 前再次读取当前 `firstIndex（RLock）`并 skip 已被 snapshot 吞掉的 entry。

* 并且 applier 的实现可以使得日志 apply 到状态机和 raft 提交新日志可以真正的并行。

需要注意：
* 引用之前的 `commitIndex`：push applyCh 结束之后更新 `lastApplied` 的时候一定得用之前的 `commitIndex` 而不是 `rf.commitIndex`，因为后者很可能在 push channel 期间发生了改变。

* 防止与 installSnapshot 并发导致 lastApplied 回退：需要注意到，applier 协程在 push channel 时，中间可能夹杂有 snapshot 也在 push channel。 如果该 snapshot 有效，那么在 CondInstallSnapshot 函数里上层状态机和 raft 模块就会原子性的发生替换，即上层状态机更新为 snapshot 的状态，raft 模块更新 log, commitIndex, lastApplied 等等，此时如果这个 snapshot 之后还有一批旧的 entry 在 push channel，那上层服务需要能够知道这些 entry 已经过时，不能再 apply，同时 applier 这里也应该加一个 Max 自身的函数来防止 lastApplied 出现回退。

