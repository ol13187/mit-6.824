# lab1 MapReduce

## 规则

1. 如果worker没有在10s内返回结果，coordinator会认为worker挂掉了，需要重新分配任务。（至少一次or恰好执行一次）
2. `pg-*.txt`相当于输入，是需要被执行分布式word count的文件，而`mr-out*` 是输出结果。
3. 将`mr/coordinator.go`中的 `Done()`函数中ret改为true,这样`main/mrmaster.go`就能知道job完成，进程即可退出。
4. 在构造coordinator时，传入的`nReduce`代表了会有nReduce个worker来执行任务。
5. worker进程应把第k个reduce task的输出保存到文件`mr-out-k`。`mr-out-k`中每行都应该是调用一次Reduce函数的输出，应按照Go语言的 `%v %v`的格式生成。
6. coordinator的Done函数返回true到coordinator终止，而worker向coordinator发送 `call()`，若没回应也终止，最终整个mapreduce流程结束。
7. 当调用workers的`Worker()`时，worker会向coordinator发送RPC索要任务，收到需要处理的文件名后开始执行。
8. 对于map阶段后生成的中间文件(intermediate files)需要被命名为`mr-X-Y`，其中X为map的任务编号，Y为reduce的任务编号。
9. 读写中间文件可以参考lab文档，具体是`encoding/json`包。
10. map阶段的worker可以采用`ihash(key)%nReduce`来选取处理reduce的worker。
11. 使用临时文件来避免crash的情况。具体地，先用`ioutil.TempFile`创建一个临时文件，写入数据后再用`os.Rename`修改成所需的最终文件名。

## 运行流程

1. 为`MakeCoordinator()`传入要被处理的文件和预计处理的reduce worker的数量，并开启socket监听，以启动整个MapReduce程序。
2. workers调用`Worker()`RPC向coordinator索要任务，后者分配任务对应的文件，worker获取之并执行map。
3. coordinator把对应的任务分配出去并记录任务和worker的对应关系。同时开启一个goroutine计时，若超过10s没有返回结果，则认为worker挂掉了，重新分配任务。
4. worker调用mapf函数处理文件内容并保存到中间变量中，接着将中间变量写入到临时文件中并重命名为所需的中间文件。(用 `ihash(key)%nReduce`指定要处理的reduce worker) 向coordinator发送`call()`表示执行map任务完毕。
5. coordinator 接收到任务被完成的信号，将map阶段被完成的任务，按中间文件名通过RPC发送给对应reduce worker。
6. worker 执行reducef函数处理map阶段的中间文件，生成最终的结果文件，返回一个RPC表示执行完毕。
7. coordinator 等待所有reduce任务执行完毕后，执行`Done()`函数，结束进程。
8. worker 定时向coordinator发送`call()`，若没有回应则终止进程。

## 想法

1. 处于效率考虑，使用条件变量`sync.WaitGroup`来代替Sleep。
2. RPC的IDL的规范: 入参和返回结构体定义在`rpc.go`中，而具体的Service定义在`worker.go`中，实现在`coordinator.go`中。
3. 为什么不传入执行map的worker数量？ 因为当我们开启coordinator进程后，并不依赖它启动worker去执行map任务，而是需要我们自行启动 worker集群，让coordinator监听并接收请求，被动布置任务。
4. worker和worker间，worker和coordinator间需要用到RPC通信
5. map阶段需要worker领取map任务，reduce阶段需要与中间文件对应的worker领取reduce任务
6. 需要哪些rpc
   1. worker向coordinator索要任务：输入无，返回任务
   2. worker告知任务执行完毕：输入任务ID，返回无
7. 构造coordinator
   1. 初始要被执行word count的文件名
   2. nReduce
   3. 当前执行的阶段
   4. 任务分配的并发控制
8. 构造task
   1. 任务ID
   2. 任务类型
   3. 要处理的文件名
   4. 被分配到的reduce ID
