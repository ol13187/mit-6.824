package mr

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Phase int

const Timeout = 10 * time.Second
const (
	ReadyForMapping  Phase = 1
	ReadyForReducing Phase = 2
	AllTasksComplete Phase = 3
)

type Coordinator struct {
	// Your definitions here.
	files        []string
	nReduce      int
	phase        Phase
	taskQueue    chan Task
	taskProgress map[int]chan struct{}
	taskId       atomic.Int64
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 为worker根据 taskQueue分配map或reduce任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	_ = args
	for {
		c.mu.Lock()
		// 所有阶段都完成后，返回空任务让 worker 退出。
		if c.phase == AllTasksComplete {
			c.mu.Unlock()
			reply.Task = Task{}
			return nil
		}
		c.mu.Unlock()

		select {
		case task := <-c.taskQueue:
			// 分配任务并登记执行中的任务状态。
			reply.Task = task
			taskId := task.TaskId
			progress := make(chan struct{})
			c.mu.Lock()
			c.taskProgress[taskId] = progress
			c.mu.Unlock()

			go func(task Task, taskProgress chan struct{}) {
				select {
				case <-taskProgress:
					// 收到完成信号，停止超时重试。
					return
				case <-time.After(Timeout):
					// 超时未完成则重入队列，交给其他 worker 重试。
					shouldRequeue := false
					c.mu.Lock()
					if c.phase != AllTasksComplete {
						shouldRequeue = true
					}
					c.mu.Unlock()

					if shouldRequeue {
						c.taskQueue <- task
					}
				}
			}(task, progress)
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// worker完成任务后告知coordinator，后者记录任务完成情况
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	_ = reply
	taskId := args.TaskId

	c.mu.Lock()
	// 找到该任务对应的进度通道，并从进行中集合移除。
	taskProgress := c.taskProgress[taskId]
	if taskProgress != nil {
		delete(c.taskProgress, taskId)
	}
	c.mu.Unlock()

	if taskProgress == nil {
		// 可能是重复上报或任务不存在。
		return errors.New("task not found")
	}

	// 关闭通道通知 AssignTask 里的超时协程停止重试。
	close(taskProgress)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == AllTasksComplete
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 初始化 coordinator 的共享状态和任务队列。
	c := Coordinator{
		files:        files,
		nReduce:      nReduce,
		phase:        ReadyForMapping,
		taskQueue:    make(chan Task, len(files)+nReduce),
		taskProgress: make(map[int]chan struct{}),
		taskId:       atomic.Int64{},
		mu:           sync.Mutex{},
	}

	// 先放入所有 map 任务。
	for _, filename := range files {
		mapTask := Task{
			TaskId:   int(c.taskId.Load()),
			TaskType: MapTaskType,
			Filename: filename,
			ReduceId: nReduce,
		}
		c.taskId.Add(1)
		c.taskQueue <- mapTask
	}

	go func(files []string, nReduce int) {
		_ = files
		for {
			c.mu.Lock()
			phase := c.phase
			pending := len(c.taskQueue)
			outstanding := len(c.taskProgress)

			// map 阶段：队列清空且无进行中任务时，切到 reduce 阶段。
			if phase == ReadyForMapping && pending == 0 && outstanding == 0 {
				c.phase = ReadyForReducing
				for i := 0; i < nReduce; i++ {
					reduceTask := Task{
						TaskId:   int(c.taskId.Load()),
						TaskType: ReduceTaskType,
						ReduceId: i,
					}
					c.taskId.Add(1)
					c.taskQueue <- reduceTask
				}
				c.mu.Unlock()
				continue
			}

			// reduce 阶段：全部 reduce 结束后，进入完成态。
			if phase == ReadyForReducing && pending == 0 && outstanding == 0 {
				c.phase = AllTasksComplete
				c.mu.Unlock()
				return
			}

			if phase == AllTasksComplete {
				c.mu.Unlock()
				return
			}

			c.mu.Unlock()
			// 轮询推进状态，避免忙等。
			time.Sleep(100 * time.Millisecond)
		}
	}(files, nReduce)
	// 3.开启socket 监听
	c.server()
	return &c
}
