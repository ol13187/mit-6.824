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
	wg           sync.WaitGroup
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
	// Your code here.

	if c.phase == AllTasksComplete {

		c.Done()
	}

	task := <-c.taskQueue
	reply.Task = task
	// 开启一个goroutine ，若10s内未返回完成进度则重新分配任务，若收到则当做任务执行完毕
	taskId := task.TaskId
	c.mu.Lock()
	c.taskProgress[taskId] = make(chan struct{})
	c.mu.Unlock()

	go func(taskProgress chan struct{}) {
		select {
		case <-taskProgress:
			return
		case <-time.After(Timeout):
			c.mu.Lock()
			c.taskQueue <- task
			c.mu.Unlock()
		}
	}(c.taskProgress[taskId])

	return nil
}

// worker完成任务后告知coordinator，后者记录任务完成情况
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	// Your code here.
	taskId := args.TaskId

	taskProgress := c.taskProgress[taskId]
	if taskProgress == nil {

		return errors.New("task not found")
	}

	select {
	case taskProgress <- struct{}{}:
		c.wg.Done()
	default:
		return errors.New("task already completed or channel closed")
	}
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
	// Your code here.
	//1.初始化构造 coordinator 结构体
	c := Coordinator{
		files:        files,
		nReduce:      nReduce,
		phase:        ReadyForMapping,
		taskId:       atomic.Int64{},
		taskQueue:    make(chan Task, len(files)+nReduce),
		taskProgress: make(map[int]chan struct{}),
		wg:           sync.WaitGroup{},
		mu:           sync.Mutex{},
	}
	//2.1将获取到的文件以任务队列的方式存储，为worker领取任务做准备
	c.wg.Add(len(files))
	go func(files []string, nReduce int) {
		//2.2 将map任务分配给worker
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

		c.wg.Wait()
		c.mu.Lock()
		c.phase = ReadyForReducing
		c.mu.Unlock()
		//2.3 将map任务完成后，开启reduce任务
		c.wg.Add(nReduce)
		for i := 0; i < nReduce; i++ {
			reduceTask := Task{
				TaskId:   int(c.taskId.Load()),
				TaskType: ReduceTaskType,
				ReduceId: i,
			}
			c.taskId.Add(1)
			c.taskQueue <- reduceTask
		}
		c.wg.Wait()
		c.mu.Lock()
		c.phase = AllTasksComplete
		c.mu.Unlock()
	}(files, nReduce)
	// 3.开启socket 监听
	c.server()
	return &c
}
