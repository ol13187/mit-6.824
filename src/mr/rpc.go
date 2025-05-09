package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	MapTaskType TaskType = iota + 1
	ReduceTaskType
)

type Task struct {
	TaskId   int
	TaskType TaskType
	Filename string
	ReduceId int
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AssignTaskArgs struct{}

type AssignTaskReply struct {
	Task Task
}

type TaskCompleteArgs struct {
	TaskId int
}

type TaskCompleteReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
