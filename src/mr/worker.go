package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task, err :=
			CallAssignTask()
		if err != nil {
			log.Fatalf(" AssignTask failed: %v", err)
		}
		switch task.Task.TaskType {
		case MapTaskType:
			doMapTask(mapf, task.Task.TaskId, task.Task.Filename, task.Task.ReduceId)
		case ReduceTaskType:
			doReduceTask(reducef, task.Task.TaskId, task.Task.ReduceId)
		default:
			fmt.Println("worker finished all tasks")
			return
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, taskId int, filename string, nReduce int) {
	//1.读取要被处理的文件内容
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Map:cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Map:cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("Map:cannot close %v", filename)
	}

	//2.调用mapf函数处理文件内容并保存到中间变量中（同key会被保存在同一中间文件中，因此一组）
	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, nReduce)
	//3. 由键值对的key来获取中间文件的Y值，也就是要处理reduce的worker ID
	for _, kv := range kva {
		y := ihash(kv.Key) % nReduce
		intermediate[y] = append(intermediate[y], kv)
	}
	//4. 将中间变量写入到中间文件中
	for y := 0; y < nReduce; y++ {
		//4.1 文件命名和临时文件创建
		intermediateFilename := fmt.Sprintf("mr-%d-%d", taskId, y)
		tempFile, err := os.CreateTemp("./", intermediateFilename)
		if err != nil {
			log.Fatalf("Map:cannot create temp file: %v", intermediateFilename)
		}
		//4.2 将结果写入到临时文件
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[y] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Map:cannot encode temp file: %v", intermediateFilename)
			}
		}
		//4.3 将临时文件重命名成中间文件
		err = tempFile.Close()
		if err != nil {
			log.Fatalf("Map: cannot close temp file:%v", tempFile.Name())
		}
		err = os.Rename(tempFile.Name(), intermediateFilename)
		if err != nil {
			log.Fatalf("Map: cannot rename temp file:%v", tempFile.Name())
		}
	}
	// 5. 通知coordinator 任务已完成
	CallTaskComplete(taskId)
}

func doReduceTask(reducef func(string, []string) string, taskId int, reduceId int) {
	//1. 扫描当前目录下的所有文件
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("Reduce:cannot read dir")
	}
	//2. 从中间文件中读取键值对
	var kva []KeyValue
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		//2.1 判断当前目录下的文件是否是本reduce worker需要执行的中间文件
		hasRightPrefix := strings.HasPrefix(file.Name(), "mr-")
		hasRightSuffix := strings.HasSuffix(file.Name(), fmt.Sprintf("-%d", reduceId))
		if hasRightPrefix && hasRightSuffix {
			//2.2 若需要读取，则读取中间文件内容
			file, err := os.Open(file.Name())
			if err != nil {
				log.Fatalf("Reduce:cannot open %v", file.Name())
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err == io.EOF {
					break
				} else if err != nil {
					log.Fatalf("Reduce:cannot decode %v", file.Name())
				}
				kva = append(kva, kv)
			}
		}
	}
	//3. 创建临时文件
	filename := fmt.Sprintf("mr-out-%d", reduceId)
	tempFile, err := os.CreateTemp("./", filename)
	if err != nil {
		log.Fatalf("Reduce:cannot create %v", filename)
	}
	//4. 执行reducef 任务
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	//5. 将临时文件重命名成最终文件
	err = tempFile.Close()
	if err != nil {
		log.Fatalf("Reduce: cannot close temp file:%v", tempFile.Name())
	}
	err = os.Rename(tempFile.Name(), filename)
	if err != nil {
		log.Fatalf("Reduce: cannot rename temp file:%v", tempFile.Name())
	}
	//6. 通知coordinator任务已完成
	CallTaskComplete(taskId)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallAssignTask() (*AssignTaskReply, error) {
	args := AssignTaskArgs{}
	reply := AssignTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return &reply, nil
	}
	return &reply, fmt.Errorf("call AssignTask failed")
}

func CallTaskComplete(taskId int) {
	args := TaskCompleteArgs{TaskId: taskId}
	reply := TaskCompleteReply{}
	call("Coordinator.TaskComplete", &args,
		&reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
