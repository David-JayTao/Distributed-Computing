package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	workerID := os.Getpid()

	for {
		reply, ok := requestTask(workerID)
		if !ok {
			// Coordinator 已退出（或当前无法连接），Worker 随之结束。
			return
		}

		switch reply.Type {
		case TaskExit:
			return
		case TaskWait:
			// 当前没有可执行任务，采用短轮询避免空转占满 CPU。
			time.Sleep(50 * time.Millisecond)
		case TaskMap:
			if err := runMapTask(mapf, reply); err != nil {
				log.Printf("worker %d: map task %d failed: %v", workerID, reply.TaskID, err)
				reportTask(reply, false)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if !reportTask(reply, true) {
				return
			}
		case TaskReduce:
			// Reduce 执行流程将在下一阶段实现。
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// requestTask 向 Coordinator 拉取一个新任务。
func requestTask(workerID int) (GetTaskReply, bool) {
	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

// reportTask 回报任务完成情况。Success=false 表示本地执行失败，
// Coordinator 会将该任务重新放回待分配状态。
func reportTask(task GetTaskReply, success bool) bool {
	args := ReportTaskArgs{
		Type:    task.Type,
		TaskID:  task.TaskID,
		Attempt: task.Attempt,
		Success: success,
	}
	reply := ReportTaskReply{}
	return call("Coordinator.ReportTask", &args, &reply)
}

// mapFile 保存一个中间分区文件的写入资源。
type mapFile struct {
	file   *os.File
	writer *bufio.Writer
	enc    *json.Encoder
}

// runMapTask 读取输入文件，调用应用 Map 函数，并按 ihash(key)%nReduce
// 将结果写入 nReduce 个中间文件。文件名采用实验建议的 mr-X-Y 形式。
func runMapTask(mapf func(string, string) []KeyValue, task GetTaskReply) error {
	if task.NReduce <= 0 {
		return fmt.Errorf("invalid nReduce %d", task.NReduce)
	}

	content, err := os.ReadFile(task.InputFile)
	if err != nil {
		return fmt.Errorf("read input %q: %w", task.InputFile, err)
	}

	kvs := mapf(task.InputFile, string(content))

	outputs := make([]mapFile, task.NReduce)
	closeOutputs := func() {
		for i := range outputs {
			if outputs[i].writer != nil {
				outputs[i].writer.Flush()
			}
			if outputs[i].file != nil {
				outputs[i].file.Close()
			}
		}
	}
	defer closeOutputs()

	for r := 0; r < task.NReduce; r++ {
		name := fmt.Sprintf("mr-%d-%d", task.TaskID, r)
		f, err := os.Create(name)
		if err != nil {
			return fmt.Errorf("create intermediate file %q: %w", name, err)
		}
		w := bufio.NewWriter(f)
		outputs[r] = mapFile{file: f, writer: w, enc: json.NewEncoder(w)}
	}

	for _, kv := range kvs {
		r := ihash(kv.Key) % task.NReduce
		if err := outputs[r].enc.Encode(&kv); err != nil {
			return fmt.Errorf("encode intermediate key %q: %w", kv.Key, err)
		}
	}

	for r := range outputs {
		if err := outputs[r].writer.Flush(); err != nil {
			return fmt.Errorf("flush intermediate file %d: %w", r, err)
		}
		if err := outputs[r].file.Close(); err != nil {
			return fmt.Errorf("close intermediate file %d: %w", r, err)
		}
		// 防止 defer 重复关闭。
		outputs[r].writer = nil
		outputs[r].file = nil
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("%d: dial coordinator failed: %v", os.Getpid(), err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	} else {
		log.Printf("%d: call failed err %v", os.Getpid(), err)
	}
	return false
}
