package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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
			if err := runReduceTask(reducef, reply); err != nil {
				log.Printf("worker %d: reduce task %d failed: %v", workerID, reply.TaskID, err)
				reportTask(reply, false)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if !reportTask(reply, true) {
				return
			}
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
// Accepted=false 表示任务已被超时重派，本次是迟到报告，不应影响调度状态。
func reportTask(task GetTaskReply, success bool) bool {
	args := ReportTaskArgs{
		Type:    task.Type,
		TaskID:  task.TaskID,
		Attempt: task.Attempt,
		Success: success,
	}
	reply := ReportTaskReply{}
	if !call("Coordinator.ReportTask", &args, &reply) {
		return false
	}
	if success && !reply.Accepted {
		log.Printf("worker %d: stale completion report for %s task %d attempt %d ignored",
			os.Getpid(), taskTypeName(task.Type), task.TaskID, task.Attempt)
	}
	return true
}

func taskTypeName(t TaskType) string {
	if t == TaskMap {
		return "map"
	}
	if t == TaskReduce {
		return "reduce"
	}
	return "unknown"
}

// mapFile 保存一个中间分区文件的临时写入资源和最终路径。
type mapFile struct {
	file      *os.File
	writer    *bufio.Writer
	enc       *json.Encoder
	tempName  string
	finalName string
}

// runMapTask 读取输入文件，调用应用 Map 函数，并按 ihash(key)%nReduce
// 将结果写入 nReduce 个中间文件。文件名采用实验建议的 mr-X-Y 形式。
//
// 每个分区先写入唯一临时文件，完整落盘后再原子重命名。这样 Reduce
// 不会读到某次崩溃留下的半截 JSON；失败时清理临时文件，保留原状态。
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

	cleanup := func() {
		for i := range outputs {
			if outputs[i].file != nil {
				outputs[i].file.Close()
				outputs[i].file = nil
			}
			if outputs[i].tempName != "" {
				os.Remove(outputs[i].tempName)
				outputs[i].tempName = ""
			}
		}
	}
	defer cleanup()

	for r := 0; r < task.NReduce; r++ {
		f, err := os.CreateTemp(".", fmt.Sprintf(".mr-map-%d-%d-*", task.TaskID, r))
		if err != nil {
			return fmt.Errorf("create temp intermediate file for partition %d: %w", r, err)
		}
		w := bufio.NewWriter(f)
		outputs[r] = mapFile{
			file:      f,
			writer:    w,
			enc:       json.NewEncoder(w),
			tempName:  f.Name(),
			finalName: fmt.Sprintf("mr-%d-%d", task.TaskID, r),
		}
	}

	for _, kv := range kvs {
		r := ihash(kv.Key) % task.NReduce
		if err := outputs[r].enc.Encode(&kv); err != nil {
			return fmt.Errorf("encode intermediate key %q: %w", kv.Key, err)
		}
	}

	for r := range outputs {
		out := &outputs[r]
		if err := out.writer.Flush(); err != nil {
			return fmt.Errorf("flush intermediate partition %d: %w", r, err)
		}
		if err := out.file.Sync(); err != nil {
			return fmt.Errorf("sync intermediate partition %d: %w", r, err)
		}
		if err := out.file.Close(); err != nil {
			return fmt.Errorf("close intermediate partition %d: %w", r, err)
		}
		out.file = nil
		if err := os.Rename(out.tempName, out.finalName); err != nil {
			return fmt.Errorf("publish intermediate %q: %w", out.finalName, err)
		}
		out.tempName = ""
	}
	return nil
}

// runReduceTask 读取所有 Map 任务对应本 Reduce 分区的中间文件，
// 按 Key 排序后将相同 Key 的 Value 聚合，调用应用 Reduce 函数，
// 最终以原子方式发布为 mr-out-<reduceTaskID>。
func runReduceTask(reducef func(string, []string) string, task GetTaskReply) error {
	if task.NMap < 0 {
		return fmt.Errorf("invalid nMap %d", task.NMap)
	}

	intermediate := make([]KeyValue, 0)
	for m := 0; m < task.NMap; m++ {
		name := fmt.Sprintf("mr-%d-%d", m, task.TaskID)
		f, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("open intermediate file %q: %w", name, err)
		}

		dec := json.NewDecoder(bufio.NewReader(f))
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				f.Close()
				return fmt.Errorf("decode intermediate file %q: %w", name, err)
			}
			intermediate = append(intermediate, kv)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close intermediate file %q: %w", name, err)
		}
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	outName := fmt.Sprintf("mr-out-%d", task.TaskID)
	out, err := os.CreateTemp(".", fmt.Sprintf(".mr-out-%d-*", task.TaskID))
	if err != nil {
		return fmt.Errorf("create temp output file for %q: %w", outName, err)
	}
	tempName := out.Name()
	published := false
	defer func() {
		if out != nil {
			out.Close()
		}
		if !published {
			os.Remove(tempName)
		}
	}()

	writer := bufio.NewWriter(out)
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(writer, "%v %v\n", intermediate[i].Key, output); err != nil {
			return fmt.Errorf("write output file %q: %w", outName, err)
		}
		i = j
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush output file %q: %w", outName, err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync output file %q: %w", outName, err)
	}
	if err := out.Close(); err != nil {
		out = nil
		return fmt.Errorf("close output file %q: %w", outName, err)
	}
	out = nil
	if err := os.Rename(tempName, outName); err != nil {
		return fmt.Errorf("publish output file %q: %w", outName, err)
	}
	published = true
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
