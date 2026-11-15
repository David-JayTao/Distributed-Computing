package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 任务超时后由 Coordinator 重新分配。官方实验要求为 10 秒。
const taskTimeout = 10 * time.Second

// taskStatus 是单个任务的生命周期状态。
type taskStatus int

const (
	taskIdle taskStatus = iota
	taskRunning
	taskCompleted
)

// taskInfo 保存一个 Map 或 Reduce 任务的调度元数据。
type taskInfo struct {
	input   string // Map 任务的输入文件；Reduce 任务为空
	status  taskStatus
	attempt int       // 每次重新分配时递增，用于拒绝迟到的完成报告
	start   time.Time // 本轮分配开始时间，供超时检测使用
}

// jobPhase 表示整个作业当前所处的阶段。
type jobPhase int

const (
	phaseMap jobPhase = iota
	phaseReduce
	phaseDone
)

// Coordinator 是全局唯一的调度器。所有字段都由 mu 保护。
type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []taskInfo
	reduceTasks []taskInfo
	nReduce     int
	phase       jobPhase
}

// pickRunnableTask 在任务数组中寻找可分配的任务。
// 空闲任务立即可分配；运行超过 taskTimeout 的任务视为 Worker 失联并被重派。
// 每次重派都会递增 attempt，使旧 Worker 的迟到报告自动失效。
// 调用者必须持有 c.mu。
func (c *Coordinator) pickRunnableTask(tasks []taskInfo) int {
	now := time.Now()
	for i := range tasks {
		expired := tasks[i].status == taskRunning && now.Sub(tasks[i].start) >= taskTimeout
		if tasks[i].status == taskIdle || expired {
			if expired {
				log.Printf("task %d timed out after %v; reassigning", i, taskTimeout)
			}
			tasks[i].status = taskRunning
			tasks[i].attempt++
			tasks[i].start = now
			return i
		}
	}
	return -1
}

// allCompleted 判断某阶段的任务是否全部完成。
// 调用者必须持有 c.mu。
func (c *Coordinator) allCompleted(tasks []taskInfo) bool {
	for i := range tasks {
		if tasks[i].status != taskCompleted {
			return false
		}
	}
	return true
}

// GetTask 是 Worker 拉取任务的核心 RPC。
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == phaseDone {
		reply.Type = TaskExit
		return nil
	}

	if c.phase == phaseMap {
		if i := c.pickRunnableTask(c.mapTasks); i >= 0 {
			task := c.mapTasks[i]
			reply.Type = TaskMap
			reply.TaskID = i
			reply.Attempt = task.attempt
			reply.InputFile = task.input
			reply.NReduce = c.nReduce
			return nil
		}
		if c.allCompleted(c.mapTasks) {
			// Map 阶段结束，切换到 Reduce 阶段。
			c.phase = phaseReduce
		} else {
			reply.Type = TaskWait
			return nil
		}
	}

	if c.phase == phaseReduce {
		if i := c.pickRunnableTask(c.reduceTasks); i >= 0 {
			task := c.reduceTasks[i]
			reply.Type = TaskReduce
			reply.TaskID = i
			reply.Attempt = task.attempt
			reply.NReduce = c.nReduce
			reply.NMap = len(c.mapTasks)
			return nil
		}
		if c.allCompleted(c.reduceTasks) {
			c.phase = phaseDone
			reply.Type = TaskExit
		} else {
			reply.Type = TaskWait
		}
		return nil
	}

	reply.Type = TaskExit
	return nil
}

// ReportTask 接收 Worker 的任务完成回报。
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []taskInfo
	switch args.Type {
	case TaskMap:
		tasks = c.mapTasks
	case TaskReduce:
		tasks = c.reduceTasks
	default:
		return nil
	}

	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}

	task := &tasks[args.TaskID]
	// 任务可能已被超时重派；只有当前 Attempt 的报告才能改变状态。
	if task.status != taskRunning || task.attempt != args.Attempt {
		return nil
	}
	if args.Success {
		task.status = taskCompleted
		reply.Accepted = true

		// 最后一个任务完成时立即推进阶段，避免依赖后续 GetTask 触发；
		// 这样即使 Worker 恰好在这一刻退出，Done() 仍能正确结束。
		if args.Type == TaskMap && c.allCompleted(c.mapTasks) {
			c.phase = phaseReduce
		} else if args.Type == TaskReduce && c.allCompleted(c.reduceTasks) {
			c.phase = phaseDone
		}
	} else {
		// 本地执行失败时归还任务，等待下一次分配。
		task.status = taskIdle
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == phaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		phase:   phaseMap,
	}
	c.mapTasks = make([]taskInfo, len(files))
	for i, file := range files {
		c.mapTasks[i] = taskInfo{input: file}
	}
	c.reduceTasks = make([]taskInfo, nReduce)

	c.server(sockname)
	return &c
}
