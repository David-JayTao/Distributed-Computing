package mr

//
// Worker 与 Coordinator 之间的 RPC 协议定义。
//
// Go 的 net/rpc 只会序列化导出字段；因此本文件中所有需要在网络上
// 传输的字段都必须以大写字母开头。
//

// 以下两个示例类型保留自官方脚手架，便于对照 net/rpc 的基本调用方式。
// 正式实现使用本文件末尾定义的任务协议。
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// TaskType 表示一次任务请求的结果类型。
type TaskType int

const (
	TaskWait   TaskType = iota // 暂无空闲任务，Worker 应短暂等待后重试
	TaskMap                    // 执行一个 Map 任务
	TaskReduce                 // 执行一个 Reduce 任务
	TaskExit                   // 作业已完成，Worker 可以退出
)

// GetTaskArgs 是 Worker 请求任务时发送的参数。
// WorkerID 仅用于调试和日志，调度正确性由任务编号与 Attempt 保证。
type GetTaskArgs struct {
	WorkerID int
}

// GetTaskReply 描述 Coordinator 分配给 Worker 的任务。
// 对 Map 任务，InputFile 是输入文件，NReduce 指定中间文件的分区数；
// 对 Reduce 任务，NMap 指定需要读取的 Map 任务数量。
type GetTaskReply struct {
	Type      TaskType
	TaskID    int
	Attempt   int
	InputFile string
	NReduce   int
	NMap      int
}

// ReportTaskArgs 是 Worker 完成（或本地执行失败）后的回报。
// Attempt 用于识别“迟到”的重复执行，防止旧 Worker 覆盖新状态。
type ReportTaskArgs struct {
	Type    TaskType
	TaskID  int
	Attempt int
	Success bool
}

// ReportTaskReply 告诉 Worker 本次回报是否仍对应有效任务。
type ReportTaskReply struct {
	Accepted bool
}
