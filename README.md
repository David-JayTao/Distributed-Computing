# 分布计算新技术实验一：MapReduce

本项目在 MIT 6.5840 Spring 2026 Lab 1 官方脚手架上，用 Go 实现了一个单 Coordinator、多 Worker 的分布式 MapReduce 系统。调度与任务状态由 Coordinator 管理，Worker 通过 Unix domain socket 上的 `net/rpc` 拉取任务，在共享工作目录中完成 Map、Shuffle 和 Reduce。实现严格只修改官方要求中的三个文件，没有修改测试、main 程序或应用插件。

当前开发分支：`exp1-mapreduce`。本分支只在本地提交，尚未推送到 GitHub。

## 完成情况

- 官方 7 项 MapReduce 测试连续两轮全部通过，测试全程启用 `-race`，无数据竞争警告；
- 手工 Word Count 与串行实现的结果逐字节一致：10 个输出文件、22,107 行；
- 已生成中文实验报告、测试记录、系统架构图/数据流图/状态机图；
- 已生成课程要求的四文件提交压缩包，压缩包文件名中的学号和姓名仍为待填写占位符；
- 官方测试代码、`main/` 应用代码和 `mrapps/` 插件代码均未改动。

详细测试证据见 [实验一测试记录](docs/实验一测试记录.md)，正式报告见 [实验一MapReduce实验报告.docx](Exp1-MapReduce/实验一MapReduce实验报告.docx)，学习路线见 [实验一学习导览](docs/实验一学习导览.md)。

## 目录结构

```text
Distributed-Computing/
├── README.md
├── docs/
│   ├── 实验一测试记录.md
│   ├── 实验一学习导览.md
│   └── logs/
│       ├── 2026-10-27-make-mr-run1.log
│       ├── 2026-10-27-make-mr-run2.log
│       ├── 2026-10-27-repeat-race.log
│       ├── 2026-10-27-stale-report-test.log
│       └── 2026-10-27-manual-wordcount.log
├── Exp1-MapReduce/
│   ├── mapreduce.pdf                         # Google 2004 论文
│   ├── 分布计算新技术实验报告模板-实验一MapReduce.docx
│   ├── 实验一MapReduce实验报告.docx
│   ├── report-assets/                        # 报告中的三张静态示意图
│   ├── 提交材料/
│   │   └── 待填写学号+待填写姓名+实验一.zip
│   └── mit-lab/                              # MIT 官方脚手架
│       ├── Makefile
│       ├── UPSTREAM.md
│       └── src/
│           ├── Makefile
│           ├── go.mod
│           ├── main/                         # 官方 main 与输入文本
│           ├── mrapps/                       # 官方 MapReduce 应用插件
│           └── mr/
│               ├── coordinator.go            # 本次实现
│               ├── worker.go                 # 本次实现
│               ├── rpc.go                    # 本次实现
│               ├── mr_test.go                # 官方测试（未修改）
│               └── util.go                   # 官方测试工具（未修改）
└── Exp2-Pruning/                             # 其他实验资料，未改动
```

## 环境依赖

- Linux 环境；推荐 Windows + WSL2 Ubuntu。Go plugin 和 Unix domain socket 无法在 Windows 原生进程中直接运行；
- Go 1.22 及以上；本项目实测使用 `go1.27.3 linux/amd64`；
- `make`、`git`；
- 测试与运行均不需要 Python；报告中的图片和 DOCX 已经生成并保存在仓库中。

本机安装的 Go 位于 `/home/xiaoge/tools/go/bin/go`，因此开发命令中可临时执行：

```bash
export PATH=$HOME/tools/go/bin:$PATH
go version
```

如果系统已安装 Go，可忽略这一设置。

## 编译、运行与测试

所有 Go 命令都应在 `Exp1-MapReduce/mit-lab/src` 下执行。

### 1. 构建

```bash
cd Exp1-MapReduce/mit-lab/src
make mr-build
```

该命令构建 `main/mrcoordinator`、`main/mrworker`、`main/mrsequential`，以及 `mrapps/` 下测试所需的 `.so` 插件。

### 2. 手工运行 Word Count

在 `src/main` 目录启动一个 Coordinator 和多个 Worker：

```bash
cd Exp1-MapReduce/mit-lab/src/main
rm -f mr-out-* /tmp/mr-demo.sock

./mrcoordinator /tmp/mr-demo.sock pg-*.txt &
./mrworker ../mrapps/wc.so /tmp/mr-demo.sock &
./mrworker ../mrapps/wc.so /tmp/mr-demo.sock &
wait
```

Coordinator 全部完成后会退出，Worker 收到 `TaskExit` 或连接失败后也会退出。查看结果：

```bash
LC_ALL=C cat mr-out-* | sort | head
rm -f /tmp/mr-demo.sock
```

手工运行会在 `src/main` 生成 `mr-X-Y` 和 `mr-out-*`；这些构建产物和运行产物已由 `.gitignore` 忽略。

### 3. 官方测试

```bash
cd Exp1-MapReduce/mit-lab/src
make mr
```

`src/Makefile` 中 `make mr` 已经带有 `-race`。也可以只运行某一项测试：

```bash
make RUN="-run Wc" mr
make RUN="-run CrashWorker" mr
```

测试名称包括 `TestWc`、`TestIndexer`、`TestMapParallel`、`TestReduceParallel`、`TestJobCount`、`TestEarlyExit`、`TestCrashWorker`。

## 三个核心 Go 文件的职责

| 文件 | 职责 |
|---|---|
| `mr/rpc.go` | 定义 Worker 与 Coordinator 之间的任务请求/分配/完成报告协议；区分 `TaskWait`、`TaskMap`、`TaskReduce`、`TaskExit`，并携带 `TaskID`、`Attempt`、`NReduce`、`NMap` 等字段。 |
| `mr/coordinator.go` | 初始化 Map/Reduce 任务表；维护 `Idle/Running/Completed` 任务状态和 `Map → Reduce → Done` 阶段；实现 `GetTask`、`ReportTask`、10 秒超时重派、迟到报告过滤、`Done()` 与互斥锁保护。 |
| `mr/worker.go` | 循环请求任务；调用插件加载的 Map/Reduce 函数；用 `ihash(key) % nReduce` 做哈希分区；以 JSON 写入 `mr-X-Y`；按 Key 排序分组生成 `mr-out-X`；使用临时文件与原子重命名发布结果。 |

官方 `main/mrcoordinator.go`、`main/mrworker.go`、`main/mrsequential.go`、`mr/mr_test.go`、`mr/util.go` 与 `mrapps/*.go` 均保持原样。

## 预期输出

- 每个 Map 任务生成 `nReduce` 个中间文件 `mr-X-Y`，文件内容为逐行 JSON 编码的 `KeyValue`；
- 每个 Reduce 任务生成 `mr-out-Y`，每行严格采用 `%v %v` 格式；
- Word Count 的合并排序输出与 `mrsequential` 完全一致。实际样例：

```text
A 509
ABOUT 2
ACT 8
AND 46
OF 108
THE 188
```

- 正式测试结果两轮均为 7/7 PASS，`-race` 无警告；具体耗时和日志见 [实验一测试记录](docs/实验一测试记录.md)。

## Git 阶段划分

历史上按真实开发顺序保留了以下提交：

| Commit | 说明 |
|---|---|
| `cc290ec` | 实验一：引入 MIT MapReduce 官方实验框架 |
| `a25aa9d` | 实验一：实现 RPC 通信与任务调度基础结构 |
| `6fccb7b` | 实验一：实现 Map 任务与中间数据分区 |
| `b4a504d` | 实验一：实现 Reduce 任务与完整计算流程 |
| `1c1c899` | 实验一：完善并发控制与故障恢复机制 |
| `dc426d8` | 实验一：通过官方测试并补充边界情况处理 |

报告、README、学习导览和最终提交包汇总在本分支的最后一个交付提交中。提交信息均为中文，未改写历史，未强推远端。

## 提交材料

课程要求将实验报告和三个 Go 文件压缩为“学号+姓名+实验一”。当前文件：

```text
Exp1-MapReduce/提交材料/待填写学号+待填写姓名+实验一.zip
```

ZIP 内仅包含：

```text
实验一MapReduce实验报告.docx
worker.go
coordinator.go
rpc.go
```

三个 `.go` 文件直接来自最终通过验证的 `Exp1-MapReduce/mit-lab/src/mr/`，不是空白模板或临时版本。提交前请：

1. 在报告封面填写姓名、学号（当前为红色占位符）；
2. 把 ZIP 重命名为自己的“学号+姓名+实验一.zip”；
3. 如需要，可用 Word 重新保存报告确认个人信息。

## 说明

- 本实验未自动执行 `git push`，也没有创建 PR；本地分支为 `exp1-mapreduce`。
- 其他实验目录（如 `Exp2-Pruning/`）未被修改。
- 报告中未填写或编造姓名、学号，未使用伪造运行截图；测试结论均来自真实命令输出和日志。
