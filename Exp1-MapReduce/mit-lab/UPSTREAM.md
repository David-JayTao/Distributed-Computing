# 上游来源记录

- 课程：MIT 6.5840 Spring 2026
- 实验：Lab 1: MapReduce
- 官方实验说明：https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
- 上游 Git 地址：git://g.csail.mit.edu/6.5840-golabs-2026
- 上游分支：master
- 上游 commit：`513cac0aef010750fc74688da8a395974a83cf7a`
- 引入日期：2026-10-27
- 引入方式：`git clone --depth 1` 后复制工作树，删除嵌套 `.git`，由课程仓库统一管理版本历史。

本目录中的实验脚手架保持官方原始内容；实验实现仅修改 `src/mr/worker.go`、`src/mr/coordinator.go`、`src/mr/rpc.go`。
