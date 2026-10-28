# 统一测试入口

在项目根目录运行。需要 C++11 编译器、CMake 及 Python 3.8+，不需要第三方 Python 包。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

统一入口为 `python3 tests/run_tests.py`；它负责选择用例、生成/校验 DAG、重放配置和设置进程超时，
实际调度与任务计算由 C++ 可执行文件 `build/task_scheduler_tests` 完成。
自定义构建目录可通过 `--binary /path/to/task_scheduler_tests` 指定。
仅构建核心库时可设置 `-DBUILD_TESTING=OFF`。

## 复用现有测试

已迁移 29 个内置用例，包括轻重任务、负载不均衡、数学运算、Mandelbrot、同步/异步以及严格 DAG 依赖检查。
用例注册在 `builtin_cases.cpp` 的 `builtinCases()` 中；添加 C++ 用例后，在这里注册名称、异步标志和函数即可复用统一入口。

```sh
# 查看名称和执行模式。
python3 tests/run_tests.py list

# 所有实现运行选定的同步用例。
python3 tests/run_tests.py builtin --scheduler all \
    --case simple_test_sync super_light --threads 4 --iterations 3

# 运行异步依赖用例。
python3 tests/run_tests.py builtin --case strict_diamond_deps_async strict_graph_deps_large_async

# 全部用例；同步用例覆盖四种实现，异步用例只运行 blocking。
python3 tests/run_tests.py builtin --scheduler all --threads 16

# 只跑某个实现支持的同步用例。
python3 tests/run_tests.py builtin --scheduler busy-wait --suite sync
```

`--scheduler`：`sequential`、`spawn`、`busy-wait`、`blocking` 或 `all`，默认 `blocking`。
除 `blocking` 外的实现没有异步接口实现：显式选择它们运行异步用例会报错；
使用 `all` 时会明确打印 `SKIP`，不会把未执行的组合计为通过。

默认每组运行一次，线程参数为 4，进程超时为 120 秒。
`simple_run_deps_test` 包含以秒为单位的固定休眠，少线程、多次重复时需要增大 `--timeout`。

## 空闲资源测量

`idle` 在每次迭代中同步执行一批计算任务，随后保持调度器存活并让调用线程休眠，观察空闲期间保留的工作线程和 CPU 开销。

```sh
python3 tests/run_tests.py idle --scheduler all --threads 4 \
    --iterations 7 --idle-ms 250
```

工作负载固定为一批 8 个子任务，每个子任务执行 500000 次整数 CPU 循环，计算与正确性校验复用自定义测试逻辑。每次迭代新建调度器，批次完成后再开始空闲测量；`--idle-ms` 必须为正整数，默认 250。

输出中的 `min_ms`、`median_ms` 是任务批次的执行耗时，不包含随后休眠的时间。额外指标包括：

- `idle_workers`：批次完成后调度器保留的可连接工作线程数，不含调用线程。按调用创建线程的实现已在 `execute()` 返回前连接并回收线程，两个线程池则保留工作线程。
- `idle_cpu_median_ms`：休眠区间内整个进程的 CPU 时间中位数，通过 `std::clock()` 测量，包含所有工作线程的 CPU 时间。
- `idle_wall_median_ms`：实际休眠区间的墙钟耗时中位数。多个工作线程同时忙等待时，累计 CPU 时间可以超过墙钟耗时。

## 自定义任务

```sh
# 每个子任务重量相同：20 批，每批 64 个子任务，链式依赖。
python3 tests/run_tests.py custom --batches 20 --tasks-per-batch 64 \
    --weight 10000 --dag chain --threads 8 --iterations 3

# 子任务重量在闭区间 [100, 50000] 内均匀随机分布，生成随机 DAG。
python3 tests/run_tests.py custom --batches 30 --tasks-per-batch 32 \
    --weight-range 100 50000 --dag random --edge-probability 0.15 --seed 42 \
    --scheduler all --save-config /tmp/workload.json

# 各批次分别指定子任务数，支持零任务批次。
python3 tests/run_tests.py custom --batches 5 --tasks-per-batch 8,0,4,16,0 \
    --weight 5000 --dag chain

# 自定义边和提交顺序。2 必须先于 0，0 必须先于 1。
python3 tests/run_tests.py custom --batches 3 --tasks-per-batch 8 \
    --dag custom --edges '2:0,0:1' --order '2,0,1'

# 重放完全相同的任务重量、依赖图和提交顺序。
python3 tests/run_tests.py custom --config /tmp/workload.json --threads 8 --iterations 3
```

重量是**每个子任务的整数 CPU 循环次数**，不是微秒。默认重量为 10000；支持 0。
循环使用确定性的无符号整数运算，结果参与正确性校验，避免被优化器直接移除。
`--weight-range MIN MAX` 对每个子任务独立采样，边界都包含在内。
实际耗时由机器、编译选项和竞争情况决定。`--seed` 固定随机工作量和随机图生成。

批次数由 `--batches` 指定，批内数量由 `--tasks-per-batch` 指定；后者接受一个统一数量，
或与批次数等长的逗号分隔列表。

### DAG 与提交顺序

边 `A:B` 表示批次 B 的任何子任务都必须等到批次 A 全部完成后才能开始。批次编号从 0 开始。

| `--dag` | 依赖规则 |
| --- | --- |
| `independent` | 所有批次互不依赖 |
| `chain` | `0 → 1 → …` |
| `fan-in` | 前面所有批次指向最后一批 |
| `fan-out` | 第一批指向后面所有批次 |
| `diamond` | 第一批 → 所有中间批 → 最后一批，至少 4 批 |
| `tree` | 二叉树，父节点 `(i-1)/2` 指向子节点 `i` |
| `random` | 随机拓扑排列中，每对前后节点按 `--edge-probability` 独立连边 |
| `custom` | `--edges '0:2,1:2'`，或 `--edges-file edges.json` |

边文件格式为 JSON 二元数组，例如 `[[0,2],[1,2]]`。
默认自动生成拓扑提交序；`--order` 可指定其他合法拓扑序。
框架会拒绝环、自依赖、越界编号、重复/缺失的提交节点、违反依赖的顺序及非法重量。
重复边会合并。编号较大的批次可以依赖提交在它之前的任意编号批次。

保存的配置格式如下，`weights` 每行对应一个批次，行内每个值对应一个子任务重量：

```json
{
  "version": 1,
  "weights": [[1000, 1000], [], [500, 2000]],
  "edges": [[0, 1], [1, 2]],
  "order": [0, 1, 2]
}
```

使用 `--config` 时，配置决定任务与 DAG，生成参数被忽略；调度器、线程数、重复次数和执行模式仍由命令行决定。
不含 `order` 的配置会自动计算拓扑序。

### 执行模式与校验

`--mode auto` 为默认：`blocking` 使用 `submit()` 提交整个 DAG 后 `waitForAll()`，
其余实现按拓扑顺序逐批调用 `execute()`。
`--mode sync` 强制同步执行，可用于比较同一实现的同步与异步开销；
`--mode async` 只接受 `--scheduler blocking`。
同步模式会串行化批次，不能体现独立批次重叠执行的收益。输出会注明 `async-dag` 或 `sync-topological`。

自定义测试逐任务验证执行次数恰为一次、索引/数量参数正确、计算结果与串行参考计算相同，
并在每个子任务开始时检查其前驱批次是否全部结束。零任务依赖会展开到其非空前驱进行检查。
生成工作量、分配内存、计算参考答案和验证结果均不计入调度执行计时。
每次迭代新建调度器，输出全部迭代中的最短值 `min_ms` 和中位数 `median_ms`；
线程池构造与析构也不计入该计时。

`--timeout` 对每个“用例 × 调度器”的整个子进程生效，包含参考计算和所有迭代。
错误、崩溃或超时都会让统一入口以非零状态退出。这些结果是本机性能测量，不设置相对参考二进制的评分阈值。

## 文件结构

- `run_tests.py`：统一 CLI、DAG/工作量生成和配置重放。
- `test_runner.cpp`：调度器选择、自定义计算、正确性校验与计时。
- `builtin_cases.cpp` / `test_cases.hpp`：迁入的用例及注册表。
- `test_clock.hpp`：基于 `steady_clock` 的单调计时。
- `test_framework.py`：生成、图校验和重放格式的单元测试。

CTest 默认执行框架校验、四种实现的同步测试、异步严格依赖测试、随机 DAG、零任务批次和空闲资源测试。
完整内置测试集需使用上面的 `builtin --scheduler all` 命令运行。
