# TaskScheduler

TaskScheduler 是一个基于 C++11 的批量任务调度库，提供串行执行、按调用创建线程、忙等待线程池和阻塞线程池四种实现，可用于学习任务调度机制、比较不同工作负载下的调度开销，以及执行具有批次依赖的并行任务。

项目使用 CMake 构建，核心库仅依赖 C++ 标准库和系统线程库；测试入口使用 Python 标准库，无需安装第三方 Python 包。

## 调度器

所有实现均位于 `task_scheduler` 命名空间，通过 `Scheduler` 接口执行任务。

| 实现 | 测试命令中的名称 | 执行方式 | 同步执行 | 异步依赖调度 |
| --- | --- | --- | --- | --- |
| `SequentialScheduler` | `sequential` | 在调用线程逐个执行子任务 | 支持 | 未实现 |
| `SpawnPerCallScheduler` | `spawn` | 每次调用创建线程，批次完成后回收 | 支持 | 未实现 |
| `BusyWaitThreadPoolScheduler` | `busy-wait` | 复用工作线程，通过轮询等待任务 | 支持 | 未实现 |
| `BlockingThreadPoolScheduler` | `blocking` | 复用工作线程，通过条件变量等待任务 | 支持 | 支持 |

构造函数的线程数参数 `num_threads` 可以省略，默认使用 `std::thread::hardware_concurrency()` 返回的硬件线程数减一，最少为 1；无法获取硬件线程数时使用 1。显式传入正整数时直接使用该值，不再减一。

```cpp
task_scheduler::BlockingThreadPoolScheduler automatic;  // 默认：硬件线程数减一
task_scheduler::BlockingThreadPoolScheduler fixed(4);   // 显式：4 个工作线程
```

`spawn` 每批最多创建 `min(num_total_tasks, num_threads)` 个工作线程；两个线程池按选定数量创建工作线程，调用线程不参与任务计算。串行调度器始终在调用线程执行。测试脚本的 `--threads` 默认值仍为 4。

## 构建

需要支持 C++11 的编译器、CMake 3.16 或更新版本，以及系统线程库。构建测试还需要 Python 3.8 或更新版本。

在项目根目录执行：

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

默认构建核心库 `task_scheduler` 和测试程序 `task_scheduler_tests`，并注册 CTest 测试。

只构建核心库：

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
cmake --build build --parallel
```

## 任务与接口

一个 `Task` 对象描述一批共享回调的子任务。调度器对批次中的每个子任务调用一次：

```cpp
void executeTask(int task_id, int num_total_tasks);
```

`task_id` 是批内索引，范围为 `[0, num_total_tasks)`；`num_total_tasks` 是该批次的子任务总数。它们与用于批次依赖的 `TaskId` 是不同概念。

| 接口 | 含义 |
| --- | --- |
| `execute(work, num_total_tasks)` | 执行一批任务并等待完成 |
| `submit(work, num_total_tasks, deps)` | 提交一批异步任务，返回该批次的 `TaskId`；只有阻塞线程池实现此接口 |
| `waitForAll()` | 在阻塞线程池中等待所有已提交的任务完成；其余实现为空操作 |
| `name()` | 返回调度器名称 |

`deps` 包含当前批次的前驱批次 ID。当前批次必须等所有前驱的全部子任务完成后才能开始执行；没有依赖时传入 `{}`。按拓扑顺序提交批次，依赖 ID 使用同一调度器此前 `submit()` 返回的值。

使用时应满足以下约定：

- 线程数为正整数，子任务数量为非负整数。零任务批次不会调用回调；在阻塞线程池中，它仍会等待自身依赖完成，并可作为其他批次的前驱。
- 调度器通过指针访问任务，不接管 `Task` 对象的所有权。任务对象及其引用的数据必须存活到任务完成。
- 同一批次的多个回调可能并行执行，批内执行顺序不确定。回调应通过独立的数据分区或适当的同步机制避免数据竞争。
- 异步任务提交后，在读取结果、销毁任务对象或销毁调度器之前调用 `waitForAll()`。

`BlockingThreadPoolScheduler::execute()` 内部调用 `submit()` 和 `waitForAll()`，因此也会等待该调度器此前提交的其他未完成任务。

## 使用示例

下面先同步地将数组元素乘以 2，再提交两个具有依赖关系的异步批次，依次乘以 3 和 5。每个子任务负责一个数组元素，最终所有元素均为 30。

保存为 `main.cpp`：

```cpp
#include <iostream>
#include <vector>

#include "task_scheduler/thread_pools.hpp"

class MultiplyTask : public task_scheduler::Task {
public:
    MultiplyTask(std::vector<int>& values, int factor)
        : values_(values), factor_(factor) {}

    void executeTask(int task_id, int num_total_tasks) override {
        (void)num_total_tasks;
        values_[task_id] *= factor_;
    }

private:
    std::vector<int>& values_;
    int factor_;
};

int main() {
    std::vector<int> values(1000, 1);
    const int count = static_cast<int>(values.size());

    MultiplyTask times_two(values, 2);
    task_scheduler::SpawnPerCallScheduler sync_scheduler(4);
    sync_scheduler.execute(&times_two, count);

    MultiplyTask times_three(values, 3);
    MultiplyTask times_five(values, 5);
    task_scheduler::BlockingThreadPoolScheduler async_scheduler(4);

    const auto first = async_scheduler.submit(&times_three, count, {});
    async_scheduler.submit(&times_five, count, {first});
    async_scheduler.waitForAll();

    for (int value : values) {
        if (value != 30) return 1;
    }
    std::cout << "All values are 30\n";
}
```

### 在 CMake 项目中使用

将本项目放在使用方项目的 `external/TaskScheduler` 目录下，通过 `add_subdirectory()` 引入并链接导出的别名目标：

```cmake
cmake_minimum_required(VERSION 3.16)
project(TaskSchedulerExample LANGUAGES CXX)

add_subdirectory(external/TaskScheduler)
add_executable(example main.cpp)
target_link_libraries(example PRIVATE TaskScheduler::task_scheduler)
```

目标会传递头文件路径、C++11 要求和线程库依赖。可以用以下命令构建并运行示例：

```sh
cmake -S . -B build -DBUILD_TESTING=OFF
cmake --build build --parallel
./build/example
```

## 测试与性能测量

统一测试入口为 `tests/run_tests.py`，负责选择用例、生成工作负载、校验依赖图、重放配置和控制进程超时。任务计算与调度由 C++ 测试程序完成。

```sh
# 查看内置用例。
python3 tests/run_tests.py list

# 用四种调度器运行指定同步用例。
python3 tests/run_tests.py builtin --scheduler all \
    --case simple_test_sync super_light --threads 4 --iterations 3

# 生成可复现的随机 DAG 和子任务工作量。
python3 tests/run_tests.py custom --scheduler all \
    --batches 30 --tasks-per-batch 32 --weight-range 100 50000 \
    --dag random --seed 42 --save-config /tmp/workload.json

# 在阻塞线程池中重放配置。
python3 tests/run_tests.py custom --config /tmp/workload.json \
    --scheduler blocking --threads 8 --iterations 3
```

`--weight` 和 `--weight-range` 表示每个子任务的整数 CPU 循环次数。自定义工作负载支持独立批次、链、扇入、扇出、菱形、树、随机 DAG 和自定义依赖边，也支持零任务批次。

自定义测试默认让 `blocking` 使用异步 DAG 调度，其余实现按拓扑顺序逐批同步执行。可通过 `--mode sync` 统一使用同步模式。输出包含执行模式、最短耗时 `min_ms` 和中位耗时 `median_ms`，便于结合任务规模、线程数和依赖结构比较结果。

CTest 执行默认测试集；运行全部内置用例可使用 `python3 tests/run_tests.py builtin --scheduler all`。测试参数、配置格式、计时范围和校验方式见 [测试说明](tests/README.md)。

## 性能基线

下面四个场景分别展示四种调度策略的适用优势：极轻任务的低开销、间歇任务的资源释放、连续短批次的响应速度，以及宽依赖图的批次重叠。

统一使用 Release 构建，显式传入 4 个工作线程，每组顺序运行 7 次。耗时均为中位数，单位为毫秒；`weight` 表示每个子任务的整数 CPU 循环次数。全部 16 组“场景 × 调度器”组合均通过正确性校验。

执行耗时包含任务提交、调度、回调计算及回调内部校验、等待完成；不包含调度器构造与析构、工作负载准备、串行参考计算和执行后的结果校验。空闲资源指标另行测量。

先在项目根目录构建测试程序：

```sh
cmake -S . -B build/baseline -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=ON -DCMAKE_CXX_STANDARD=11 -DCMAKE_CXX_EXTENSIONS=OFF
cmake --build build/baseline --parallel 4
```

### 1. 串行：极轻任务，减少调度开销

64 批，每批 64 个子任务，`weight=0`，按链式依赖同步执行。回调只保留固定操作和正确性校验，适合观察调度本身的开销。

| 指标 | sequential | spawn | busy-wait | blocking |
| --- | ---: | ---: | ---: | ---: |
| 执行耗时 | **0.138** | 5.696 | 1.645 | 2.194 |

串行执行直接调用回调，省去创建线程、跨线程分发和同步等待，适合计算量很小的任务。

```sh
python3 tests/run_tests.py custom --binary build/baseline/task_scheduler_tests \
    --scheduler all --threads 4 --iterations 7 --mode sync \
    --batches 64 --tasks-per-batch 64 --weight 0 --dag chain
```

### 2. 按调用创建线程：间歇并行，空闲时释放资源

每次执行一批 8 个子任务，`weight=500000`；批次结束后保持调度器存活，调用线程休眠 250 毫秒，测量空闲资源。

| 调度器 | 批次执行耗时 | 空闲时保留的工作线程数 | 空闲区间 CPU 时间 |
| --- | ---: | ---: | ---: |
| sequential | 12.954 | 0 | 0.026 |
| **spawn** | **5.489** | **0** | **0.028** |
| busy-wait | 4.830 | 4 | 999.183 |
| blocking | 5.226 | 4 | 0.032 |

`spawn` 能在任务阶段并行计算，并在 `execute()` 返回前回收全部工作线程。这个场景展示其“需要时创建、结束后释放”的特点，适合调用间隔较长、希望减少常驻线程的工作负载。

工作线程数不含调用线程；CPU 时间通过 `std::clock()` 统计休眠区间内整个进程的累计消耗，多个线程同时忙等待时可以超过 250 毫秒。阻塞线程池在空闲时仍保留工作线程，但通过休眠保持很低的 CPU 消耗。批次执行耗时不包含后续休眠时间。

```sh
python3 tests/run_tests.py idle --binary build/baseline/task_scheduler_tests \
    --scheduler all --threads 4 --iterations 7 --idle-ms 250
```

### 3. 忙等待线程池：连续短批次，快速响应

256 批，每批 4 个子任务，`weight=10000`，按链式依赖同步执行。工作持续到达，线程池可以反复复用工作线程。

| 指标 | sequential | spawn | busy-wait | blocking |
| --- | ---: | ---: | ---: | ---: |
| 执行耗时 | 33.524 | 36.573 | **14.624** | 18.249 |

忙等待工作线程持续轮询，减少了短批次之间创建线程和从条件变量休眠中唤醒的开销。在连续到达、每批计算量足以并行的短任务中，这种响应方式能降低总耗时；其空闲 CPU 消耗见上一场景。

```sh
python3 tests/run_tests.py custom --binary build/baseline/task_scheduler_tests \
    --scheduler all --threads 4 --iterations 7 --mode sync \
    --batches 256 --tasks-per-batch 4 --weight 10000 --dag chain
```

### 4. 阻塞线程池：宽依赖图，跨批次并行

64 批，每批只有 1 个子任务，`weight=1000000`。使用扇出依赖：第 0 批完成后，其余 63 批均可独立执行。

| 指标 | sequential（同步） | spawn（同步） | busy-wait（同步） | blocking（异步） |
| --- | ---: | ---: | ---: | ---: |
| 执行耗时 | 159.810 | 313.187 | 175.298 | **58.689** |

同步接口每次只能提交并等待一个批次，此处每批只有一个子任务，无法利用批内并行。阻塞线程池可一次提交整个 DAG，在前驱结束后让多个就绪批次并行执行，从而利用图的宽度。

这里使用 `--mode auto`：前三种实现按拓扑顺序同步执行，`blocking` 使用异步 DAG 调度。该场景比较不同接口能够表达的并行度。

```sh
python3 tests/run_tests.py custom --binary build/baseline/task_scheduler_tests \
    --scheduler all --threads 4 --iterations 7 --mode auto \
    --batches 64 --tasks-per-batch 1 --weight 1000000 --dag fan-out
```

## 项目结构

```text
TaskScheduler/
├── CMakeLists.txt                         # 库、测试程序及 CTest 配置
├── include/task_scheduler/
│   ├── scheduler.hpp                      # Task、TaskId 与 Scheduler 接口
│   └── thread_pools.hpp                   # 四种调度器的声明
├── src/
│   └── thread_pools.cpp                   # 调度器实现
└── tests/
    ├── README.md                         # 测试使用说明
    ├── run_tests.py                      # 统一命令行入口
    ├── test_runner.cpp                   # C++ 执行器与自定义工作负载校验
    ├── builtin_cases.cpp                 # 内置任务与测试用例
    ├── test_cases.hpp                    # 内置用例注册接口
    ├── test_clock.hpp                    # 单调计时工具
    └── test_framework.py                 # Python 测试框架单元测试
```
