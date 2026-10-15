#include "task_scheduler/thread_pools.hpp"

#include <algorithm>

namespace task_scheduler {

Task::~Task() {}

int Scheduler::defaultThreadCount() {
    const unsigned int hardware_threads = std::thread::hardware_concurrency();
    return hardware_threads > 1 ? static_cast<int>(hardware_threads - 1) : 1;
}

Scheduler::Scheduler(int num_threads) {}
Scheduler::~Scheduler() {}

const char* SequentialScheduler::name() { return "SequentialScheduler"; }

SequentialScheduler::SequentialScheduler(int num_threads)
    : Scheduler(num_threads) {}

SequentialScheduler::~SequentialScheduler() {}

void SequentialScheduler::execute(Task* work, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        work->executeTask(i, num_total_tasks);
    }
}

TaskId SequentialScheduler::submit(Task* work,
    int num_total_tasks,
    const std::vector<TaskId>& deps) {
    return 0;
}

void SequentialScheduler::waitForAll() { return; }

}  // namespace task_scheduler
