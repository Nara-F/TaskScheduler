#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "task_scheduler/scheduler.hpp"

namespace task_scheduler {

class SequentialScheduler : public Scheduler {
   public:
    SequentialScheduler(int num_threads = defaultThreadCount());
    ~SequentialScheduler();
    const char* name();
    void execute(Task* work, int num_total_tasks);
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps);
    void waitForAll();
};

}  // namespace task_scheduler
