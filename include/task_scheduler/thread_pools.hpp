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

class SpawnPerCallScheduler : public Scheduler {
   public:
    int num_threads;
    std::mutex task_id_mutex;
    TaskId curTask;
    SpawnPerCallScheduler(int num_threads = defaultThreadCount());
    ~SpawnPerCallScheduler();
    const char* name();
    void execute(Task* work, int num_total_tasks);
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps);
    void waitForAll();
};

class BusyWaitThreadPoolScheduler : public Scheduler {
   public:
    BusyWaitThreadPoolScheduler(int num_threads = defaultThreadCount());
    ~BusyWaitThreadPoolScheduler();
    std::vector<std::mutex> work_queues_mutexs;
    std::mutex task_id_mutex;
    std::mutex finish_mutex;
    int finish_count = 0;
    TaskId curTask;
    struct WorkItem {
        Task* work;
        int num_total_tasks;
    };
    int num_threads;
    std::vector<std::thread> threads;
    std::vector<WorkItem> work_queues;
    const char* name();
    void execute(Task* work, int num_total_tasks);
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps);
    void waitForAll();
};

}  // namespace task_scheduler
