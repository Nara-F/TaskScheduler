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

class BlockingThreadPoolScheduler : public Scheduler {
   public:
    struct WorkItem {
        Task* work;
        int task_idx;
        int num_total_tasks;
        int task_id;
    };
    struct PendingTask {
        Task* work;
        int num_total_tasks;
        std::vector<TaskId> deps;
        TaskId task_id;
    };

    int num_threads;
    TaskId curTask;
    std::mutex task_mutex;
    std::vector<int> task_remaining;
    std::deque<WorkItem> task_queue;
    std::mutex queue_mutex;
    std::vector<std::thread> threads;

    std::mutex set_mutex;
    std::unordered_set<TaskId> finished_tasks;

    std::mutex pending_set_mutex;
    std::vector<PendingTask> pending_tasks;

    // Empty batches count as one until their dependencies complete.
    int pending_task_count = 0;
    std::mutex finished_task_mutex;
    std::condition_variable finished_task_cv;

    // Worker states: -1 = stop, 0 = idle, 1 = running.
    std::vector<int> status_call;
    std::vector<std::mutex> status_mutex;

    std::vector<std::condition_variable> cv;

    BlockingThreadPoolScheduler(int num_threads = defaultThreadCount());
    ~BlockingThreadPoolScheduler();
    const char* name();
    bool enqueueIfReady(PendingTask& task);
    void execute(Task* work, int num_total_tasks);
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps);
    void waitForAll();
};

}  // namespace task_scheduler
