#pragma once

#include <cstddef>
#include <condition_variable>
#include <deque>
#include <exception>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "task_scheduler/scheduler.hpp"

namespace task_scheduler {

class SequentialScheduler final : public Scheduler {
   public:
    explicit SequentialScheduler(int num_threads = defaultThreadCount());
    ~SequentialScheduler() override = default;
    const char* name() const noexcept override;
    void execute(Task* work, int num_total_tasks) override;
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps) override;
    void waitForAll() override;
};

class SpawnPerCallScheduler final : public Scheduler {
   public:
    explicit SpawnPerCallScheduler(int num_threads = defaultThreadCount());
    ~SpawnPerCallScheduler() override = default;
    const char* name() const noexcept override;
    void execute(Task* work, int num_total_tasks) override;
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps) override;
    void waitForAll() override;

   private:
    void workerLoop(Task* work, int num_total_tasks);

    const int num_threads;
    std::mutex task_id_mutex;
    TaskId curTask = 0;
    std::exception_ptr task_exception;
};

class BusyWaitThreadPoolScheduler final : public Scheduler {
   public:
    explicit BusyWaitThreadPoolScheduler(int num_threads = defaultThreadCount());
    ~BusyWaitThreadPoolScheduler() override;
    const char* name() const noexcept override;
    void execute(Task* work, int num_total_tasks) override;
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps) override;
    void waitForAll() override;

   private:
    void workerLoop(int worker_index);
    void stopWorkers();

    std::vector<std::mutex> work_queues_mutexs;
    std::mutex task_id_mutex;
    std::mutex finish_mutex;
    int finish_count = 0;
    std::exception_ptr task_exception;
    TaskId curTask = 0;
    struct WorkItem {
        Task* work;
        int num_total_tasks;
    };
    const int num_threads;
    std::vector<std::thread> threads_;
    std::vector<WorkItem> work_queues;

   public:
    // Read-only compatibility view for worker inspection.
    const std::vector<std::thread>& threads = threads_;
};

class BlockingThreadPoolScheduler final : public Scheduler {
   public:
    explicit BlockingThreadPoolScheduler(int num_threads = defaultThreadCount());
    ~BlockingThreadPoolScheduler() override;
    const char* name() const noexcept override;
    void execute(Task* work, int num_total_tasks) override;
    TaskId submit(Task* work, int num_total_tasks,
                  const std::vector<TaskId>& deps) override;
    void waitForAll() override;

   private:
    struct WorkItem {
        Task* work;
        int task_idx;
        int num_total_tasks;
        TaskId task_id;
    };
    struct PendingTask {
        Task* work;
        int num_total_tasks;
        std::vector<TaskId> deps;
        TaskId task_id;
    };
    struct BatchState {
        int remaining;
        std::exception_ptr error;
    };

    bool enqueueIfReady(const PendingTask& task);
    void workerLoop(int worker_index);
    void stopWorkers();
    void waitUntilIdle();

    const int num_threads;
    TaskId curTask = 0;
    std::mutex task_mutex;
    std::vector<BatchState> task_states;
    std::deque<WorkItem> task_queue;
    std::mutex queue_mutex;
    std::vector<std::thread> threads_;

    std::mutex set_mutex;
    std::unordered_set<TaskId> finished_tasks;

    std::mutex pending_set_mutex;
    std::vector<PendingTask> pending_tasks;

    // Empty batches count as one until their dependencies complete.
    std::size_t pending_task_count = 0;
    std::exception_ptr task_exception;
    std::mutex finished_task_mutex;
    std::condition_variable finished_task_cv;

    // Worker states: -1 = stop, 0 = idle, 1 = running.
    std::vector<int> status_call;
    std::vector<std::mutex> status_mutex;

    std::vector<std::condition_variable> cv;

   public:
    // Read-only compatibility view for worker inspection.
    const std::vector<std::thread>& threads = threads_;
};

}  // namespace task_scheduler
