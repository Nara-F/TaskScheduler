#pragma once

#include <cstddef>
#include <condition_variable>
#include <deque>
#include <exception>
#include <mutex>
#include <thread>
#include <unordered_map>
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

    // Wait for completion, then release retained state and invalidate all old IDs.
    // Unreported task errors are thrown before any history is released.
    void releaseHistory();

   private:
    struct WorkItem {
        Task* work;
        int task_idx;
        int num_total_tasks;
        TaskId task_id;
    };
    struct BatchState {
        Task* work;
        int num_total_tasks;
        int remaining;
        std::size_t unresolved_dependencies = 0;
        bool complete = false;
        TaskId next_completed = -1;
        std::vector<TaskId> successors;
        std::exception_ptr error;

        BatchState(Task* task, int count)
            : work(task), num_total_tasks(count), remaining(count) {}
    };

    // Dependency and queue updates require task_mutex to be held.
    void enqueueBatch(TaskId task_id);
    void completeBatch(TaskId task_id);
    void notifyWorkers(std::size_t count);
    void workerLoop();
    void stopWorkers();
    void waitUntilIdle();

    const int num_threads;
    TaskId curTask = 0;
    TaskId first_valid_task_id = 0;
    std::mutex task_mutex;
    // Missing valid IDs denote successful batches whose state has been reclaimed.
    std::unordered_map<TaskId, BatchState> task_states;
    std::deque<WorkItem> task_queue;
    std::condition_variable work_cv;
    std::size_t idle_workers = 0;
    bool stopping = false;
    std::vector<std::thread> threads_;

    // Every accepted batch remains pending until its dependencies and tasks finish.
    std::size_t pending_batch_count = 0;
    std::exception_ptr task_exception;
    std::condition_variable finished_task_cv;

   public:
    // Read-only compatibility view for worker inspection.
    const std::vector<std::thread>& threads = threads_;
};

}  // namespace task_scheduler
