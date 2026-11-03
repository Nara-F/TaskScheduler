#pragma once

#include <vector>

namespace task_scheduler {

using TaskId = int;

// A batch of indexed tasks sharing the same execution callback.
class Task {
   public:
    virtual ~Task() = default;
    // Execute one subtask, indexed in [0, num_total_tasks).
    virtual void executeTask(int task_id, int num_total_tasks) = 0;
};

class Scheduler {
   public:
    // Hardware concurrency minus one, with a minimum of one worker.
    // Also returns one when the hardware thread count is unavailable.
    static int defaultThreadCount();

    virtual ~Scheduler() = default;

    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;
    Scheduler(Scheduler&&) = delete;
    Scheduler& operator=(Scheduler&&) = delete;

    virtual const char* name() const noexcept = 0;

    // Execute a batch and wait for completion.
    virtual void execute(Task* work, int num_total_tasks) = 0;

    // Submit a batch after all prerequisite batches have completed.
    // Only BlockingThreadPoolScheduler implements asynchronous submission.
    // The work must remain alive until its tasks complete.
    virtual TaskId submit(Task* work, int num_total_tasks,
                          const std::vector<TaskId>& deps) = 0;

    // Wait for all previously submitted work to complete.
    virtual void waitForAll() = 0;

   protected:
    Scheduler() = default;
};

}  // namespace task_scheduler
