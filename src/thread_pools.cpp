#include "task_scheduler/thread_pools.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace task_scheduler {

namespace {

int checkedThreadCount(int num_threads) {
    if (num_threads < 1) {
        throw std::invalid_argument("worker thread count must be positive");
    }
    return num_threads;
}

void validateTask(Task* work, int num_total_tasks) {
    if (num_total_tasks < 0) {
        throw std::invalid_argument("task count must not be negative");
    }
    if (num_total_tasks > 0 && work == nullptr) {
        throw std::invalid_argument("nonempty batches require a task");
    }
}

void joinThreads(std::vector<std::thread>& threads) {
    for (auto& thread : threads) {
        if (thread.joinable()) thread.join();
    }
}

class ThreadJoiner {
   public:
    explicit ThreadJoiner(std::vector<std::thread>& threads) : threads_(threads) {}
    ~ThreadJoiner() { joinThreads(threads_); }

    ThreadJoiner(const ThreadJoiner&) = delete;
    ThreadJoiner& operator=(const ThreadJoiner&) = delete;

   private:
    std::vector<std::thread>& threads_;
};

}  // namespace

int Scheduler::defaultThreadCount() {
    const unsigned int hardware_threads = std::thread::hardware_concurrency();
    return hardware_threads > 1 ? static_cast<int>(hardware_threads - 1) : 1;
}

const char* SequentialScheduler::name() const noexcept {
    return "SequentialScheduler";
}

SequentialScheduler::SequentialScheduler(int num_threads) {
    checkedThreadCount(num_threads);
}

void SequentialScheduler::execute(Task* work, int num_total_tasks) {
    validateTask(work, num_total_tasks);
    for (int i = 0; i < num_total_tasks; i++) {
        work->executeTask(i, num_total_tasks);
    }
}

TaskId SequentialScheduler::submit(Task*, int, const std::vector<TaskId>&) {
    throw std::logic_error("SequentialScheduler does not support asynchronous submission");
}

void SequentialScheduler::waitForAll() { return; }

const char* SpawnPerCallScheduler::name() const noexcept {
    return "SpawnPerCallScheduler";
}

SpawnPerCallScheduler::SpawnPerCallScheduler(int num_threads)
    : num_threads(checkedThreadCount(num_threads)) {}

void SpawnPerCallScheduler::execute(Task* work, int num_total_tasks) {
    validateTask(work, num_total_tasks);
    const int maxThread = std::min(num_total_tasks, num_threads);
    std::vector<std::thread> threads;
    threads.reserve(maxThread);

    curTask = 0;
    task_exception = nullptr;
    {
        ThreadJoiner joiner(threads);
        for (int i = 0; i < maxThread; i++) {
            threads.emplace_back(&SpawnPerCallScheduler::workerLoop, this,
                                 work, num_total_tasks);
        }
    }
    if (task_exception) std::rethrow_exception(task_exception);
}

void SpawnPerCallScheduler::workerLoop(Task* work, int num_total_tasks) {
    while (true) {
        TaskId task_id;
        {
            std::lock_guard<std::mutex> lock(task_id_mutex);
            if (curTask >= num_total_tasks) return;
            task_id = curTask++;
        }
        try {
            work->executeTask(task_id, num_total_tasks);
        } catch (...) {
            std::lock_guard<std::mutex> lock(task_id_mutex);
            if (!task_exception) task_exception = std::current_exception();
        }
    }
}

TaskId SpawnPerCallScheduler::submit(
    Task*, int, const std::vector<TaskId>&) {
    throw std::logic_error("SpawnPerCallScheduler does not support asynchronous submission");
}

void SpawnPerCallScheduler::waitForAll() { return; }

const char* BusyWaitThreadPoolScheduler::name() const noexcept {
    return "BusyWaitThreadPoolScheduler";
}

BusyWaitThreadPoolScheduler::BusyWaitThreadPoolScheduler(
    int num_threads)
    : work_queues_mutexs(checkedThreadCount(num_threads)),
      num_threads(num_threads),
      threads_(this->num_threads),
      work_queues(this->num_threads) {
    try {
        for (int i = 0; i < this->num_threads; i++) {
            threads_[i] = std::thread(&BusyWaitThreadPoolScheduler::workerLoop,
                                      this, i);
        }
    } catch (...) {
        stopWorkers();
        throw;
    }
}

void BusyWaitThreadPoolScheduler::workerLoop(int idx) {
    WorkItem task;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(work_queues_mutexs[idx]);
            task = work_queues[idx];
        }
        if (task.work == nullptr && task.num_total_tasks == -1) break;
        if (task.work == nullptr && task.num_total_tasks == 0) continue;

        TaskId task_id;
        do {
            {
                std::lock_guard<std::mutex> lock(task_id_mutex);
                if (curTask >= task.num_total_tasks) break;
                task_id = curTask++;
            }
            try {
                task.work->executeTask(task_id, task.num_total_tasks);
            } catch (...) {
                std::lock_guard<std::mutex> lock(finish_mutex);
                if (!task_exception) task_exception = std::current_exception();
            }
        } while (true);

        {
            std::lock_guard<std::mutex> lock(work_queues_mutexs[idx]);
            work_queues[idx].work = nullptr;
            work_queues[idx].num_total_tasks = 0;
        }
        {
            std::lock_guard<std::mutex> lock(finish_mutex);
            finish_count++;
        }
    }
}

BusyWaitThreadPoolScheduler::~BusyWaitThreadPoolScheduler() {
    stopWorkers();
}

void BusyWaitThreadPoolScheduler::stopWorkers() {
    for (int i = 0; i < num_threads; i++) {
        {
            std::lock_guard<std::mutex> lock(work_queues_mutexs[i]);
            work_queues[i] = WorkItem{nullptr, -1};
        }
    }
    joinThreads(threads_);
}

void BusyWaitThreadPoolScheduler::execute(Task* work,
    int num_total_tasks) {
    validateTask(work, num_total_tasks);
    int maxThread = std::min(num_total_tasks, num_threads);
    {
        std::lock_guard<std::mutex> lock(task_id_mutex);
        curTask = 0;
    }
    {
        std::lock_guard<std::mutex> lock(finish_mutex);
        finish_count = 0;
        task_exception = nullptr;
    }
    {
        for (int i = 0; i < maxThread; i++) {
            {
                std::lock_guard<std::mutex> lock(work_queues_mutexs[i]);
                work_queues[i] = WorkItem{work, num_total_tasks};
            }
        }
    }
    std::exception_ptr error;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(finish_mutex);
            if (finish_count >= maxThread) {
                error = task_exception;
                break;
            }
        }
    }
    if (error) std::rethrow_exception(error);
}

void BusyWaitThreadPoolScheduler::waitForAll() { return; }

TaskId BusyWaitThreadPoolScheduler::submit(
    Task*, int, const std::vector<TaskId>&) {
    throw std::logic_error("BusyWaitThreadPoolScheduler does not support asynchronous submission");
}

const char* BlockingThreadPoolScheduler::name() const noexcept {
    return "BlockingThreadPoolScheduler";
}

BlockingThreadPoolScheduler::BlockingThreadPoolScheduler(int num_threads)
    : num_threads(checkedThreadCount(num_threads)),
      threads_(this->num_threads) {
    try {
        for (int i = 0; i < this->num_threads; i++) {
            threads_[i] = std::thread(&BlockingThreadPoolScheduler::workerLoop, this);
        }
    } catch (...) {
        stopWorkers();
        throw;
    }
}

void BlockingThreadPoolScheduler::workerLoop() {
    while (true) {
        WorkItem task;
        {
            std::unique_lock<std::mutex> lock(task_mutex);
            ++idle_workers;
            work_cv.wait(lock, [this] { return stopping || !task_queue.empty(); });
            --idle_workers;
            if (stopping && task_queue.empty()) return;
            task = task_queue.front();
            task_queue.pop_front();
        }

        std::exception_ptr error;
        try {
            task.work->executeTask(task.task_idx, task.num_total_tasks);
        } catch (...) {
            error = std::current_exception();
        }

        std::size_t wake_count;
        bool finished;
        {
            std::lock_guard<std::mutex> lock(task_mutex);
            auto& batch = task_states[task.task_id];
            if (error) {
                if (!batch.error) batch.error = error;
                if (!task_exception) task_exception = error;
            }
            const auto previous_size = task_queue.size();
            if (--batch.remaining == 0) completeBatch(task.task_id);
            wake_count = std::min(idle_workers, task_queue.size() - previous_size);
            finished = pending_batch_count == 0;
        }
        notifyWorkers(wake_count);
        if (finished) finished_task_cv.notify_all();
    }
}

BlockingThreadPoolScheduler::~BlockingThreadPoolScheduler() {
    waitUntilIdle();
    stopWorkers();
}

void BlockingThreadPoolScheduler::stopWorkers() {
    {
        std::lock_guard<std::mutex> lock(task_mutex);
        stopping = true;
    }
    work_cv.notify_all();
    joinThreads(threads_);
}

void BlockingThreadPoolScheduler::notifyWorkers(std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) work_cv.notify_one();
}

void BlockingThreadPoolScheduler::enqueueBatch(TaskId task_id) {
    const auto& batch = task_states[task_id];
    const auto previous_size = task_queue.size();
    try {
        for (int i = 0; i < batch.num_total_tasks; ++i) {
            task_queue.push_back(WorkItem{batch.work, i, batch.num_total_tasks, task_id});
        }
    } catch (...) {
        while (task_queue.size() > previous_size) task_queue.pop_back();
        throw;
    }
}

void BlockingThreadPoolScheduler::completeBatch(TaskId task_id) {
    // Link newly completed batches through their own state to avoid recursion
    // and allocations when propagating long chains of empty or failed batches.
    TaskId current = task_id;
    TaskId tail = task_id;
    task_states[current].next_completed = -1;
    while (current != -1) {
        auto& batch = task_states[current];
        batch.complete = true;
        --pending_batch_count;
        if (batch.error && !task_exception) task_exception = batch.error;

        for (TaskId successor_id : batch.successors) {
            auto& successor = task_states[successor_id];
            if (batch.error && !successor.error) successor.error = batch.error;
            if (--successor.unresolved_dependencies != 0) continue;

            if (successor.num_total_tasks > 0 && !successor.error) {
                try {
                    enqueueBatch(successor_id);
                    continue;
                } catch (...) {
                    successor.error = std::current_exception();
                }
            }
            successor.remaining = 0;
            successor.next_completed = -1;
            task_states[tail].next_completed = successor_id;
            tail = successor_id;
        }
        current = batch.next_completed;
    }
}

void BlockingThreadPoolScheduler::execute(Task* work, int num_total_tasks) {
    submit(work, num_total_tasks, {});
    waitForAll();
}

TaskId BlockingThreadPoolScheduler::submit(
    Task* work, int num_total_tasks, const std::vector<TaskId>& deps) {
    validateTask(work, num_total_tasks);
    std::unique_lock<std::mutex> lock(task_mutex);
    for (TaskId dep : deps) {
        if (dep < 0 || dep >= curTask) {
            throw std::invalid_argument("dependencies must name previously submitted batches");
        }
    }
    if (curTask == std::numeric_limits<TaskId>::max()) {
        throw std::overflow_error("task ID space exhausted");
    }

    const TaskId task_id = curTask;
    const auto previous_size = task_queue.size();
    task_states.emplace_back(work, num_total_tasks);
    auto& batch = task_states.back();
    std::size_t processed_dependencies = 0;
    try {
        for (TaskId dep : deps) {
            auto& predecessor = task_states[dep];
            if (predecessor.complete) {
                if (predecessor.error && !batch.error) batch.error = predecessor.error;
            } else {
                predecessor.successors.push_back(task_id);
                ++batch.unresolved_dependencies;
            }
            ++processed_dependencies;
        }
        if (batch.unresolved_dependencies == 0 && num_total_tasks > 0 && !batch.error) {
            enqueueBatch(task_id);
        }
    } catch (...) {
        for (std::size_t i = 0; i < processed_dependencies; ++i) {
            auto& predecessor = task_states[deps[i]];
            if (!predecessor.complete) predecessor.successors.pop_back();
        }
        task_states.pop_back();
        throw;
    }

    ++curTask;
    ++pending_batch_count;
    if (batch.unresolved_dependencies == 0 && (num_total_tasks == 0 || batch.error)) {
        completeBatch(task_id);
    }
    const auto wake_count = std::min(idle_workers, task_queue.size() - previous_size);
    const bool finished = pending_batch_count == 0;
    lock.unlock();
    notifyWorkers(wake_count);
    if (finished) finished_task_cv.notify_all();
    return task_id;
}

void BlockingThreadPoolScheduler::waitForAll() {
    std::exception_ptr error;
    {
        std::unique_lock<std::mutex> lock(task_mutex);
        finished_task_cv.wait(lock, [this] { return pending_batch_count == 0; });
        error = task_exception;
        task_exception = nullptr;
    }
    if (error) std::rethrow_exception(error);
}

void BlockingThreadPoolScheduler::waitUntilIdle() {
    std::unique_lock<std::mutex> lock(task_mutex);
    finished_task_cv.wait(lock, [this] { return pending_batch_count == 0; });
}

}  // namespace task_scheduler
