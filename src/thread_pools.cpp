#include "task_scheduler/thread_pools.hpp"

#include <algorithm>

namespace task_scheduler {

namespace {

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

const char* SequentialScheduler::name() const noexcept { return "SequentialScheduler"; }

SequentialScheduler::SequentialScheduler(int) {}

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

const char* SpawnPerCallScheduler::name() const noexcept {
    return "SpawnPerCallScheduler";
}

SpawnPerCallScheduler::SpawnPerCallScheduler(int num_threads)
    : num_threads(num_threads) {}

void SpawnPerCallScheduler::execute(Task* work, int num_total_tasks) {
    const int maxThread = std::min(num_total_tasks, num_threads);
    std::vector<std::thread> threads;
    threads.reserve(maxThread);
    ThreadJoiner joiner(threads);

    curTask = 0;
    for (int i = 0; i < maxThread; i++) {
        threads.emplace_back(&SpawnPerCallScheduler::workerLoop, this,
                             work, num_total_tasks);
    }
}

void SpawnPerCallScheduler::workerLoop(Task* work, int num_total_tasks) {
    while (true) {
        TaskId task_id;
        {
            std::lock_guard<std::mutex> lock(task_id_mutex);
            task_id = curTask++;
        }
        if (task_id >= num_total_tasks) return;
        work->executeTask(task_id, num_total_tasks);
    }
}

TaskId SpawnPerCallScheduler::submit(
    Task* work, int num_total_tasks, const std::vector<TaskId>& deps) {
    return 0;
}

void SpawnPerCallScheduler::waitForAll() { return; }

const char* BusyWaitThreadPoolScheduler::name() const noexcept {
    return "BusyWaitThreadPoolScheduler";
}

BusyWaitThreadPoolScheduler::BusyWaitThreadPoolScheduler(
    int num_threads)
    : work_queues_mutexs(std::max(1, num_threads)),
      num_threads(std::max(1, num_threads)),
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
                task_id = curTask;
                curTask++;
            }
            if (task_id >= task.num_total_tasks) break;
            task.work->executeTask(task_id, task.num_total_tasks);
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
    int maxThread = std::min(num_total_tasks, num_threads);
    {
        std::lock_guard<std::mutex> lock(task_id_mutex);
        curTask = 0;
    }
    {
        std::lock_guard<std::mutex> lock(finish_mutex);
        finish_count = 0;
    }
    {
        for (int i = 0; i < maxThread; i++) {
            {
                std::lock_guard<std::mutex> lock(work_queues_mutexs[i]);
                work_queues[i] = WorkItem{work, num_total_tasks};
            }
        }
    }
    while (true) {
        {
            std::lock_guard<std::mutex> lock(finish_mutex);
            if (finish_count >= maxThread) {
                break;
            }
        }
    }
}

void BusyWaitThreadPoolScheduler::waitForAll() { return; }

TaskId BusyWaitThreadPoolScheduler::submit(
    Task* work, int num_total_tasks, const std::vector<TaskId>& deps) {
    return 0;
}

const char* BlockingThreadPoolScheduler::name() const noexcept {
    return "BlockingThreadPoolScheduler";
}

BlockingThreadPoolScheduler::BlockingThreadPoolScheduler(
    int num_threads)
    : num_threads(std::max(num_threads, 1)),
      threads_(this->num_threads),
      status_call(this->num_threads, 0),
      status_mutex(std::max(num_threads, 1)),
      cv(std::max(num_threads, 1)) {
    try {
        for (int i = 0; i < this->num_threads; i++) {
            threads_[i] = std::thread(&BlockingThreadPoolScheduler::workerLoop,
                                      this, i);
        }
    } catch (...) {
        stopWorkers();
        throw;
    }
}

void BlockingThreadPoolScheduler::workerLoop(int idx) {
    while (true) {
        std::unique_lock<std::mutex> lock(status_mutex[idx]);
        cv[idx].wait(lock, [&] {
            return status_call[idx] == 1 || status_call[idx] == -1;
        });
        auto sta = status_call[idx];
        lock.unlock();
        if (sta == 1) {
            WorkItem task;
            do {
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    if (!task_queue.empty()) {
                        task = task_queue.front();
                        task_queue.pop_front();
                    } else {
                        {
                            std::lock_guard<std::mutex> lock(
                                status_mutex[idx]);
                            if (status_call[idx] == 1) status_call[idx] = 0;
                        }
                        {
                            std::lock_guard<std::mutex> lock(
                                finished_task_mutex);
                            if (pending_task_count == 0) {
                                finished_task_cv.notify_all();
                            }
                        }
                        break;
                    }
                }
                task.work->executeTask(task.task_idx, task.num_total_tasks);
                bool batch_finished;
                {
                    std::lock_guard<std::mutex> lock(task_mutex);
                    batch_finished = --task_remaining[task.task_id] == 0;
                }
                if (batch_finished) {
                    {
                        std::lock_guard<std::mutex> lock(set_mutex);
                        finished_tasks.insert(task.task_id);
                    }
                    {
                        std::lock_guard<std::mutex> lock(pending_set_mutex);
                        for (auto it = pending_tasks.begin();
                             it != pending_tasks.end();) {
                            if (enqueueIfReady(*it))
                                it = pending_tasks.erase(it);
                            else
                                ++it;
                        }
                    }
                }
                {
                    std::lock_guard<std::mutex> lock(finished_task_mutex);
                    if (--pending_task_count == 0) {
                        finished_task_cv.notify_all();
                    }
                }
            } while (true);
        } else if (sta == -1) {
            break;
        }
    }
}

BlockingThreadPoolScheduler::~BlockingThreadPoolScheduler() {
    waitUntilIdle();
    stopWorkers();
}

void BlockingThreadPoolScheduler::stopWorkers() {
    for (int i = 0; i < num_threads; i++) {
        std::unique_lock<std::mutex> lock(status_mutex[i]);
        status_call[i] = -1;
        cv[i].notify_all();
    }
    joinThreads(threads_);
}

bool BlockingThreadPoolScheduler::enqueueIfReady(const PendingTask& task) {
    auto& work = task.work;
    auto& num_total_tasks = task.num_total_tasks;
    auto& deps = task.deps;
    auto& task_id = task.task_id;
    bool deps_completed = true;
    for (auto dep : deps) {
        std::lock_guard<std::mutex> lock(set_mutex);
        if (!finished_tasks.count(dep)) {
            deps_completed = false;
            break;
        }
    }
    if (deps_completed) {
        if (num_total_tasks == 0) {
            {
                std::lock_guard<std::mutex> lock(set_mutex);
                finished_tasks.insert(task_id);
            }
            {
                std::lock_guard<std::mutex> lock(finished_task_mutex);
                if (--pending_task_count == 0) {
                    finished_task_cv.notify_all();
                }
            }
            return true;
        }
        for (int i = 0; i < num_total_tasks; i++) {
            std::lock_guard<std::mutex> lock(queue_mutex);
            task_queue.push_back(WorkItem{work, i, num_total_tasks, task_id});
        }
        for (int i = 0; i < num_threads; i++) {
            std::unique_lock<std::mutex> lock(status_mutex[i]);
            status_call[i] = 1;
            cv[i].notify_all();
        }
        return true;
    }
    return false;
}
void BlockingThreadPoolScheduler::execute(Task* work,
    int num_total_tasks) {
    submit(work, num_total_tasks, {});
    waitForAll();
}

TaskId BlockingThreadPoolScheduler::submit(
    Task* work, int num_total_tasks, const std::vector<TaskId>& deps) {

    TaskId task_id;
    {
        std::lock_guard<std::mutex> lock(task_mutex);
        task_id = curTask++;
        task_remaining.push_back(num_total_tasks);
    }

    {
        std::lock_guard<std::mutex> lock(finished_task_mutex);
        pending_task_count += num_total_tasks == 0 ? 1 : num_total_tasks;
    }
    std::unique_lock<std::mutex> lockP(pending_set_mutex);
    PendingTask task{work, num_total_tasks, deps, task_id};
    if (!enqueueIfReady(task)) {
        pending_tasks.push_back(task);
    }

    return task_id;
}

void BlockingThreadPoolScheduler::waitForAll() {
    waitUntilIdle();
}

void BlockingThreadPoolScheduler::waitUntilIdle() {
    std::unique_lock<std::mutex> lock(finished_task_mutex);
    finished_task_cv.wait(lock, [&] { return pending_task_count == 0; });
    return;
}

}  // namespace task_scheduler
