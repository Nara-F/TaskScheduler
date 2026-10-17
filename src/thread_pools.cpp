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

const char* SpawnPerCallScheduler::name() {
    return "SpawnPerCallScheduler";
}

SpawnPerCallScheduler::SpawnPerCallScheduler(int num_threads)
    : Scheduler(num_threads), num_threads(num_threads) {}

SpawnPerCallScheduler::~SpawnPerCallScheduler() {}

void SpawnPerCallScheduler::execute(Task* work, int num_total_tasks) {
    int maxThread = std::min(num_total_tasks, num_threads);
    std::vector<std::thread> threads(maxThread);

    auto thread_Run = [&](Task* work, int num_total_tasks) {
        TaskId task_id;
        do {
            {
                std::lock_guard<std::mutex> lock(task_id_mutex);
                task_id = curTask;
                curTask++;
            }
            if (task_id >= num_total_tasks) break;
            work->executeTask(task_id, num_total_tasks);
        } while (true);
    };

    SpawnPerCallScheduler::curTask = 0;
    for (int i = 0; i < maxThread; i++) {
        threads[i] = std::thread(thread_Run, work, num_total_tasks);
    }
    for (int i = 0; i < maxThread; i++) {
        threads[i].join();
    }
}

TaskId SpawnPerCallScheduler::submit(
    Task* work, int num_total_tasks, const std::vector<TaskId>& deps) {
    return 0;
}

void SpawnPerCallScheduler::waitForAll() { return; }

const char* BusyWaitThreadPoolScheduler::name() {
    return "BusyWaitThreadPoolScheduler";
}

BusyWaitThreadPoolScheduler::BusyWaitThreadPoolScheduler(
    int num_threads)
    : Scheduler(num_threads),
      work_queues_mutexs(std::max(1, num_threads)) {
    this->num_threads = std::max(1, num_threads);
    threads.resize(this->num_threads);
    work_queues.resize(this->num_threads);

    auto waiting = [&](int idx) {
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
    };

    for (int i = 0; i < this->num_threads; i++) {
        threads[i] = std::thread(waiting, i);
    }
}

BusyWaitThreadPoolScheduler::~BusyWaitThreadPoolScheduler() {
    for (int i = 0; i < num_threads; i++) {
        {
            std::lock_guard<std::mutex> lock(work_queues_mutexs[i]);
            work_queues[i] = WorkItem{nullptr, -1};
        }
    }
    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }
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

}  // namespace task_scheduler
