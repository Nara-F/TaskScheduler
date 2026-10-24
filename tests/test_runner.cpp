#include "task_scheduler/thread_pools.hpp"
#include "test_cases.hpp"
#include "test_clock.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using namespace task_scheduler;

namespace {
std::unique_ptr<Scheduler> makeScheduler(const std::string& kind, int threads) {
    if (kind == "sequential") return std::unique_ptr<Scheduler>(new SequentialScheduler(threads));
    if (kind == "spawn") return std::unique_ptr<Scheduler>(new SpawnPerCallScheduler(threads));
    if (kind == "busy-wait") return std::unique_ptr<Scheduler>(new BusyWaitThreadPoolScheduler(threads));
    if (kind == "blocking") return std::unique_ptr<Scheduler>(new BlockingThreadPoolScheduler(threads));
    throw std::runtime_error("unknown scheduler: " + kind);
}

// Unsigned arithmetic defines overflow; storing the result prevents dead-code removal.
std::uint64_t compute(std::uint64_t seed, std::uint64_t iterations) {
    std::uint64_t value = seed + UINT64_C(0x9e3779b97f4a7c15);
    for (std::uint64_t i = 0; i < iterations; ++i) {
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        value *= UINT64_C(2685821657736338717);
    }
    return value;
}

struct BatchSpec {
    std::vector<int> dependencies;
    std::vector<std::uint64_t> weights;
};

std::vector<BatchSpec> readWorkload(const std::string& path) {
    std::ifstream input(path);
    int count;
    if (!(input >> count) || count < 1) throw std::runtime_error("invalid workload header");
    std::vector<BatchSpec> specs(count);
    for (int batch = 0; batch < count; ++batch) {
        int tasks, dependencies;
        if (!(input >> tasks >> dependencies) || tasks < 0 || dependencies < 0)
            throw std::runtime_error("invalid batch header");
        specs[batch].dependencies.resize(dependencies);
        for (auto& predecessor : specs[batch].dependencies) {
            if (!(input >> predecessor) || predecessor < 0 || predecessor >= batch)
                throw std::runtime_error("workload is not in topological order");
        }
        specs[batch].weights.resize(tasks);
        for (auto& weight : specs[batch].weights)
            if (!(input >> weight)) throw std::runtime_error("missing task weight");
    }
    std::string trailing;
    if (input >> trailing) throw std::runtime_error("unexpected workload data");
    return specs;
}

class CheckedBatch : public Task {
   public:
    const std::vector<std::uint64_t>& weights;
    const int batch;
    std::vector<CheckedBatch*> predecessors;
    std::vector<std::atomic<int>> calls;
    std::vector<std::atomic<std::uint64_t>> output;
    std::vector<std::uint64_t> expected;
    std::atomic<int> completed{0};
    std::atomic<bool> valid{true};

    CheckedBatch(const BatchSpec& spec, int index)
        : weights(spec.weights), batch(index), calls(weights.size()),
          output(weights.size()), expected(weights.size()) {
        for (std::size_t i = 0; i < weights.size(); ++i) {
            calls[i].store(0);
            output[i].store(0);
            expected[i] = compute(seed(i), weights[i]);
        }
    }

    std::uint64_t seed(std::size_t index) const {
        return (static_cast<std::uint64_t>(batch) << 32) ^ index;
    }

    void executeTask(int index, int total) override {
        if (total != static_cast<int>(weights.size()) || index < 0 || index >= total) {
            valid.store(false);
            return;
        }
        for (auto* predecessor : predecessors) {
            if (predecessor->completed.load() != static_cast<int>(predecessor->weights.size()))
                valid.store(false);
        }
        calls[index].fetch_add(1);
        output[index].store(compute(seed(index), weights[index]));
        completed.fetch_add(1);
    }

    bool passed() const {
        if (!valid.load() || completed.load() != static_cast<int>(weights.size())) return false;
        for (std::size_t i = 0; i < weights.size(); ++i)
            if (calls[i].load() != 1 || output[i].load() != expected[i]) return false;
        return true;
    }
};

scheduler_tests::TestResults runCustom(Scheduler* scheduler,
                                     const std::vector<BatchSpec>& specs, bool async) {
    std::vector<std::unique_ptr<CheckedBatch>> batches;
    for (std::size_t i = 0; i < specs.size(); ++i) {
        batches.emplace_back(new CheckedBatch(specs[i], static_cast<int>(i)));
        for (int predecessor : specs[i].dependencies) {
            // Empty batches have no callback; check their effective predecessors.
            if (specs[predecessor].weights.empty()) {
                const auto& expanded = batches[predecessor]->predecessors;
                batches.back()->predecessors.insert(batches.back()->predecessors.end(),
                                                    expanded.begin(), expanded.end());
            } else {
                batches.back()->predecessors.push_back(batches[predecessor].get());
            }
        }
        auto& deps = batches.back()->predecessors;
        std::sort(deps.begin(), deps.end(), std::less<CheckedBatch*>());
        deps.erase(std::unique(deps.begin(), deps.end()), deps.end());
    }
    std::vector<TaskId> ids(specs.size());
    double start = CycleTimer::currentSeconds();
    for (std::size_t i = 0; i < specs.size(); ++i) {
        if (async) {
            std::vector<TaskId> dependencies;
            for (int predecessor : specs[i].dependencies) dependencies.push_back(ids[predecessor]);
            ids[i] = scheduler->submit(batches[i].get(), static_cast<int>(specs[i].weights.size()), dependencies);
        } else {
            scheduler->execute(batches[i].get(), static_cast<int>(specs[i].weights.size()));
        }
    }
    scheduler->waitForAll();
    double elapsed = CycleTimer::currentSeconds() - start;
    bool passed = true;
    for (const auto& batch : batches) passed = batch->passed() && passed;
    return {passed, elapsed};
}

double median(std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    const std::size_t middle = samples.size() / 2;
    return samples.size() % 2 ? samples[middle]
                             : (samples[middle - 1] + samples[middle]) / 2;
}

std::size_t idleWorkers(Scheduler* scheduler) {
    const std::vector<std::thread>* workers = nullptr;
    if (auto* pool = dynamic_cast<BusyWaitThreadPoolScheduler*>(scheduler))
        workers = &pool->threads;
    if (auto* pool = dynamic_cast<BlockingThreadPoolScheduler*>(scheduler))
        workers = &pool->threads;
    if (!workers) return 0;
    return std::count_if(workers->begin(), workers->end(),
                         [](const std::thread& worker) { return worker.joinable(); });
}
}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--list") {
            for (const auto& test : scheduler_tests::builtinCases())
                std::cout << test.name << '\t' << (test.asynchronous ? "async" : "sync") << '\n';
            return 0;
        }
        if (argc != 7) throw std::runtime_error("Use tests/run_tests.py as the unified entry point");
        const std::string mode = argv[1], argument = argv[2], kind = argv[3];
        int threads = std::stoi(argv[4]), iterations = std::stoi(argv[5]);
        const std::string execution = argv[6];
        if (threads < 1 || iterations < 1) throw std::runtime_error("threads and iterations must be positive");
        const bool async = execution == "async";
        if (execution != "async" && execution != "sync") throw std::runtime_error("invalid execution mode");
        if (async && kind != "blocking") throw std::runtime_error("async requires the blocking scheduler");
        const scheduler_tests::TestCase* builtin = nullptr;
        std::vector<BatchSpec> specs;
        int idle_ms = 0;
        if (mode == "--builtin") {
            for (const auto& test : scheduler_tests::builtinCases())
                if (argument == test.name) builtin = &test;
            if (!builtin) throw std::runtime_error("unknown builtin case: " + argument);
            if (builtin->asynchronous != async) throw std::runtime_error("incorrect builtin execution mode");
        } else if (mode == "--custom") {
            specs = readWorkload(argument);
        } else if (mode == "--idle") {
            if (async) throw std::runtime_error("idle profiling requires sync mode");
            idle_ms = std::stoi(argument);
            if (idle_ms < 1) throw std::runtime_error("idle duration must be positive");
            specs.resize(1);
            specs.front().weights.assign(8, 500000);
        } else {
            throw std::runtime_error("unknown runner mode");
        }
        std::vector<double> times;
        std::vector<double> idle_cpu_times, idle_wall_times;
        std::size_t idle_workers = 0;
        for (int iteration = 0; iteration < iterations; ++iteration) {
            auto scheduler = makeScheduler(kind, threads);
            auto result = builtin ? builtin->run(scheduler.get()) : runCustom(scheduler.get(), specs, async);
            if (!result.passed) throw std::runtime_error("correctness check failed at iteration " + std::to_string(iteration + 1));
            times.push_back(result.time * 1000.0);
            if (idle_ms) {
                idle_workers = idleWorkers(scheduler.get());
                const auto cpu_start = std::clock();
                const auto wall_start = CycleTimer::currentSeconds();
                std::this_thread::sleep_for(std::chrono::milliseconds(idle_ms));
                const auto wall_elapsed = CycleTimer::currentSeconds() - wall_start;
                const auto cpu_end = std::clock();
                if (cpu_start == std::clock_t(-1) || cpu_end == std::clock_t(-1))
                    throw std::runtime_error("process CPU time is unavailable");
                idle_cpu_times.push_back(1000.0 * (cpu_end - cpu_start) / CLOCKS_PER_SEC);
                idle_wall_times.push_back(wall_elapsed * 1000.0);
            }
        }
        std::sort(times.begin(), times.end());
        std::cout << "PASS " << (builtin ? argument : (idle_ms ? "idle" : "custom")) << " scheduler=" << kind
                  << " mode=" << (async ? "async-dag" : "sync-topological")
                  << " threads=" << threads << " iterations=" << iterations
                  << std::fixed << std::setprecision(3)
                  << " min_ms=" << times.front() << " median_ms=" << median(times);
        if (idle_ms) {
            std::cout << " idle_workers=" << idle_workers
                      << " idle_cpu_median_ms=" << median(idle_cpu_times)
                      << " idle_wall_median_ms=" << median(idle_wall_times);
        }
        std::cout << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
