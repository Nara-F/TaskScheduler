#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <set>
#include <thread>

#include "test_clock.hpp"
#include "test_cases.hpp"

namespace scheduler_tests {
using namespace task_scheduler;

/*
Sync tests
==========
TestResults pingPongEqualTest(Scheduler *t);
TestResults pingPongUnequalTest(Scheduler *t);
TestResults superLightTest(Scheduler *t);
TestResults superSuperLightTest(Scheduler *t);
TestResults recursiveFibonacciTest(Scheduler* t);
TestResults mathOperationsInTightForLoopTest(Scheduler* t);
TestResults mathOperationsInTightForLoopFanInTest(Scheduler* t);
TestResults mathOperationsInTightForLoopReductionTreeTest(Scheduler* t);
TestResults spinBetweenRunCallsTest(Scheduler *t);
TestResults mandelbrotChunkedTest(Scheduler* t);

Async with dependencies tests
=============================
TestResults pingPongEqualAsyncTest(Scheduler *t);
TestResults pingPongUnequalAsyncTest(Scheduler *t);
TestResults superLightAsyncTest(Scheduler *t);
TestResults superSuperLightAsyncTest(Scheduler *t);
TestResults recursiveFibonacciAsyncTest(Scheduler* t);
TestResults mathOperationsInTightForLoopAsyncTest(Scheduler* t);
TestResults mathOperationsInTightForLoopFanInAsyncTest(Scheduler* t);
TestResults mathOperationsInTightForLoopReductionTreeAsyncTest(Scheduler* t);
TestResults spinBetweenRunCallsAsyncTest(Scheduler *t);
TestResults mandelbrotChunkedAsyncTest(Scheduler* t);
TestResults simpleRunDepsTest(Scheduler *t);
*/

class SimpleMultiplyTask : public Task {
   public:
    int num_elements_;
    int* array_;

    SimpleMultiplyTask(int num_elements, int* array)
        : num_elements_(num_elements), array_(array) {}
    ~SimpleMultiplyTask() {}

    static inline int multiply_task(int iters, int input) {
        int accumulator = 1;
        for (int i = 0; i < iters; ++i) {
            accumulator *= input;
        }
        return accumulator;
    }

    void executeTask(int task_id, int num_total_tasks) {
        // handle case where num_elements is not evenly divisible by
        // num_total_tasks
        int elements_per_task =
            (num_elements_ + num_total_tasks - 1) / num_total_tasks;
        int start_el = elements_per_task * task_id;
        int end_el = std::min(start_el + elements_per_task, num_elements_);

        for (int i = start_el; i < end_el; i++)
            array_[i] = multiply_task(3, array_[i]);
    }
};

/*
 * Each task computes an output list of accumulated counters. Each counter
 * is incremented in a tight for loop. The `equal_work_` field ensures that
 * each element of the output array requires a different amount of computation.
 */
class PingPongTask : public Task {
   public:
    int num_elements_;
    int* input_array_;
    int* output_array_;
    bool equal_work_;
    int iters_;

    PingPongTask(int num_elements, int* input_array, int* output_array,
                 bool equal_work, int iters)
        : num_elements_(num_elements),
          input_array_(input_array),
          output_array_(output_array),
          equal_work_(equal_work),
          iters_(iters) {}
    ~PingPongTask() {}

    static inline int ping_pong_iters(int i, int num_elements, int iters) {
        int max_iters = 2 * iters;
        return std::floor(
            (static_cast<float>(num_elements - i) / num_elements) * max_iters);
    }

    static inline int ping_pong_work(int iters, int input) {
        int accum = input;
        for (int j = 0; j < iters; j++) {
            if (j % 2 == 0) accum++;
        }
        return accum;
    }

    void executeTask(int task_id, int num_total_tasks) {
        // handle case where num_elements is not evenly divisible by
        // num_total_tasks
        int elements_per_task =
            (num_elements_ + num_total_tasks - 1) / num_total_tasks;
        int start_el = elements_per_task * task_id;
        int end_el = std::min(start_el + elements_per_task, num_elements_);

        if (equal_work_) {
            for (int i = start_el; i < end_el; i++)
                output_array_[i] = ping_pong_work(iters_, input_array_[i]);
        } else {
            for (int i = start_el; i < end_el; i++) {
                int el_iters = ping_pong_iters(i, num_elements_, iters_);
                output_array_[i] = ping_pong_work(el_iters, input_array_[i]);
            }
        }
    }
};

/*
 * Each task computes and writes the idx-th fibonacci number into the
 * position output[task_id].
 */
class RecursiveFibonacciTask : public Task {
   public:
    int idx_;
    int* output_;
    RecursiveFibonacciTask(int idx, int* output) : idx_(idx), output_(output) {}
    ~RecursiveFibonacciTask() {}

    // very slow recursive implementation of the nth fibbonacci number
    int slowFn(int n) {
        if (n < 2) return 1;
        return slowFn(n - 1) + slowFn(n - 2);
    }

    void executeTask(int task_id, int num_total_tasks) {
        output_[task_id] = slowFn(idx_);
    }
};

/*
 * Each task copies its task id into the output.
 */
class LightTask : public Task {
   public:
    int* output_;
    LightTask(int* output) : output_(output) {}
    ~LightTask() {}

    void executeTask(int task_id, int num_total_tasks) {
        output_[task_id] = task_id;
    }
};

/*
 * Each task performs a sequence of exp, log, and multiplication
 * operations in a tight for loop.
 */
class MathOperationsInTightForLoopTask : public Task {
   public:
    float* output_;
    int array_size_;
    MathOperationsInTightForLoopTask(int array_size, float* output) {
        array_size_ = array_size;
        output_ = output;
    }
    ~MathOperationsInTightForLoopTask() {}

    void executeTask(int task_id, int num_total_tasks) {
        int elements_per_task = array_size_ / num_total_tasks;
        int start = task_id * elements_per_task;
        int end = std::min(start + elements_per_task, array_size_);
        if (array_size_ - end < elements_per_task) {
            end = array_size_;
        }

        for (int i = start; i < end; i++) {
            output_[i] = 0.0;
        }

        for (int i = start; i < end; i++) {
            for (int j = 1; j < 151; j++) {
                float val;
                if (i % 3 == 0) {
                    val = exp(j / 100.);
                } else if (i % 3 == 1) {
                    val = log(j * 2.);
                } else {
                    val = j * 6;
                }
                output_[i] += val;
            }
        }
    }
};

/*
 * Each task computes the sum of `num_to_reduce_` input arrays.
 */
class ReduceTask : public Task {
   public:
    float* input_;
    float* output_;
    int array_size_;
    int num_to_reduce_;
    ReduceTask(int array_size, int num_to_reduce, float* input, float* output) {
        array_size_ = array_size;
        num_to_reduce_ = num_to_reduce;
        input_ = input;
        output_ = output;
    }

    void executeTask(int task_id, int num_total_tasks) {
        for (int i = 0; i < array_size_; i++) {
            output_[i] = 0.0;
            for (int j = 0; j < num_to_reduce_; j++) {
                output_[i] += input_[(j * array_size_) + i];
            }
        }
    }
};

/*
 * Each task computes a number of rows of the output Mandelbrot image.
 * These rows either form a contiguous chunk of the image (if
 * interleave is false) or are interleaved throughout the image.
 */
class MandelbrotTask : public Task {
   public:
    typedef struct {
        float x0, x1;
        float y0, y1;
        int width;
        int height;
        int max_iterations;
        int* output;
    } MandelArgs;

    MandelArgs* args_;
    int interleave_;

    MandelbrotTask(MandelArgs* args, int interleave)
        : args_(args), interleave_(interleave) {}
    ~MandelbrotTask() {}

    // helper function used by Mandelbrot computations
    inline int mandel(float c_re, float c_im, int count) {
        float z_re = c_re, z_im = c_im;
        int i;
        for (i = 0; i < count; ++i) {
            if (z_re * z_re + z_im * z_im > 4.f) break;

            float new_re = z_re * z_re - z_im * z_im;
            float new_im = 2.f * z_re * z_im;
            z_re = c_re + new_re;
            z_im = c_im + new_im;
        }

        return i;
    }

    void mandelbrotSerial(float x0, float y0, float x1, float y1, int width,
                          int height, int startRow, int totalRows,
                          int max_iterations, int output[]) {
        float dx = (x1 - x0) / width;
        float dy = (y1 - y0) / height;

        int endRow = startRow + totalRows;

        for (int j = startRow; j < endRow; j++) {
            for (int i = 0; i < width; ++i) {
                float x = x0 + i * dx;
                float y = y0 + j * dy;

                int index = (j * width + i);
                output[index] = mandel(x, y, max_iterations);
            }
        }
    }

    void mandelbrotSerial_interleaved(float x0, float y0, float x1, float y1,
                                      int width, int height, int startRow,
                                      int totalRows, int interleaving,
                                      int max_iterations, int output[]) {
        float dx = (x1 - x0) / width;
        float dy = (y1 - y0) / height;

        int endRow = startRow + totalRows;

        for (int j = startRow; j < endRow; j += interleaving) {
            for (int i = 0; i < width; ++i) {
                float x = x0 + i * dx;
                float y = y0 + j * dy;

                int index = (j * width + i);
                output[index] = mandel(x, y, max_iterations);
            }
        }
    }

    void executeTask(int task_id, int num_total_tasks) {
        int rowsPerTask = args_->height / num_total_tasks;

        if (interleave_ == 1) {
            mandelbrotSerial_interleaved(
                args_->x0, args_->y0, args_->x1, args_->y1, args_->width,
                args_->height, task_id, args_->height - task_id,
                num_total_tasks, args_->max_iterations, args_->output);
        } else {
            mandelbrotSerial(args_->x0, args_->y0, args_->x1, args_->y1,
                             args_->width, args_->height, task_id * rowsPerTask,
                             rowsPerTask, args_->max_iterations, args_->output);
        }
    }
};

/*
 * Each task sleeps for the prescribed amount of time, and then
 * print a message to stdout.
 */
class SleepTask : public Task {
   public:
    int sleep_period_;
    SleepTask(int sleep_period) { sleep_period_ = sleep_period; }
    ~SleepTask() {}

    void executeTask(int task_id, int num_total_tasks) {
        std::this_thread::sleep_for(std::chrono::seconds(sleep_period_));
        printf("Running SleepTask (%ds), task %d\n", sleep_period_, task_id);
    }
};

/*
 * This task sets its "done" flag when the following conditions are met:
 *  - All dependencies have their "done" flag set prior to the first
      task of this bulk task running.
 *  - The last task of this bulk task has finished running.
 *
 * Intended for building correctness tests of arbitrary task graphs.
 */
class StrictDependencyTask : public Task {
   private:
    const std::vector<bool*>& in_flags_;
    bool* out_flag_;
    std::atomic<int> tasks_started_;
    std::atomic<int> tasks_ended_;
    bool satisfied_;

   public:
    StrictDependencyTask(const std::vector<bool*>& in_flags, bool* out_flag)
        : in_flags_(in_flags),
          out_flag_(out_flag),
          tasks_started_(0),
          tasks_ended_(0),
          satisfied_(false) {}

    void executeTask(int task_id, int num_total_tasks) {
        int entry_id = tasks_started_++;
        // First task to be execute: check if all dependencies are met.
        if (entry_id == 0) {
            satisfied_ = depsMet();
        }

        doWork(task_id, num_total_tasks);

        int exit_id = ++tasks_ended_;
        // Last task to finish running: mark this entire task launch as done.
        if (exit_id == num_total_tasks) {
            *out_flag_ = satisfied_;
        }
    }

    bool depsMet() {
        for (bool* b : in_flags_) {
            if (*b == false) {
                return false;
            }
        }
        return true;
    }

    void doWork(int task_id, int num_total_tasks) {
        // Using this as a proxy for actual work.
        std::this_thread::sleep_for(
            std::chrono::microseconds((1 + (task_id % 10))));
    }
    ~StrictDependencyTask() {}
};

/*
 * ==================================================================
 *   Begin test definitions
 * ==================================================================
 */

/*
 * Computation: simpleTest is designed as solely a correctness test to be used
 * for early debugging. It is a small test - it launches 2 bulk task launches
 * with 3 tasks each. The computation done by each bulk task launch is a small
 * number of multiplies on each element of the input array. The array
 * modification is done in-place. The array has 6 total elements, divided into
 * 2 elements per task.
 *
 */
TestResults simpleTest(Scheduler* t, bool do_async) {
    int num_elements_per_task = 2;
    int num_tasks = 3;
    int num_elements = num_elements_per_task * num_tasks;
    int num_bulk_task_launches = 2;

    int* array = new int[num_elements];

    for (int i = 0; i < num_elements; i++) {
        array[i] = i + 1;
    }

    SimpleMultiplyTask first = SimpleMultiplyTask(num_elements, array);
    SimpleMultiplyTask second = SimpleMultiplyTask(num_elements, array);

    // Run the test
    double start_time = CycleTimer::currentSeconds();
    if (do_async) {
        std::vector<TaskId> firstDeps;
        TaskId first_task_id =
            t->submit(&first, num_tasks, firstDeps);
        std::vector<TaskId> secondDeps;
        secondDeps.push_back(first_task_id);
        t->submit(&second, num_tasks, secondDeps);
        t->waitForAll();
    } else {
        t->execute(&first, num_tasks);
        t->execute(&second, num_tasks);
    }
    double end_time = CycleTimer::currentSeconds();

    // Correctness validation
    TestResults results;
    results.passed = true;

    for (int i = 0; i < num_elements; i++) {
        int value = i + 1;

        for (int j = 0; j < num_bulk_task_launches; j++)
            value = SimpleMultiplyTask::multiply_task(3, value);

        int expected = value;
        if (array[i] != expected) {
            results.passed = false;
            printf("%d: %d expected=%d\n", i, array[i], expected);
            break;
        }
    }
    results.time = end_time - start_time;

    delete[] array;

    return results;
}

TestResults simpleTestSync(Scheduler* t) { return simpleTest(t, false); }

TestResults simpleTestAsync(Scheduler* t) { return simpleTest(t, true); }

/*
 * Computation: pingPongTest launches 400 bulk task launches with 64 tasks each.
 * The computation done by each bulk task launch takes as input a buffer of size
 * `num_elements` as input, performs an elementwise computation, and writes to
 * an equal-sized output buffer. By default, the elementwise computation defined
 * by class PingPongTask increments the input element `base_iters` times and
 * writes the result to the corresponding location in the output buffer. If
 * `equal_work` is true, each element requires the same amount of work to
 * compute. Otherwise, the lower index elements require more work to compute,
 * and each task therefore does a different amount of work. Each task takes as
 * input the output buffer of the previous task, so each task depends on the
 * task before it. When base_iters is sufficiently small, the overhead of
 * launching threads is non-trival and so there are benefits to a thread pool.
 * The amount of computation per task is controlled using `num_elements` and
 * `base_iters`, because each task gets `num_elements` / `num_tasks` elements
 * and does O(base_iters) work per element.
 */
TestResults pingPongTest(Scheduler* t, bool equal_work, bool do_async,
                         int num_elements, int base_iters) {
    int num_tasks = 64;
    int num_bulk_task_launches = 400;

    int* input = new int[num_elements];
    int* output = new int[num_elements];

    // Init input
    for (int i = 0; i < num_elements; i++) {
        input[i] = i;
        output[i] = 0;
    }

    // Ping-pong input and output buffers with all the
    // back-to-back task launches
    std::vector<PingPongTask*> runnables(num_bulk_task_launches);
    for (int i = 0; i < num_bulk_task_launches; i++) {
        if (i % 2 == 0)
            runnables[i] = new PingPongTask(num_elements, input, output,
                                            equal_work, base_iters);
        else
            runnables[i] = new PingPongTask(num_elements, output, input,
                                            equal_work, base_iters);
    }

    // Run the test
    double start_time = CycleTimer::currentSeconds();
    TaskId prev_task_id;
    for (int i = 0; i < num_bulk_task_launches; i++) {
        if (do_async) {
            std::vector<TaskId> deps;
            if (i > 0) {
                deps.push_back(prev_task_id);
            }
            prev_task_id = t->submit(runnables[i], num_tasks, deps);
        } else {
            t->execute(runnables[i], num_tasks);
        }
    }
    if (do_async) t->waitForAll();
    double end_time = CycleTimer::currentSeconds();

    // Correctness validation
    TestResults results;
    results.passed = true;

    // Number of ping-pongs determines which buffer to look at for the results
    int* buffer = (num_bulk_task_launches % 2 == 1) ? output : input;

    for (int i = 0; i < num_elements; i++) {
        int value = i;
        for (int j = 0; j < num_bulk_task_launches; j++) {
            int iters = (!equal_work) ? PingPongTask::ping_pong_iters(
                                            i, num_elements, base_iters)
                                      : base_iters;
            value = PingPongTask::ping_pong_work(iters, value);
        }

        int expected = value;
        if (buffer[i] != expected) {
            results.passed = false;
            printf("%d: %d expected=%d\n", i, buffer[i], expected);
            break;
        }
    }
    results.time = end_time - start_time;

    delete[] input;
    delete[] output;
    for (int i = 0; i < num_bulk_task_launches; i++) delete runnables[i];

    return results;
}

TestResults superSuperLightTest(Scheduler* t) {
    int num_elements = 32 * 1024;
    int base_iters = 0;
    return pingPongTest(t, true, false, num_elements, base_iters);
}

TestResults superSuperLightAsyncTest(Scheduler* t) {
    int num_elements = 32 * 1024;
    int base_iters = 0;
    return pingPongTest(t, true, true, num_elements, base_iters);
}

TestResults superLightTest(Scheduler* t) {
    int num_elements = 32 * 1024;
    int base_iters = 32;
    return pingPongTest(t, true, false, num_elements, base_iters);
}

TestResults superLightAsyncTest(Scheduler* t) {
    int num_elements = 32 * 1024;
    int base_iters = 32;
    return pingPongTest(t, true, true, num_elements, base_iters);
}

TestResults pingPongEqualTest(Scheduler* t) {
    int num_elements = 512 * 1024;
    int base_iters = 32;
    return pingPongTest(t, true, false, num_elements, base_iters);
}

TestResults pingPongUnequalTest(Scheduler* t) {
    int num_elements = 512 * 1024;
    int base_iters = 32;
    return pingPongTest(t, false, false, num_elements, base_iters);
}

TestResults pingPongEqualAsyncTest(Scheduler* t) {
    int num_elements = 512 * 1024;
    int base_iters = 32;
    return pingPongTest(t, true, true, num_elements, base_iters);
}

TestResults pingPongUnequalAsyncTest(Scheduler* t) {
    int num_elements = 512 * 1024;
    int base_iters = 32;
    return pingPongTest(t, false, true, num_elements, base_iters);
}

/*
 * Computation: The following tests compute Fibonacci numbers using
 * recursion. Since the tasks are compute intensive, the tests show
 * the benefit of parallelism.
 */
TestResults recursiveFibonacciTestBase(Scheduler* t, bool do_async) {
    int num_tasks = 256;
    int num_bulk_task_launches = 30;
    int fib_index = 25;

    int* task_output = new int[num_tasks];
    for (int i = 0; i < num_tasks; i++) {
        task_output[i] = 0;
    }

    std::vector<RecursiveFibonacciTask*> fib_tasks(num_bulk_task_launches);
    for (int i = 0; i < num_bulk_task_launches; i++) {
        fib_tasks[i] = new RecursiveFibonacciTask(fib_index, task_output);
    }

    double start_time = CycleTimer::currentSeconds();
    if (do_async) {
        std::vector<TaskId> deps;  // Call submit without dependencies
        for (int i = 0; i < num_bulk_task_launches; i++) {
            t->submit(fib_tasks[i], num_tasks, deps);
        }
        t->waitForAll();
    } else {
        for (int i = 0; i < num_bulk_task_launches; i++) {
            t->execute(fib_tasks[i], num_tasks);
        }
    }
    double end_time = CycleTimer::currentSeconds();

    // Validate correctness
    TestResults result;
    result.passed = true;
    for (int i = 0; i < num_tasks; i++) {
        if (task_output[i] != 121393) {
            printf("%d\n", task_output[i]);
            result.passed = false;
            break;
        }
    }
    result.time = end_time - start_time;

    delete[] task_output;
    for (int i = 0; i < num_bulk_task_launches; i++) {
        delete fib_tasks[i];
    }

    return result;
}

TestResults recursiveFibonacciTest(Scheduler* t) {
    return recursiveFibonacciTestBase(t, false);
}

TestResults recursiveFibonacciAsyncTest(Scheduler* t) {
    return recursiveFibonacciTestBase(t, true);
}

/*
 * Computation: The following tests perform exps, logs, and multiplications
 * in a tight for loop. Tasks are sufficiently compute-intensive and
 * lightweight: the threadpool implementation should perform better than the one
 * that spawns new threads with every bulk task launch.
 */
TestResults mathOperationsInTightForLoopTestBase(Scheduler* t, int num_tasks,
                                                 bool run_with_dependencies,
                                                 bool do_async) {
    int num_bulk_task_launches = 2000;

    int array_size = 512;
    float* task_output = new float[num_bulk_task_launches * array_size];

    for (int i = 0; i < (num_bulk_task_launches * array_size); i++) {
        task_output[i] = 0.0;
    }

    std::vector<MathOperationsInTightForLoopTask> medium_tasks;
    for (int i = 0; i < num_bulk_task_launches; i++) {
        medium_tasks.push_back(MathOperationsInTightForLoopTask(
            array_size, &task_output[i * array_size]));
    }

    double start_time = CycleTimer::currentSeconds();
    if (do_async) {
        if (run_with_dependencies) {
            TaskId prev_task_id;
            for (int i = 0; i < num_bulk_task_launches; i++) {
                std::vector<TaskId> deps;
                if (i > 0) {
                    deps.push_back(prev_task_id);
                }
                prev_task_id =
                    t->submit(&medium_tasks[i], num_tasks, deps);
            }
        } else {
            std::vector<TaskId> deps;
            for (int i = 0; i < num_bulk_task_launches; i++) {
                t->submit(&medium_tasks[i], num_tasks, deps);
            }
        }
        t->waitForAll();
    } else {
        for (int i = 0; i < num_bulk_task_launches; i++) {
            t->execute(&medium_tasks[i], num_tasks);
        }
    }
    double end_time = CycleTimer::currentSeconds();

    TestResults result;
    result.passed = true;
    for (int i = 0; i < array_size; i++) {
        if (i % 3 == 0) {
            if (std::floor(task_output[i]) != 349) {
                printf("%d: %f expected=%d\n", i, std::floor(task_output[i]),
                       349);
                result.passed = false;
            }
        } else if (i % 3 == 1) {
            if (std::floor(task_output[i]) != 708) {
                printf("%d: %f expected=%d\n", i, std::floor(task_output[i]),
                       708);
                result.passed = false;
            }
        } else {
            if (std::floor(task_output[i]) != 67950) {
                printf("%d: %f expected=%d\n", i, std::floor(task_output[i]),
                       67950);
                result.passed = false;
            }
        }
    }
    result.time = end_time - start_time;

    delete[] task_output;

    return result;
}

TestResults mathOperationsInTightForLoopTest(Scheduler* t) {
    return mathOperationsInTightForLoopTestBase(t, 16, true, false);
}

TestResults mathOperationsInTightForLoopAsyncTest(Scheduler* t) {
    return mathOperationsInTightForLoopTestBase(t, 16, true, true);
}

TestResults mathOperationsInTightForLoopFewerTasksTest(Scheduler* t) {
    return mathOperationsInTightForLoopTestBase(t, 9, false, false);
}

TestResults mathOperationsInTightForLoopFewerTasksAsyncTest(Scheduler* t) {
    return mathOperationsInTightForLoopTestBase(t, 9, false, true);
}

/*
 * Computation: The following tests perform exps, logs, and multiplications
 * in a tight for loop, then sum the outputs of the different tasks using
 * a single reduce task. The async version of this test features a computation
 * DAG with fan-in dependencies.
 */
TestResults mathOperationsInTightForLoopFanInTestBase(Scheduler* t,
                                                      bool do_async) {
    int num_tasks = 64;
    int num_bulk_task_launches = 256;

    int array_size = 2048;
    float* task_output = new float[num_bulk_task_launches * array_size];
    float* final_task_output = new float[array_size];

    for (int i = 0; i < array_size; i++) {
        final_task_output[i] = 0.0;
    }

    std::vector<MathOperationsInTightForLoopTask> medium_tasks;
    for (int i = 0; i < num_bulk_task_launches; i++) {
        medium_tasks.push_back(MathOperationsInTightForLoopTask(
            array_size, &task_output[i * array_size]));
    }
    ReduceTask reduce_task(array_size, num_bulk_task_launches, task_output,
                           final_task_output);

    double start_time = CycleTimer::currentSeconds();
    if (do_async) {
        std::vector<TaskId> no_deps;
        std::vector<TaskId> deps;
        for (int i = 0; i < num_bulk_task_launches; i++) {
            TaskId task_id =
                t->submit(&medium_tasks[i], num_tasks, no_deps);
            deps.push_back(task_id);
        }
        t->submit(&reduce_task, 1, deps);
        t->waitForAll();
    } else {
        for (int i = 0; i < num_bulk_task_launches; i++) {
            t->execute(&medium_tasks[i], num_tasks);
        }
        t->execute(&reduce_task, 1);
    }
    double end_time = CycleTimer::currentSeconds();

    TestResults result;
    result.passed = true;
    for (int i = 0; i < array_size; i++) {
        if (i % 3 == 0) {
            if (std::floor(final_task_output[i]) != 89577) {
                printf("%d: %f expected=%d\n", i,
                       std::floor(final_task_output[i]), 69982);
                result.passed = false;
            }
        } else if (i % 3 == 1) {
            if (std::floor(final_task_output[i]) != 181502) {
                printf("%d: %f expected=%d\n", i,
                       std::floor(final_task_output[i]), 141798);
                result.passed = false;
            }
        } else {
            if (std::floor(final_task_output[i]) !=
                (67950 * num_bulk_task_launches)) {
                printf("%d: %f expected=%d\n", i,
                       std::floor(final_task_output[i]),
                       67950 * num_bulk_task_launches);
                result.passed = false;
            }
        }
    }
    result.time = end_time - start_time;

    delete[] task_output;
    delete[] final_task_output;

    return result;
}

TestResults mathOperationsInTightForLoopFanInTest(Scheduler* t) {
    return mathOperationsInTightForLoopFanInTestBase(t, false);
}

TestResults mathOperationsInTightForLoopFanInAsyncTest(Scheduler* t) {
    return mathOperationsInTightForLoopFanInTestBase(t, true);
}

/*
 * Computation: The following tests perform exps, logs, and multiplications
 * in a tight for loop, then sum the outputs of the different tasks using
 * multiple reduce tasks in a binary tree structure. The async version of this
 * test features a binary tree computation DAG.
 */
TestResults mathOperationsInTightForLoopReductionTreeTestBase(Scheduler* t,
                                                              bool do_async) {
    int num_tasks = 64;
    int num_bulk_task_launches = 32;

    int array_size = 16384;
    float* buffer1 = new float[num_bulk_task_launches * array_size];
    float* buffer2 = new float[(num_bulk_task_launches / 2) * array_size];
    float* buffer3 = new float[(num_bulk_task_launches / 4) * array_size];
    float* buffer4 = new float[(num_bulk_task_launches / 8) * array_size];
    float* buffer5 = new float[(num_bulk_task_launches / 16) * array_size];
    float* buffer6 = new float[(num_bulk_task_launches / 32) * array_size];
    std::vector<float*> buffers = std::vector<float*>();
    buffers.push_back(buffer1);
    buffers.push_back(buffer2);
    buffers.push_back(buffer3);
    buffers.push_back(buffer4);
    buffers.push_back(buffer5);
    buffers.push_back(buffer6);

    for (int i = 0; i < (num_bulk_task_launches / 32) * array_size; i++) {
        buffer6[i] = 0.0;
    }

    // First, execute several MathOperationsInTightForLoopTasks
    std::vector<MathOperationsInTightForLoopTask> medium_tasks;
    for (int i = 0; i < num_bulk_task_launches; i++) {
        medium_tasks.push_back(MathOperationsInTightForLoopTask(
            array_size, &buffer1[i * array_size]));
    }
    // Now, compute the sum of each of the outputs of each of these
    // bulk task launches
    std::vector<ReduceTask> reduce_tasks;
    int num_reduce_tasks = num_bulk_task_launches / 2;
    int i = 0;
    while (num_reduce_tasks >= 1) {
        float* input = buffers[i];
        float* output = buffers[i + 1];
        ;
        for (int i = 0; i < num_reduce_tasks; i++) {
            ReduceTask reduce_task(array_size, 2, input, output);
            reduce_tasks.emplace_back(reduce_task);
            input += (2 * array_size);
            output += (array_size);
        }
        i++;
        num_reduce_tasks /= 2;
    }

    double start_time = CycleTimer::currentSeconds();
    if (do_async) {
        std::vector<TaskId> no_deps;
        std::vector<std::vector<TaskId>> all_deps;
        std::vector<std::vector<TaskId>> new_all_deps;
        std::vector<TaskId> cur_deps;
        for (int i = 0; i < num_bulk_task_launches; i++) {
            TaskId task_id =
                t->submit(&medium_tasks[i], num_tasks, no_deps);
            cur_deps.push_back(task_id);
            if (cur_deps.size() == 2) {
                all_deps.emplace_back(cur_deps);
                cur_deps = std::vector<TaskId>();
            }
        }
        // Make sure submit() is called with the right dependencies
        cur_deps = std::vector<TaskId>();
        int num_reduce_tasks = num_bulk_task_launches / 2;
        int reduce_idx = 0;
        while (num_reduce_tasks >= 1) {
            for (int i = 0; i < num_reduce_tasks; i++) {
                TaskId task_id = t->submit(
                    &reduce_tasks[reduce_idx + i], 1, all_deps[i]);
                cur_deps.push_back(task_id);
                if (cur_deps.size() == 2) {
                    new_all_deps.emplace_back(cur_deps);
                    cur_deps = std::vector<TaskId>();
                }
            }
            reduce_idx += num_reduce_tasks;
            all_deps.clear();
            for (std::vector<TaskId> deps : new_all_deps) {
                all_deps.emplace_back(deps);
            }
            new_all_deps.clear();
            num_reduce_tasks /= 2;
        }
        t->waitForAll();
    } else {
        for (int i = 0; i < num_bulk_task_launches; i++) {
            t->execute(&medium_tasks[i], num_tasks);
        }
        for (size_t i = 0; i < reduce_tasks.size(); i++) {
            t->execute(&reduce_tasks[i], 1);
        }
    }
    double end_time = CycleTimer::currentSeconds();

    TestResults result;
    result.passed = true;
    for (int i = 0; i < array_size; i++) {
        if (i % 3 == 0) {
            if (std::floor(buffer6[i]) != 11197) {
                printf("%d: %f expected=%d\n", i, std::floor(buffer6[i]),
                       11197);
                result.passed = false;
            }
        } else if (i % 3 == 1) {
            if (std::floor(buffer6[i]) != 22687) {
                printf("%d: %f expected=%d\n", i, std::floor(buffer6[i]),
                       22687);
                result.passed = false;
            }
        } else {
            if (std::floor(buffer6[i]) != (67950 * num_bulk_task_launches)) {
                printf("%d: %f expected=%d\n", i, std::floor(buffer6[i]),
                       67950 * num_bulk_task_launches);
                result.passed = false;
            }
        }
    }
    result.time = end_time - start_time;

    delete[] buffer1;
    delete[] buffer2;
    delete[] buffer3;
    delete[] buffer4;
    delete[] buffer5;
    delete[] buffer6;

    return result;
}

TestResults mathOperationsInTightForLoopReductionTreeTest(Scheduler* t) {
    return mathOperationsInTightForLoopReductionTreeTestBase(t, false);
}

TestResults mathOperationsInTightForLoopReductionTreeAsyncTest(Scheduler* t) {
    return mathOperationsInTightForLoopReductionTreeTestBase(t, true);
}

/*
 * Computation: In between two calls to a light weight task, these tests spawn
 * a medium weight bulk task launch that only has enough enough tasks to
 * use a subset of the available hardware threads.
 * If the main thread and unused threads spin, they will compete with
 * the useful work, causing slowdowns. In contrast, if unused threads
 * sleep until they are required to do work, the threads doing useful work
 * will not be crowded out.
 */
TestResults spinBetweenRunCallsTestBase(Scheduler* t, bool do_async) {
    int num_light_tasks = 1;
    int num_med_tasks = 2;

    int* light_task_output = new int[num_light_tasks];
    int* med_task_output = new int[num_med_tasks];

    for (int i = 0; i < num_light_tasks; i++) {
        light_task_output[i] = 0;
    }

    for (int i = 0; i < num_med_tasks; i++) {
        med_task_output[i] = 0;
    }

    LightTask light_task(light_task_output);
    RecursiveFibonacciTask medium_task(40, med_task_output);

    double start_time = CycleTimer::currentSeconds();
    if (do_async) {
        std::vector<TaskId> deps;
        TaskId light_task_id =
            t->submit(&light_task, num_light_tasks, deps);
        // A subset of available threads performs work
        // here, while the rest spin
        TaskId medium_task_id =
            t->submit(&medium_task, num_med_tasks, deps);
        deps.push_back(light_task_id);
        deps.push_back(medium_task_id);
        t->submit(&light_task, num_light_tasks, deps);
        t->waitForAll();
    } else {
        t->execute(&light_task, num_light_tasks);
        // Notice that since the number of tasks is small, only a small
        // number of available threads can perform work here, while the
        // rest spin.
        t->execute(&medium_task, num_med_tasks);
        t->execute(&light_task, num_light_tasks);
    }
    double end_time = CycleTimer::currentSeconds();

    // Validate correctness
    TestResults result;
    result.passed = true;
    for (int i = 0; i < num_med_tasks; i++) {
        if (med_task_output[i] != 165580141) {
            result.passed = false;
        }
    }
    result.time = end_time - start_time;

    delete[] light_task_output;
    delete[] med_task_output;
    return result;
}

TestResults spinBetweenRunCallsTest(Scheduler* t) {
    return spinBetweenRunCallsTestBase(t, false);
}

TestResults spinBetweenRunCallsAsyncTest(Scheduler* t) {
    return spinBetweenRunCallsTestBase(t, true);
}

/*
 * Computation: This test computes a Mandelbrot fractal image by
 * decomposing the problem into tasks that produce contiguous chunks of
 * output image rows. Note that only one bulk task launch is performed,
 * which means thread pool and spawning threads each execute() should have
 * similar performance.
 */
TestResults mandelbrotChunkedTestBase(Scheduler* t, bool do_async) {
    int num_tasks = 128;

    MandelbrotTask::MandelArgs ma;
    ma.x0 = -2;
    ma.x1 = 1;
    ma.y0 = -1;
    ma.y1 = 1;
    ma.width = 1600;
    ma.height = 1200;
    ma.max_iterations = 256;
    ma.output = new int[ma.width * ma.height];
    for (int i = 0; i < (ma.width * ma.height); i++) {
        ma.output[i] = 0;
    }

    MandelbrotTask mandel_task(&ma, true);  // No interleaving

    // time task-based implementation
    double start_time = CycleTimer::currentSeconds();
    if (do_async) {
        std::vector<TaskId>
            deps;  // Call submit without dependencies.
        t->submit(&mandel_task, num_tasks, deps);
        t->waitForAll();
    } else {
        t->execute(&mandel_task, num_tasks);
    }
    double end_time = CycleTimer::currentSeconds();

    // Validate correctness of the task-based implementation
    // against sequential implementation
    int* golden = new int[ma.width * ma.height];
    mandel_task.mandelbrotSerial(ma.x0, ma.y0, ma.x1, ma.y1, ma.width,
                                 ma.height, 0, ma.height, ma.max_iterations,
                                 golden);

    TestResults result;
    result.passed = true;
    for (int i = 0; i < ma.width * ma.height; i++) {
        if (golden[i] != ma.output[i]) {
            result.passed = false;
        }
    }

    result.time = end_time - start_time;

    delete[] golden;
    delete[] ma.output;

    return result;
}

TestResults mandelbrotChunkedTest(Scheduler* t) {
    return mandelbrotChunkedTestBase(t, false);
}

TestResults mandelbrotChunkedAsyncTest(Scheduler* t) {
    return mandelbrotChunkedTestBase(t, true);
}

/*
 * Computation: Simple correctness test for submit.
 * Tasks sleep for a prescribed amount of time and then print
 * a message to stdout.
 */
TestResults simpleRunDepsTest(Scheduler* t) {
    Task* a = new SleepTask(1);
    Task* b = new SleepTask(5);
    Task* c = new SleepTask(10);

    std::vector<TaskId> a_deps;
    std::vector<TaskId> b_deps;
    std::vector<TaskId> c_deps;

    double start_time = CycleTimer::currentSeconds();
    auto a_taskid = t->submit(a, 10, a_deps);

    b_deps.push_back(a_taskid);
    auto b_task_id = t->submit(b, 6, b_deps);

    c_deps.push_back(b_task_id);
    t->submit(c, 12, c_deps);

    t->waitForAll();
    double end_time = CycleTimer::currentSeconds();

    TestResults result;
    result.passed = true;
    result.time = end_time - start_time;

    delete a;
    delete b;
    delete c;

    return result;
}

/*
 * This test makes dependencies in a diamond topology are satisfied.
 */
TestResults strictDiamondDepsTest(Scheduler* t) {
    // Just four tasks in a diamond.
    bool* done = new bool[4]();

    // When these strict tasks execute, they check their dependencies.
    std::vector<bool*> a_dep_fs;
    Task* a = new StrictDependencyTask(a_dep_fs, done);

    std::vector<bool*> b_dep_fs = {done};
    Task* b = new StrictDependencyTask(b_dep_fs, done + 1);

    std::vector<bool*> c_dep_fs = {done};
    Task* c = new StrictDependencyTask(c_dep_fs, done + 2);

    std::vector<bool*> d_dep_fs = {done + 1, done + 2};
    Task* d = new StrictDependencyTask(d_dep_fs, done + 3);

    std::vector<TaskId> a_deps;
    std::vector<TaskId> b_deps;
    std::vector<TaskId> c_deps;
    std::vector<TaskId> d_deps;

    double start_time = CycleTimer::currentSeconds();
    auto a_taskid = t->submit(a, 1, a_deps);

    b_deps.push_back(a_taskid);
    auto b_task_id = t->submit(b, 6, b_deps);

    c_deps.push_back(b_task_id);
    auto c_task_id = t->submit(c, 12, c_deps);

    d_deps.push_back(b_task_id);
    d_deps.push_back(c_task_id);
    t->submit(d, 4, d_deps);

    t->waitForAll();
    double end_time = CycleTimer::currentSeconds();

    TestResults result;
    result.passed = done[3];
    result.time = end_time - start_time;

    delete[] done;
    delete a;
    delete b;
    delete c;
    delete d;

    return result;
};

/*
 * These tests generates and execute a random DAG of n tasks and at most m edges,
 * and make all dependencies are satisfied.
 */
TestResults strictGraphDepsTestBase(Scheduler* t, int n, int m,
                                    unsigned int seed) {
    // For repeatability.
    srand(seed);

    // Each StrictDependencyTask sets this when it is complete.
    bool* done = new bool[n]();

    std::vector<std::vector<int>> idx_deps(n);
    std::vector<std::vector<bool*>> flag_deps(n);
    std::vector<std::vector<TaskId>> task_deps(n);

    // For keeping track of the TaskIDs returned by the task system.
    TaskId* task_ids = new TaskId[n];

    // Used to avoid duplicate edges
    std::set<std::pair<int, int>> eset;

    // Generate random graph.
    for (int i = 0; i < m; i++) {
        int s = rand() % n;
        int t = rand() % n;
        if (s > t) {
            std::swap(s, t);
        }
        if (s == t || eset.count({s, t})) {
            continue;
        }
        idx_deps[t].push_back(s);
        flag_deps[t].push_back(done + s);
        eset.insert({s, t});
    }

    std::vector<Task*> tasks;
    for (int i = 0; i < n; i++) {
        tasks.push_back(new StrictDependencyTask(flag_deps[i], done + i));
    }

    double start_time = CycleTimer::currentSeconds();
    for (int i = 0; i < n; i++) {
        // Populate TaskId deps.
        for (int idx : idx_deps[i]) {
            task_deps[i].push_back(task_ids[idx]);
        }
        // Launch async and record this task's id.
        task_ids[i] =
            t->submit(tasks[i], (rand() % 15) + 1, task_deps[i]);
    }
    t->waitForAll();
    double end_time = CycleTimer::currentSeconds();

    TestResults result;
    result.passed = true;
    for (int i = 0; i < n; ++i) result.passed = result.passed && done[i];
    result.time = end_time - start_time;
    for (auto* task : tasks) delete task;
    delete[] task_ids;
    delete[] done;
    return result;
}

TestResults strictGraphDepsSmall(Scheduler* t) {
    return strictGraphDepsTestBase(t, 4, 2, 0);
}

TestResults strictGraphDepsMedium(Scheduler* t) {
    return strictGraphDepsTestBase(t, 100, 1000, 0);
}

TestResults strictGraphDepsLarge(Scheduler* t) {
    return strictGraphDepsTestBase(t, 1000, 20000, 0);
}

const std::vector<TestCase>& builtinCases() {
    static const std::vector<TestCase> cases = {
        {"simple_test_sync", false, simpleTestSync},
        {"simple_test_async", true, simpleTestAsync},
        {"ping_pong_equal", false, pingPongEqualTest},
        {"ping_pong_unequal", false, pingPongUnequalTest},
        {"super_light", false, superLightTest},
        {"super_super_light", false, superSuperLightTest},
        {"recursive_fibonacci", false, recursiveFibonacciTest},
        {"math_operations_in_tight_for_loop", false, mathOperationsInTightForLoopTest},
        {"math_operations_in_tight_for_loop_fewer_tasks", false, mathOperationsInTightForLoopFewerTasksTest},
        {"math_operations_in_tight_for_loop_fan_in", false, mathOperationsInTightForLoopFanInTest},
        {"math_operations_in_tight_for_loop_reduction_tree", false, mathOperationsInTightForLoopReductionTreeTest},
        {"spin_between_run_calls", false, spinBetweenRunCallsTest},
        {"mandelbrot_chunked", false, mandelbrotChunkedTest},
        {"ping_pong_equal_async", true, pingPongEqualAsyncTest},
        {"ping_pong_unequal_async", true, pingPongUnequalAsyncTest},
        {"super_light_async", true, superLightAsyncTest},
        {"super_super_light_async", true, superSuperLightAsyncTest},
        {"recursive_fibonacci_async", true, recursiveFibonacciAsyncTest},
        {"math_operations_in_tight_for_loop_async", true, mathOperationsInTightForLoopAsyncTest},
        {"math_operations_in_tight_for_loop_fewer_tasks_async", true, mathOperationsInTightForLoopFewerTasksAsyncTest},
        {"math_operations_in_tight_for_loop_fan_in_async", true, mathOperationsInTightForLoopFanInAsyncTest},
        {"math_operations_in_tight_for_loop_reduction_tree_async", true, mathOperationsInTightForLoopReductionTreeAsyncTest},
        {"mandelbrot_chunked_async", true, mandelbrotChunkedAsyncTest},
        {"spin_between_run_calls_async", true, spinBetweenRunCallsAsyncTest},
        {"simple_run_deps_test", true, simpleRunDepsTest},
        {"strict_diamond_deps_async", true, strictDiamondDepsTest},
        {"strict_graph_deps_small_async", true, strictGraphDepsSmall},
        {"strict_graph_deps_med_async", true, strictGraphDepsMedium},
        {"strict_graph_deps_large_async", true, strictGraphDepsLarge},
    };
    return cases;
}

}  // namespace scheduler_tests
