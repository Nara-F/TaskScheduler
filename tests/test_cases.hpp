#pragma once
#include <vector>
#include "task_scheduler/scheduler.hpp"

namespace scheduler_tests {
struct TestResults {
    bool passed;
    double time;
};
struct TestCase {
    const char* name;
    bool asynchronous;
    TestResults (*run)(task_scheduler::Scheduler*);
};
const std::vector<TestCase>& builtinCases();
}  // namespace scheduler_tests
