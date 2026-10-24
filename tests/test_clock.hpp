#pragma once
#include <chrono>

class CycleTimer {
   public:
    static double currentSeconds() {
        using Clock = std::chrono::steady_clock;
        static const auto origin = Clock::now();
        return std::chrono::duration<double>(Clock::now() - origin).count();
    }
};
