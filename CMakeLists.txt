cmake_minimum_required(VERSION 3.16)
project(TaskScheduler LANGUAGES CXX)

find_package(Threads REQUIRED)

add_library(task_scheduler src/thread_pools.cpp)
add_library(TaskScheduler::task_scheduler ALIAS task_scheduler)
target_include_directories(task_scheduler PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(task_scheduler PUBLIC cxx_std_11)
target_link_libraries(task_scheduler PUBLIC Threads::Threads)

