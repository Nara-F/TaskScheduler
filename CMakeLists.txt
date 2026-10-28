cmake_minimum_required(VERSION 3.16)
project(TaskScheduler LANGUAGES CXX)

find_package(Threads REQUIRED)

add_library(task_scheduler src/thread_pools.cpp)
add_library(TaskScheduler::task_scheduler ALIAS task_scheduler)
target_include_directories(task_scheduler PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(task_scheduler PUBLIC cxx_std_11)
target_link_libraries(task_scheduler PUBLIC Threads::Threads)

include(CTest)
if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    add_executable(task_scheduler_tests tests/test_runner.cpp tests/builtin_cases.cpp)
    target_link_libraries(task_scheduler_tests PRIVATE TaskScheduler::task_scheduler)
    set_target_properties(task_scheduler_tests PROPERTIES CXX_STANDARD 11 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO)

    add_test(NAME framework_validation
        COMMAND ${Python3_EXECUTABLE} -m unittest discover -s ${CMAKE_CURRENT_SOURCE_DIR}/tests -p test_framework.py)
    add_test(NAME builtin_sync
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_tests.py builtin
        --binary $<TARGET_FILE:task_scheduler_tests> --scheduler all --case simple_test_sync super_light)
    add_test(NAME builtin_dependencies
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_tests.py builtin
        --binary $<TARGET_FILE:task_scheduler_tests> --case strict_diamond_deps_async strict_graph_deps_large_async)
    add_test(NAME custom_random_dag
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_tests.py custom
        --binary $<TARGET_FILE:task_scheduler_tests> --scheduler all --batches 24 --tasks-per-batch 7
        --weight-range 0 2000 --dag random --seed 42 --iterations 2)
    add_test(NAME custom_empty_batches
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_tests.py custom
        --binary $<TARGET_FILE:task_scheduler_tests> --scheduler all --batches 5
        --tasks-per-batch 4,0,0,3,0 --dag chain --weight 1000)
    add_test(NAME idle_resources
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_tests.py idle
        --binary $<TARGET_FILE:task_scheduler_tests> --scheduler all --threads 2 --idle-ms 1)
    set_tests_properties(builtin_sync builtin_dependencies custom_random_dag custom_empty_batches idle_resources
        PROPERTIES TIMEOUT 60)
endif()
