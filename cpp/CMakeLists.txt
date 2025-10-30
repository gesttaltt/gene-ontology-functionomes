cmake_minimum_required(VERSION 3.15)
project(FunctionomeDAG VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build options
option(BUILD_PYTHON_BINDINGS "Build Python bindings" ON)
option(BUILD_BENCHMARKS "Build benchmark suite" ON)
option(USE_ARROW "Use Apache Arrow for columnar storage" ON)

# Find dependencies
find_package(Threads REQUIRED)

if(USE_ARROW)
    find_package(Arrow QUIET)
    if(NOT Arrow_FOUND)
        message(STATUS "Apache Arrow not found, will fetch from source")
        include(FetchContent)
        FetchContent_Declare(
            arrow
            GIT_REPOSITORY https://github.com/apache/arrow.git
            GIT_TAG apache-arrow-14.0.0
            SOURCE_SUBDIR cpp
        )
        set(ARROW_BUILD_STATIC ON CACHE BOOL "")
        set(ARROW_BUILD_SHARED OFF CACHE BOOL "")
        set(ARROW_COMPUTE ON CACHE BOOL "")
        set(ARROW_CSV ON CACHE BOOL "")
        set(ARROW_DATASET OFF CACHE BOOL "")
        set(ARROW_FILESYSTEM OFF CACHE BOOL "")
        set(ARROW_PARQUET OFF CACHE BOOL "")
        set(ARROW_BUILD_TESTS OFF CACHE BOOL "")
        set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "")
        FetchContent_MakeAvailable(arrow)
    endif()
endif()

if(BUILD_PYTHON_BINDINGS)
    find_package(Python3 COMPONENTS Interpreter Development QUIET)
    if(NOT Python3_FOUND)
        message(STATUS "Python3 not found, fetching pybind11")
    endif()

    include(FetchContent)
    FetchContent_Declare(
        pybind11
        GIT_REPOSITORY https://github.com/pybind/pybind11.git
        GIT_TAG v2.11.1
    )
    FetchContent_MakeAvailable(pybind11)
endif()

# Core DAG library
add_library(functionome_dag STATIC
    src/dag/node.cpp
    src/dag/graph.cpp
    src/dag/optimizer.cpp
    src/dag/executor.cpp
    src/ops/map.cpp
    src/ops/filter.cpp
    src/ops/reduce.cpp
    src/ops/aggregate.cpp
    src/columnar/table.cpp
    src/columnar/column.cpp
)

target_include_directories(functionome_dag PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(functionome_dag PUBLIC
    Threads::Threads
)

if(USE_ARROW)
    target_link_libraries(functionome_dag PUBLIC arrow_static)
    target_compile_definitions(functionome_dag PUBLIC USE_ARROW)
endif()

# Python bindings
if(BUILD_PYTHON_BINDINGS)
    pybind11_add_module(pyfunctionome_dag
        src/bindings/python_bindings.cpp
    )

    target_link_libraries(pyfunctionome_dag PRIVATE functionome_dag)

    # Install to site-packages
    install(TARGETS pyfunctionome_dag
        LIBRARY DESTINATION ${Python3_SITEARCH}
    )
endif()

# Benchmarks
if(BUILD_BENCHMARKS)
    add_executable(benchmark_dag
        benchmarks/benchmark_main.cpp
        benchmarks/benchmark_operations.cpp
    )

    target_link_libraries(benchmark_dag PRIVATE functionome_dag)
endif()

# Installation
install(TARGETS functionome_dag
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

install(DIRECTORY include/
    DESTINATION include
)
