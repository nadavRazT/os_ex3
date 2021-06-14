cmake_minimum_required(VERSION 3.19)
project(os_ex3)

set(CMAKE_CXX_FLAGS -pthread)

include_directories(.)
#include_directories(Barrier Atomic)

add_executable(os_ex3
        MapReduceFramework.h MapReduceFramework.cpp SampleClient.cpp MapReduceClient.h)
