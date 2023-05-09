cmake_minimum_required(VERSION 3.22)
project(sample)

# - conda environment -----------------------------------------------------------------------------
if ("$ENV{CONDA_BUILD}" STREQUAL "1")
    set(PREFIX $ENV{PREFIX})
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{BUILD_PREFIX};$ENV{PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    message(STATUS "Conda build detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
elseif (DEFINED ENV{CONDA_PREFIX})
    set(CMAKE_SYSTEM_PREFIX_PATH "$ENV{CONDA_PREFIX};${CMAKE_SYSTEM_PREFIX_PATH}")
    set(PREFIX $ENV{CONDA_PREFIX})
    message(STATUS "Conda environment detected, CMAKE_SYSTEM_PREFIX_PATH set to: ${CMAKE_SYSTEM_PREFIX_PATH}")
endif ()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O3")
# clang++ -std=c++2b -o main sample.cpp

include_directories(
        $ENV{CONDA_PREFIX}/include
)

find_package(Boost REQUIRED)

find_package(Threads)
# find_package(fmt REQUIRED)
# find_package(glog REQUIRED)

add_executable(sample main.cpp)

target_include_directories(sample PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(sample ${Boost_LIBRARIES}  Threads::Threads)
