cmake_minimum_required(VERSION 3.22)
project(iejoin VERSION 0.1.0 LANGUAGES CXX)

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
set(CMAKE_CXX_STANDARD 20)

find_package(Boost REQUIRED)
find_package(Threads)
find_package(GTest REQUIRED)

include_directories(
        $ENV{CONDA_PREFIX}/include
)

set(ARG_SOURCES
        src/main.cpp
)

set(ARG_LIBRARY_PATHS
        ${Boost_LIBRARIES}  Threads::Threads
)
message (STATUS "Log: main ${PROJECT_NAME}-main")
add_executable(${PROJECT_NAME}-main ${ARG_SOURCES})
target_include_directories(${PROJECT_NAME}-main PUBLIC src)
target_link_libraries(${PROJECT_NAME}-main PUBLIC ${ARG_LIBRARY_PATHS})


if (GTEST_FOUND)
    set(ARG_TESTS tests/iejoin.cpp)
    message (STATUS "Log: unit test ${PROJECT_NAME}-gtest")
    add_executable (${PROJECT_NAME}-gtest ${ARG_TESTS})
    target_include_directories (${PROJECT_NAME}-gtest PUBLIC src)
    target_link_directories(${PROJECT_NAME}-gtest PRIVATE $ENV{CONDA_PREFIX}/lib)

    target_link_libraries (${PROJECT_NAME}-gtest
            GTest::GTest
            gmock)

    add_custom_target (${PROJECT_NAME}-gtest-run COMMAND $<TARGET_FILE:${PROJECT_NAME}-gtest>)
    add_test(NAME ${PROJECT_NAME}-gtest COMMAND ${CMAKE_BINARY_DIR}/${PROJECT_NAME}-gtest)
endif ()
