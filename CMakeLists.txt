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


include(FeatureSummary)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CTest)

find_package(Threads)
find_package(GTest REQUIRED)
find_package(fmt REQUIRED)
enable_testing ()

include_directories(
        $ENV{PROJECT_SOURCE_DIR}/include
        $ENV{CONDA_PREFIX}/include
        $ENV{PREFIX}/include
)

link_directories($ENV{CONDA_PREFIX}/lib)

if (GTEST_FOUND)
    enable_testing ()
    add_custom_target (coverage
            COMMAND lcov -c -d ${CMAKE_BINARY_DIR}/src -o coverage.info
            COMMAND lcov -r coverage.info '/usr*' '*boost*' '*build*' -o coverage.info
            COMMAND genhtml coverage.info -o ${CMAKE_BINARY_DIR}/coverage-html)
endif ()

function (project_artifact)
    set (OPTIONS OPTIONAL VIRTUAL)
    set (ONE_VALUE_ARGS PREFIX TARGET)
    set (MULTI_VALUE_ARGS SOURCES INCLUDES LIBRARIES PUBLIC_HEADERS TESTS MAIN_SOURCE_FILE)
    cmake_parse_arguments (ARG "${OPTIONS}" "${ONE_VALUE_ARGS}"
            "${MULTI_VALUE_ARGS}" ${ARGN})
    if (ARG_UNPARSED_ARGUMENTS)
        message (SEND_ERROR
                "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif ()

    if (${ARG_VIRTUAL})
        set (TYPE OBJECT)
    endif ()

    if (ARG_SOURCES)
        add_library (${ARG_TARGET} ${TYPE} ${ARG_SOURCES})
    endif()

    if (ARG_LIBRARIES)
        target_link_libraries (${ARG_TARGET} ${ARG_LIBRARIES})
    endif ()

    if (ARG_INCLUDES)
        target_include_directories (${ARG_TARGET} ${ARG_INCLUDES})
    endif ()

    if (ARG_LIBRARY_PATHS)
        target_link_libraries (${ARG_TARGET} ${ARG_LIBRARY_PATHS})
    endif ()

    if (NOT ARG_PREFIX)
        set (ARG_PREFIX "")
    endif ()

    if (ARG_PREFIX)
        set(prefix_option -DCMAKE_INSTALL_PREFIX="${ARG_PREFIX}")
    endif ()

    install (TARGETS ${ARG_TARGET}
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib)

    if (ARG_PUBLIC_HEADERS)
        message (STATUS "Log: public headers ${ARG_PUBLIC_HEADERS}")
        install(DIRECTORY ${ARG_PUBLIC_HEADERS} DESTINATION include)
    endif ()

    if (GTEST_FOUND AND ARG_TESTS)
        message (STATUS "Log: unit test ${ARG_TARGET}-gtest")
        add_executable (${ARG_TARGET}-gtest ${ARG_TESTS})
        target_include_directories (${ARG_TARGET}-gtest PUBLIC src)
        target_compile_options (${ARG_TARGET} PUBLIC -ggdb -O0)
        target_link_libraries (${ARG_TARGET}-gtest
                ${ARG_TARGET}
                GTest::GTest
                gmock)

        add_custom_target (${ARG_TARGET}-gtest-run COMMAND $<TARGET_FILE:${ARG_TARGET}-gtest>)

        add_test(NAME ${ARG_TARGET}-gtest COMMAND ${CMAKE_BINARY_DIR}/${ARG_TARGET}-gtest)

        add_dependencies (coverage ${ARG_TARGET}-gtest-run)
    endif ()

    if (ARG_MAIN_SOURCE_FILE)
        message (STATUS "Log: main ${ARG_MAIN_SOURCE_FILE}-main")
        add_executable(${ARG_TARGET}-main ${ARG_MAIN_SOURCE_FILE})
        target_include_directories(${ARG_TARGET}-main PUBLIC src)
        target_link_libraries(${ARG_TARGET}-main PUBLIC ${ARG_TARGET} ${ARG_LIBRARY_PATHS})
    endif()
endfunction ()


project_artifact (
        TARGET
        iejoin

        INCLUDES PUBLIC
        $ENV{CONDA_PREFIX}/include

        PUBLIC_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dataframe

        LIBRARIES
        ${Boost_LIBRARIES}  Threads::Threads

        SOURCES
        src/dataframe/dataframe.cpp
        src/dataframe/logical_expr.cpp
        src/dataframe/iejoin.cpp

        TESTS
        tests/iejoin.cpp

        MAIN_SOURCE_FILE
        src/main.cpp
)

