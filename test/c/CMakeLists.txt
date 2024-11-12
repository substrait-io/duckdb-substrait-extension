cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME substrait_c_test)
project(${TARGET_NAME})

if (CMAKE_VERSION VERSION_LESS "3.1")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else ()
  set (CMAKE_CXX_STANDARD 11)
endif ()

include_directories(../../duckdb/src/include)
include_directories(../../duckdb/test/include)
include_directories(../../duckdb/third_party/catch)

set(ALL_SOURCES test_substrait_c_api.cpp)


add_library_unity(test_substrait OBJECT ${ALL_SOURCES})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_substrait>
    PARENT_SCOPE)

option(ENABLE_TEST_EXE "Build the optional test executable" OFF)
if (ENABLE_TEST_EXE)
    # Add an executable target with main.cpp
    add_executable(test_substrait_exe $<TARGET_OBJECTS:test_substrait> main.cpp)
    # Link the executable with necessary libraries
    target_link_libraries(test_substrait_exe duckdb substrait_extension test_helpers)
endif()
