cmake_minimum_required(VERSION 3.5...3.29)
set(TARGET_NAME substrait_c_test)
project(${TARGET_NAME})

set (CMAKE_CXX_STANDARD 11)

include_directories(include)
include_directories(../../duckdb/src/include)
include_directories(../../duckdb/test/include)
include_directories(../../duckdb/third_party/catch)
find_package(nlohmann_json_schema_validator REQUIRED)
find_package(yaml-cpp REQUIRED)

# The dialect schema and the extension definitions both ship in the
# substrait-extensions port (data-only). SubstraitExtensions_DATA_DIR points at
# the installed data root: text/dialect_schema.yaml and extensions/*.yaml. It is
# pinned to the same version as SUBSTRAIT_EXTENSIONS_TAG in
# scripts/generate_custom_functions.py, so the dialect is checked against the
# definitions the generated function registry was built from.
find_package(SubstraitExtensions CONFIG REQUIRED)

set(ALL_SOURCES test_substrait_c_api.cpp test_substrait_c_utils.cpp test_projection.cpp test_base_schema_projection.cpp test_root_names.cpp test_dialect_validation.cpp)


add_library_unity(test_substrait OBJECT ${ALL_SOURCES})
target_link_libraries(test_substrait nlohmann_json::nlohmann_json nlohmann_json_schema_validator::validator yaml-cpp::yaml-cpp)
target_compile_definitions(test_substrait PRIVATE
    "SUBSTRAIT_DATA_DIR=\"${SubstraitExtensions_DATA_DIR}\""
    "DIALECT_YAML=\"${CMAKE_CURRENT_SOURCE_DIR}/../../duckdb_dialect.yaml\"")
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_substrait>
    PARENT_SCOPE)

option(SUBSTRAIT_EXTENSION_TEST_EXE "Build the optional test executable" OFF)
if (SUBSTRAIT_EXTENSION_TEST_EXE)
    # Add an executable target with main.cpp
    add_executable(test_substrait_exe $<TARGET_OBJECTS:test_substrait> main.cpp)
    # Link the executable with necessary libraries
    target_link_libraries(test_substrait_exe duckdb substrait_extension test_helpers nlohmann_json_schema_validator::validator yaml-cpp::yaml-cpp)
endif()
