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
find_package(SubstraitProtobuf CONFIG REQUIRED)

# Find and copy dialect_schema.yaml from substrait-protobuf source
# The dialect schema is required for validating the DuckDB dialect YAML specification.
# Try vcpkg_installed directory with current triplet first
if(DEFINED VCPKG_TARGET_TRIPLET)
  set(DIALECT_SCHEMA_SOURCE "${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/share/substrait-protobuf")
else()
  # Fallback: search for any vcpkg_installed subdirectory
  file(GLOB VCPKG_TRIPLETS RELATIVE "${CMAKE_BINARY_DIR}/vcpkg_installed" "${CMAKE_BINARY_DIR}/vcpkg_installed/*")
  if(VCPKG_TRIPLETS)
    list(GET VCPKG_TRIPLETS 0 FIRST_TRIPLET)
    set(DIALECT_SCHEMA_SOURCE "${CMAKE_BINARY_DIR}/vcpkg_installed/${FIRST_TRIPLET}/share/substrait-protobuf")
  endif()
endif()

if(NOT EXISTS "${DIALECT_SCHEMA_SOURCE}/dialect_schema.yaml")
  # If not found in standard location, search vcpkg buildtree
  file(GLOB DIALECT_SCHEMA_SEARCH "$ENV{VCPKG_ROOT}/buildtrees/substrait-protobuf/src/*/substrait/text/dialect_schema.yaml")
  if(DIALECT_SCHEMA_SEARCH)
    list(GET DIALECT_SCHEMA_SEARCH 0 DIALECT_SCHEMA_SOURCE)
    get_filename_component(DIALECT_SCHEMA_SOURCE "${DIALECT_SCHEMA_SOURCE}" DIRECTORY)
  endif()
endif()

# Copy dialect_schema.yaml to the build directory
if(EXISTS "${DIALECT_SCHEMA_SOURCE}/dialect_schema.yaml")
  configure_file(
    "${DIALECT_SCHEMA_SOURCE}/dialect_schema.yaml"
    "${CMAKE_BINARY_DIR}/dialect_schema.yaml"
    COPYONLY
  )
else()
  message(FATAL_ERROR "Could not find dialect_schema.yaml in substrait-protobuf package. "
    "Ensure substrait-protobuf is installed via vcpkg and your build directory contains vcpkg_installed/. "
    "Set VCPKG_TARGET_TRIPLET if using multiple platform triplets.")
endif()

set(ALL_SOURCES test_substrait_c_api.cpp test_substrait_c_utils.cpp test_projection.cpp test_base_schema_projection.cpp test_root_names.cpp test_dialect_validation.cpp)


add_library_unity(test_substrait OBJECT ${ALL_SOURCES})
target_link_libraries(test_substrait nlohmann_json::nlohmann_json nlohmann_json_schema_validator::validator yaml-cpp::yaml-cpp)
target_compile_definitions(test_substrait PRIVATE "DIALECT_SCHEMA_DIR=\"${CMAKE_BINARY_DIR}\"")
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
