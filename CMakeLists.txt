cmake_minimum_required(VERSION 3.24)

set(TARGET_NAME substrait)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

# protobuf and the Substrait protobuf bindings both come from vcpkg.
# substrait-protobuf is published by substrait-packaging's vcpkg registry
# (see vcpkg-configuration.json); it vendors the .proto files and generates the
# C++ at build time against this protobuf. find_package exposes the imported
# substrait::proto target, which (being imported) is allowed in the link
# interface of an extension that DuckDB install(EXPORT)/export()s.
find_package(Protobuf CONFIG REQUIRED)
find_package(SubstraitProtobuf CONFIG REQUIRED)

set(EXTENSION_SOURCES
    src/to_substrait.cpp
    src/from_substrait.cpp
    src/substrait_extension.cpp
    src/custom_extensions.cpp
    src/custom_extensions_generated.cpp)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
target_link_libraries(${EXTENSION_NAME} substrait::proto)

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})
target_link_libraries(${TARGET_NAME}_loadable_extension substrait::proto)

if(DEFINED ENV{SKIP_SUBSTRAIT_C_TESTS})
  message(STATUS "Skipping substrait c tests")
else()
  add_subdirectory("test/c")
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
