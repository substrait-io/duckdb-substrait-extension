cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME substrait)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

include_directories(third_party/substrait)
include_directories(third_party/)

set(PROTOBUF_SOURCES
    third_party/google/protobuf/any.cc
    third_party/google/protobuf/any.pb.cc
    third_party/google/protobuf/any_lite.cc
    third_party/google/protobuf/arena.cc
    third_party/google/protobuf/arenastring.cc
    third_party/google/protobuf/descriptor.cc
    third_party/google/protobuf/descriptor.pb.cc
    third_party/google/protobuf/descriptor_database.cc
    third_party/google/protobuf/dynamic_message.cc
    third_party/google/protobuf/empty.pb.cc
    third_party/google/protobuf/extension_set.cc
    third_party/google/protobuf/extension_set_heavy.cc
    third_party/google/protobuf/generated_enum_util.cc
    third_party/google/protobuf/generated_message_bases.cc
    third_party/google/protobuf/generated_message_reflection.cc
    third_party/google/protobuf/generated_message_table_driven.cc
    third_party/google/protobuf/generated_message_table_driven_lite.cc
    third_party/google/protobuf/generated_message_util.cc
    third_party/google/protobuf/implicit_weak_message.cc
    third_party/google/protobuf/inlined_string_field.cc
    third_party/google/protobuf/map.cc
    third_party/google/protobuf/map_field.cc
    third_party/google/protobuf/message.cc
    third_party/google/protobuf/message_lite.cc
    third_party/google/protobuf/parse_context.cc
    third_party/google/protobuf/port_def.inc
    third_party/google/protobuf/port_undef.inc
    third_party/google/protobuf/reflection_ops.cc
    third_party/google/protobuf/repeated_field.cc
    third_party/google/protobuf/repeated_ptr_field.cc
    third_party/google/protobuf/text_format.cc
    third_party/google/protobuf/unknown_field_set.cc
    third_party/google/protobuf/wire_format.cc
    third_party/google/protobuf/wire_format_lite.cc
    third_party/google/protobuf/io/coded_stream.cc
    third_party/google/protobuf/io/io_win32.cc
    third_party/google/protobuf/io/strtod.cc
    third_party/google/protobuf/io/tokenizer.cc
    third_party/google/protobuf/io/zero_copy_stream.cc
    third_party/google/protobuf/io/zero_copy_stream_impl.cc
    third_party/google/protobuf/io/zero_copy_stream_impl_lite.cc
    third_party/google/protobuf/stubs/common.cc
    third_party/google/protobuf/stubs/int128.cc
    third_party/google/protobuf/stubs/status.cc
    third_party/google/protobuf/stubs/stringpiece.cc
    third_party/google/protobuf/stubs/stringprintf.cc
    third_party/google/protobuf/stubs/structurally_valid.cc
    third_party/google/protobuf/stubs/strutil.cc
    third_party/google/protobuf/stubs/substitute.cc
    third_party/google/protobuf/stubs/bytestream.cc
    third_party/google/protobuf/util/json_util.cc
    third_party/google/protobuf/util/internal/datapiece.cc
    third_party/google/protobuf/util/internal/default_value_objectwriter.cc
    third_party/google/protobuf/util/internal/error_listener.cc
    third_party/google/protobuf/util/internal/json_escaping.cc
    third_party/google/protobuf/util/internal/json_objectwriter.cc
    third_party/google/protobuf/util/internal/json_stream_parser.cc
    third_party/google/protobuf/util/json_util.cc
    third_party/google/protobuf/util/internal/object_writer.cc
    third_party/google/protobuf/util/internal/proto_writer.cc
    third_party/google/protobuf/util/internal/protostream_objectsource.cc
    third_party/google/protobuf/util/internal/protostream_objectwriter.cc
    third_party/google/protobuf/source_context.pb.cc
    third_party/google/protobuf/stubs/statusor.cc
    third_party/google/protobuf/stubs/time.cc
    third_party/google/protobuf/type.pb.cc
    third_party/google/protobuf/wrappers.pb.cc
    third_party/google/protobuf/struct.pb.cc
    third_party/google/protobuf/util/internal/type_info.cc
    third_party/google/protobuf/util/internal/field_mask_utility.cc
    third_party/google/protobuf/util/type_resolver_util.cc
    third_party/google/protobuf/util/internal/utility.cc)

set(SUBSTRAIT_SOURCES
    third_party/substrait/substrait/algebra.pb.cc
    third_party/substrait/substrait/capabilities.pb.cc
    third_party/substrait/substrait/function.pb.cc
    third_party/substrait/substrait/parameterized_types.pb.cc
    third_party/substrait/substrait/plan.pb.cc
    third_party/substrait/substrait/type.pb.cc
    third_party/substrait/substrait/type_expressions.pb.cc
    third_party/substrait/substrait/extensions/extensions.pb.cc)

set(EXTENSION_SOURCES
    src/to_substrait.cpp
    src/from_substrait.cpp
    src/substrait_extension.cpp
    src/custom_extensions.cpp
    src/custom_extensions_generated.cpp
    ${SUBSTRAIT_SOURCES}
    ${PROTOBUF_SOURCES})

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})
IF (DEFINED ENV{SKIP_SUBSTRAIT_C_TESTS})
    message(STATUS "Skipping substrait c tests")
ELSE()
    add_subdirectory("test/c")
ENDIF()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
