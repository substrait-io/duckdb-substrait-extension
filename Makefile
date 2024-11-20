PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

all: release

# Configuration of extension
EXT_NAME=substrait
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

CORE_EXTENSIONS='tpch;tpcds;json'

# Set this flag during building to enable the benchmark runner
ifeq (${BUILD_BENCHMARK}, 1)
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DBUILD_BENCHMARKS=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

pull:
	git submodule init
	git submodule update --recursive --remote

# Client builds
%_js: export BUILD_NODE=1
debug_js: debug
release_js: release

%_r: export BUILD_R=1
debug_r: debug
release_r: release

release_c_unit_test:
	EXT_RELEASE_FLAGS=-DSUBSTRAIT_EXTENSION_TEST_EXE=ON $(MAKE) release

%_python: export BUILD_PYTHON=1
%_python: export BUILD_FTS=1
%_python: export BUILD_VISUALIZER=1
%_python: export BUILD_TPCDS=1
debug_python: debug
release_python: release

test_debug: debug
	build/debug/test/unittest "$(PROJ_DIR)test/*"

# Client tests
test_python: test_debug_python
test_debug_python: debug_python
	cd test/python && python3 -m pytest

test_release_python: release_python
	cd test/python && python3 -m pytest

test_release_r: release_r
	cd test/r && R -f test_substrait.R

test_debug_r: debug_r
	cd test/r && DUCKDB_R_DEBUG=1 R -f test_substrait.R

format:
	cp ${DUCKDB_DIRECTORY}/.clang-format .
	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
	rm .clang-format

update:
	git submodule update --remote --merge
