#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb_static_extension.h"
#include "test_helpers.hpp"

#include <cstdio>

// DuckDB 2.0 no longer links extensions into libduckdb. The ones these tests use are linked
// into this executable and registered here, so that every database the tests open loads them.
extern "C" {
int32_t duckdb_extension_substrait_describe(duckdb_extension_descriptor *descriptor);
int32_t duckdb_extension_parquet_describe(duckdb_extension_descriptor *descriptor);
}

int main(int argc, char *argv[]) {
	for (auto describe : {duckdb_extension_substrait_describe, duckdb_extension_parquet_describe}) {
		if (duckdb_register_static_extension(describe) != 0) {
			fprintf(stderr, "Failed to register a statically linked extension\n");
			return 1;
		}
	}
	// DuckDB's test helpers hand out paths under a per-run temp directory that has to be
	// provisioned up front, as DuckDB's own unittest main does.
	std::string error;
	if (!duckdb::PrepareTempDir(error)) {
		fprintf(stderr, "Failed to prepare temp directory: %s\n", error.c_str());
		return 1;
	}
	int result = Catch::Session().run(argc, argv);
	duckdb::DestroyTempDir(result == 0);
	return result;
}
