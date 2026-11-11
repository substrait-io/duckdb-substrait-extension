# DuckDB Substrait Extension

The DuckDB Substrait Extension is a [DuckDB Community Extension](https://duckdb.org/community_extensions/) that provides [Substrait](https://substrait.io) support to [DuckDB](https://www.duckdb.org).
With this extension, DuckDB can produce Substrait plans from DuckDB queries as well as consume Substrait plans and execute them with DuckDB.
The most commonly used logical relations and functions are supported: Substrait plans can be produced for all of the TPC-H and TPC-DS queries. The supported features are formally specified in the Substrait dialect file [duckdb_dialect.yaml](duckdb_dialect.yaml).

## Building

### Prerequisites

The extension consumes protobuf and the Substrait protobuf bindings from [vcpkg](https://vcpkg.io) (see [Updating the Substrait Version](#updating-the-substrait-version)), so the build needs a bootstrapped vcpkg checkout:

```sh
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
~/vcpkg/bootstrap-vcpkg.sh
```

Clone in full rather than with `--depth=1`: vcpkg resolves the `builtin-baseline` from [`vcpkg.json`](vcpkg.json) against its own git history, and a shallow clone does not contain that commit. A package that ships only the `vcpkg` tool (such as Homebrew's `vcpkg`) is not enough either, as it provides no toolchain file.

Then point `VCPKG_TOOLCHAIN_PATH` at that toolchain file, from a shell startup file so that new shells inherit it:

```sh
export VCPKG_ROOT=~/vcpkg
export VCPKG_TOOLCHAIN_PATH=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake
```

The build system passes `-DCMAKE_TOOLCHAIN_FILE` only when this variable is set. Without it, vcpkg never installs the dependencies and the configure step fails at `find_package(Protobuf CONFIG REQUIRED)` in [`CMakeLists.txt`](CMakeLists.txt).

### Building the extension

To build the extension, first clone this repository and initialize git submodules by running:

```sh
git submodule update --init
```

Then run:

```sh
make
```

If a configure already failed because `VCPKG_TOOLCHAIN_PATH` was unset, delete the `build` directory before retrying: CMake does not adopt a toolchain file into an existing cache.

To use the newly-built extension, run the bundled `duckdb` shell:

```sh
 ./build/release/duckdb
```

And load the extension like so:

```sql
LOAD 'build/release/extension/substrait/substrait.duckdb_extension';
```

## Usage

This extension provides four new functions to DuckDB:

- `get_substrait`: Converts the provided query into a binary Substrait plan
- `get_substrait_json`: Converts the provided query into a Substrait plan in JSON
- `from_substrait`: Executes a binary Substrait plan (provided as bytes) against DuckDB and returns the result
- `from_substrait_json`: Executes a Substrait plan written in JSON against DuckDB and returns the results

### Examples

Before using the extension, you need to install and load it.

You can install the development version of the extension using the commands above or you can install it from the community extensions repository by running the following in a DuckDB shell:

```sql
INSTALL substrait FROM community;
LOAD substrait;
```

#### Blob Generation

Use `get_substrait` to generate a binary plan. Save it in a variable so you can execute it later in the same connection.

```sql
--- Insert some data first
CREATE TABLE crossfit (exercise text, difficulty_level int);
INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);

SET VARIABLE exercise_plan = (SELECT * FROM get_substrait('SELECT count(exercise) AS exercise FROM crossfit WHERE difficulty_level <= 5'));
```

#### JSON Generation

Use `get_substrait_json` to see the same plan in JSON.

```sql
CALL get_substrait_json('SELECT count(exercise) AS exercise FROM crossfit WHERE difficulty_level <= 5');
```

### Blob Consumption

Pass the saved binary plan to `from_substrait`. With the sample data, the count is `2`.

```sql
SELECT * FROM from_substrait(getvariable('exercise_plan'));
```

### Controlling Query Optimization

The `get_substrait(SQL)` and `get_substrait_json(SQL)` functions accept an optional parameter, `enable_optimizer`,
to explicitly enable or disable query optimization when generating Substrait:

```sql
CALL get_substrait('select count(exercise) as exercise from crossfit', enable_optimizer=false);
CALL get_substrait_json('select count(exercise) as exercise from crossfit', enable_optimizer=true);
```

If `enable_optimizer` is not specified, it is inferred from the connection-level settings: if query optimization
is disabled at the connection level (e.g. using `PRAGMA disable_optimizer`), the Substrait generation functions
will not optimize the query; otherwise, they will.

If any specific optimizers are disabled at the connection level (e.g. using `SET disabled_optimizers TO '...'`),
they will also be disabled when generating Substrait.

The `from_substrait(blob)` function **always** respects the connection-level settings when deciding whether to
optimize a Substrait plan before executing it.

### Aggregation phases

`from_substrait` and `from_substrait_json` reject a plan if any aggregate or window call omits `phase` or sets it to `AGGREGATION_PHASE_UNSPECIFIED`. This breaks compatibility with older DuckDB exports that omitted `phase` and with DataFusion plans that leave it unspecified.

To migrate a saved DuckDB plan, regenerate it from the original SQL with the updated extension. If you edit the plan or its producer instead, check what each call reads. An `i64` column can contain either raw values or partial counts:

- If the call reads raw rows and produces a final result, set `phase` to `AGGREGATION_PHASE_INITIAL_TO_RESULT`. The DuckDB exporter uses this phase for aggregate and window calls.
- If a non-distinct `COUNT` aggregate reads partial `i64` counts and produces the final count, use `AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT`. For example, partial counts of `1`, `2`, and `4` add up to `7`. Marking them as raw input would count three rows instead.

If a non-distinct `COUNT` aggregate produces a partial count, use `AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE` for raw rows or `AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE` for partial counts. A merge returns zero for empty input and raises an error on overflow. The consumer does not support intermediate phases for other aggregates or for window calls.

### Python

You can use this extension using the [duckdb](https://pypi.org/project/duckdb/) Python package by running:

```python
import duckdb

con = duckdb.connect()
con.install_extension("substrait", repository = "community")
con.load_extension("substrait")
```

With the extension loaded, you can now use the functions provided by this extension such as `get_substrait`:

```python
# Insert some data first
con.sql("CREATE TABLE crossfit (exercise text, difficulty_level int);")
con.sql("INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);")

con.sql("CALL get_substrait('select count(exercise) as exercise from crossfit where difficulty_level <=5');")
```

### R

You can use this extension using the [duckdb](https://cran.r-project.org/web/packages/duckdb/index.html) R package by running:

```r
library(duckdb)

con <- dbConnect(duckdb())
dbExecute(con, "INSTALL substrait FROM community;")
dbExecute(con, "LOAD substrait")
```

With the extension loaded, you can now use the functions provided by this extension such as `get_substrait`:

```r
# Insert some data first
dbExecute(con, "CREATE TABLE crossfit (exercise text, difficulty_level int);")
dbExecute(con, "INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);")

dbExecute(con, "CALL get_substrait('select count(exercise) as exercise from crossfit where difficulty_level <=5');")
```

## Development

### Setting up CLion

Configuring CLion with the extension template requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

Now to configure the build targets, copy the CMake variables specified in the Makefile and ensure
the build directory is set to `../build/<build_mode>`.

### Updating the Substrait Version

The Substrait artifacts are consumed from the [substrait-packaging](https://github.com/substrait-io/substrait-packaging) project:

- **Protobuf bindings** come from the `substrait-protobuf` package, consumed via substrait-packaging's vcpkg registry (see [`vcpkg-configuration.json`](vcpkg-configuration.json)). vcpkg builds it against this project's protobuf and exposes the imported `substrait::proto` target. The version is resolved from the registry `baseline` in `vcpkg-configuration.json` (plus the `substrait-protobuf` dependency in [`vcpkg.json`](vcpkg.json)).
- **Extension definitions** used to generate `src/custom_extensions_generated.cpp` come from the `substrait-extensions` package. The version is `SUBSTRAIT_EXTENSIONS_TAG` in [`scripts/generate_custom_functions.py`](scripts/generate_custom_functions.py) (`cpp/substrait-extensions/vX.Y.Z`).

The protobuf **runtime** that `substrait-protobuf` is built against comes from vcpkg's own port, pinned by `builtin-baseline` in [`vcpkg.json`](vcpkg.json). This must match the vcpkg commit baked into DuckDB's distribution build images (DuckDB's `vcpkg_commit`) — otherwise those images cannot resolve the baseline and the build fails — so it is bumped only when upgrading DuckDB, in lockstep with the `duckdb` submodule and `duckdb_version`/`ci_tools_version` in [`.github/workflows/distribution.yml`](.github/workflows/distribution.yml), not to consume a new Substrait release.

To update to a new Substrait release:

1. Bump the registry `baseline` in [`vcpkg-configuration.json`](vcpkg-configuration.json) to a substrait-packaging `vcpkg-registry` commit that publishes the desired version (add a `substrait-protobuf` entry to `vcpkg.json` `overrides` to pin an exact version if needed), and bump `SUBSTRAIT_EXTENSIONS_TAG` to the matching `cpp/substrait-extensions/vX.Y.Z`.
2. Regenerate the custom function definitions and reformat them (requires Python with `pyyaml` and `regex`, and `clang-format`):
   ```sh
   python scripts/generate_custom_functions.py
   make format
   ```
3. Rebuild (`make`).
