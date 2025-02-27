# DuckDB Substrait Extension

The DuckDB Substrait Extension is a [DuckDB Community Extension](https://duckdb.org/community_extensions/) that provides [Substrait](https://substrait.io) support to [DuckDB](https://www.duckdb.org).
With this extension, DuckDB can produce Substrait plans from DuckDB queries as well as consume Substrait plans and execute them with DuckDB.

## Building

To build the extension, first clone this repository and initialize git submodules by running:

```sh
git submodule update --init
```

Then run:

```sh
make
```

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

To generate a Substrait blob the `get_substrait(SQL)` function must be called with a valid SQL select query.

```sql
--- Insert some data first
CREATE TABLE crossfit (exercise text, difficulty_level int);
INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);

CALL get_substrait('select count(exercise) as exercise from crossfit where difficulty_level <=5');
----
\x12\x09\x1A\x07\x10\x01\x1A\x03lte\x12\x11\x1A\x0F\x10\x02\x1A\x0Bis_not_null\x12\x09\x1A\x07\x10\x03\x1A\x03and\x12\x10\x1A\x0E\x10\x04\x1A\x0Acount_star\x1A\xCB\x01\x12\xC8\x01\x0A\xBB\x01:\xB8\x01\x12\xAB\x01"\xA8\x01\x12\x97\x01\x0A\x94\x01\x12.\x0A\x08exercise\x0A\x0Fdifficulty_level\x12\x11\x0A\x07\xB2\x01\x04\x08\x0D\x18\x01\x0A\x04*\x02\x10\x01\x18\x02\x1AJ\x1AH\x08\x03\x1A\x04\x0A\x02\x10\x01""\x1A \x1A\x1E\x08\x01\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x06\x1A\x04\x0A\x02(\x05"\x1A\x1A\x18\x1A\x16\x08\x02\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x0A\x0A\x06\x0A\x02\x08\x01\x0A\x00\x10\x01:\x0A\x0A\x08crossfit\x1A\x00"\x0A\x0A\x08\x08\x04*\x04:\x02\x10\x01\x1A\x08\x12\x06\x0A\x02\x12\x00"\x00\x12\x08exercise
```

#### JSON Generation

To generate a JSON representation of a Substrait plan the `get_substrait_json(SQL)` function must be called with a valid SQL select query.

```sql
CALL get_substrait_json('select count(exercise) as exercise from crossfit where difficulty_level <=5');
----
{"extensions":[{"extensionFunction":{"functionAnchor":1,"name":"lte"}},{"extensionFunction":{"functionAnchor":2,"name":"is_not_null"}},{"extensionFunction":{"functionAnchor":3,"name":"and"}},{"extensionFunction":{"functionAnchor":4,"name":"count_star"}}],"relations":[{"root":{"input":{"project":{"input":{"aggregate":{"input":{"read":{"baseSchema":{"names":["exercise","difficulty_level"],"struct":{"types":[{"varchar":{"length":13,"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"filter":{"scalarFunction":{"functionReference":3,"outputType":{"bool":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"scalarFunction":{"functionReference":1,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}},{"value":{"literal":{"i32":5}}}]}}},{"value":{"scalarFunction":{"functionReference":2,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}}]}}}]}},"projection":{"select":{"structItems":[{"field":1},{}]},"maintainSingularStruct":true},"namedTable":{"names":["crossfit"]}}},"groupings":[{}],"measures":[{"measure":{"functionReference":4,"outputType":{"i64":{"nullability":"NULLABILITY_NULLABLE"}}}}]}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"names":["exercise"]}}]}
```

### Blob Consumption

To consume a Substrait blob the `from_substrait(blob)` function must be called with a valid Substrait BLOB plan.

```sql
CALL from_substrait('\x12\x07\x1A\x05\x1A\x03lte\x12\x11\x1A\x0F\x10\x01\x1A\x0Bis_not_null\x12\x09\x1A\x07\x10\x02\x1A\x03and\x12\x10\x1A\x0E\x10\x03\x1A\x0Acount_star\x1A\xA4\x01\x12\xA1\x01\x0A\x94\x01:\x91\x01\x12\x86\x01"\x83\x01\x12y:w\x12c\x12a\x12+\x0A)\x12\x1B\x0A\x08exercise\x0A\x0Fdifficulty_level:\x0A\x0A\x08crossfit\x1A2\x1A0\x08\x02"\x18\x1A\x16\x1A\x14"\x0A\x1A\x08\x12\x06\x0A\x04\x12\x02\x08\x01"\x06\x1A\x04\x0A\x02(\x05"\x12\x1A\x10\x1A\x0E\x08\x01"\x0A\x1A\x08\x12\x06\x0A\x04\x12\x02\x08\x01\x1A\x08\x12\x06\x0A\x04\x12\x02\x08\x01\x1A\x06\x12\x04\x0A\x02\x12\x00\x1A\x00"\x04\x0A\x02\x08\x03\x1A\x06\x12\x04\x0A\x02\x12\x00\x12\x08exercise'::BLOB);
----
2
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

Use the script in `scripts/update_substrait.py` to update the Substrait version. It requires [protoc 3.19.4](https://github.com/protocolbuffers/protobuf/releases/tag/v3.19.4), GitHub, and Python.
To update the Substrait code, simply change [the git commit tag in the script](https://github.com/duckdb/substrait/blob/main/scripts/update_substrait.py#L8) to the desired Substrait release version.
Then, execute the script by running `python scripts/update_substrait.py`.
