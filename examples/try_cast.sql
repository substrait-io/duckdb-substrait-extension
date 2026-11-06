-- Run from the repository root after building the extension:
-- ./build/release/duckdb -unsigned < examples/try_cast.sql
.bail on
.mode box
.nullvalue NULL

LOAD 'build/release/extension/substrait/substrait.duckdb_extension';

CREATE TABLE raw_values (id INTEGER, raw VARCHAR);
INSERT INTO raw_values VALUES (1, '42'), (2, 'invalid'), (3, NULL);

SET VARIABLE query_sql = 'SELECT id, TRY_CAST(raw AS INTEGER) AS amount FROM raw_values';

.print Native DuckDB result
SELECT id, TRY_CAST(raw AS INTEGER) AS amount FROM raw_values ORDER BY id;

SET VARIABLE json_plan = (
    SELECT json FROM get_substrait_json(getvariable('query_sql'), enable_optimizer=false)
);

.print Imported JSON plan
SELECT * FROM from_substrait_json(getvariable('json_plan')) ORDER BY id;

SET VARIABLE binary_plan = (
    SELECT * FROM get_substrait(getvariable('query_sql'), enable_optimizer=true)
);

.print Imported binary plan
SELECT * FROM from_substrait(getvariable('binary_plan')) ORDER BY id;
