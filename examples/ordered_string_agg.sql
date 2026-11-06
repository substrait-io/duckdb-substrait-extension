-- Run from the repository root after building the extension:
-- ./build/release/duckdb -unsigned < examples/ordered_string_agg.sql
.bail on
.mode box
.nullvalue NULL

LOAD 'build/release/extension/substrait/substrait.duckdb_extension';

CREATE TABLE labels (priority INTEGER, label VARCHAR);
INSERT INTO labels VALUES (2, 'beta'), (1, 'alpha'), (NULL, 'omega'), (2, 'gamma');

SET VARIABLE query_sql = 'SELECT string_agg(label, '','' ORDER BY priority DESC NULLS LAST, label ASC) AS summary FROM labels';

.print Native DuckDB result
SELECT string_agg(label, ',' ORDER BY priority DESC NULLS LAST, label ASC) AS summary FROM labels;

SET VARIABLE json_plan = (
    SELECT json FROM get_substrait_json(getvariable('query_sql'), enable_optimizer=false)
);

.print Imported JSON plan
SELECT * FROM from_substrait_json(getvariable('json_plan'));

SET VARIABLE binary_plan = (
    SELECT * FROM get_substrait(getvariable('query_sql'), enable_optimizer=true)
);

.print Imported binary plan
SELECT * FROM from_substrait(getvariable('binary_plan'));
