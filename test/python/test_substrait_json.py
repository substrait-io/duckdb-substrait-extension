import duckdb
import pytest

def test_substrait_json(require):
    connection = require('substrait')
    if connection is None:
        return

    connection.execute('CREATE TABLE integers (i integer)')
    json =  connection.get_substrait_json("select * from integers limit 5").fetchone()[0]
    expected_result = '{"relations":[{"root":{"input":{"project":{"input":{"fetch":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"offset":"0","count":"5"}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"names":["i"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}}'
    assert json == expected_result

