import pandas as pd
import duckdb

EXPECTED_RESULT = '''
┌───────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  explain_key  │                                            explain_value                                             │
│    varchar    │                                               varchar                                                │
├───────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ physical_plan │ ┌───────────────────────────┐\n│      STREAMING_LIMIT      │\n└─────────────┬─────────────┘\n┌────…  │
└───────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────┘

'''

def test_roundtrip_substrait(require):
    connection = require('substrait')
    connection.execute('CREATE TABLE integers (i integer)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    translate_result = connection.get_substrait('SELECT * FROM integers LIMIT 5')
    proto_bytes      = translate_result.fetchone()[0]

    expected = pd.Series([EXPECTED_RESULT], name='Explain Plan', dtype='str')
    actual   = connection.table_function('explain_substrait', proto_bytes).execute()

    pd.testing.assert_series_equal(actual.df()['Explain Plan'], expected)

