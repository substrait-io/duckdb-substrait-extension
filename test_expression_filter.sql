-- Test TransformExpressionFilter functionality
LOAD substrait;

-- Create a simple table
CREATE TABLE test_table AS SELECT range as a, range * 2 as b FROM range(10);

-- Test a complex expression that should trigger ExpressionFilter
-- This should use an expression filter rather than a simple constant comparison
CALL get_substrait('SELECT * FROM test_table WHERE a + b > 10');

-- Test another complex expression
CALL get_substrait('SELECT * FROM test_table WHERE a * 2 + b > a + 5');