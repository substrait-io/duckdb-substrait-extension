-- Demo: ExpressionFilter vs Other Filter Types
PRAGMA explain_output = 'optimized_only';

-- Create test table
CREATE TABLE demo AS SELECT range as id, 'test_' || range as name FROM range(1000);

-- 1. SIMPLE FILTERS (become ConstantFilter, pushed down completely)
EXPLAIN SELECT * FROM demo WHERE id = 5;
-- No FILTER node in plan - fully pushed down

EXPLAIN SELECT * FROM demo WHERE id > 100;
-- No FILTER node in plan - fully pushed down

-- 2. COMPLEX EXPRESSIONS (become ExpressionFilter if supported)
EXPLAIN SELECT * FROM demo WHERE LENGTH(name) > 6;
-- May or may not have FILTER node depending on table function support

EXPLAIN SELECT * FROM demo WHERE CAST(id AS VARCHAR) LIKE '1%';
-- Likely to have FILTER node - complex cast + pattern matching

EXPLAIN SELECT * FROM demo WHERE id + 10 > 500;
-- Arithmetic expression - depends on pushdown support

-- 3. MULTI-COLUMN EXPRESSIONS (cannot use ExpressionFilter)
EXPLAIN SELECT * FROM demo WHERE id + LENGTH(name) > 10;
-- Will have FILTER node - references multiple columns