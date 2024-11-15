import pandas as pd
import duckdb


def test_roundtrip_substrait(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i integer)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')
    res = connection.get_substrait("select * from integers limit 5")
    proto_bytes = res.fetchone()[0]

    query_result = connection.from_substrait(proto_bytes)

    expected = pd.Series(range(5), name="i", dtype="int32")

    pd.testing.assert_series_equal(query_result.df()["i"], expected)


def test_ctas_with_select_columns(require):
    connection = require('substrait')
    create_employee_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE employee_salaries AS 
        SELECT name, salary FROM employees""")

    expected = pd.DataFrame({"name": ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"],
                             "salary": pd.Series([120000, 80000, 50000, 95000, 60000], dtype="float64")})

    query_result = execute_via_substrait(connection, "SELECT * FROM employee_salaries")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def test_ctas_with_filter(require):
    connection = require('substrait')
    create_employee_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE high_earners AS 
        SELECT * FROM employees WHERE salary > 80000""")

    expected = pd.DataFrame({"employee_id": pd.Series([1, 4], dtype="int32"),
                             "name": ["John Doe", "Bob Brown"],
                             "department_id": pd.Series([1, 3], dtype="int32"),
                             "salary":  pd.Series([120000, 95000], dtype="float64")})

    query_result = execute_via_substrait(connection, "SELECT * FROM high_earners")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def test_ctas_with_case_and_when(require):
    connection = require('substrait')
    create_employee_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE categorized_employees AS 
        SELECT name, 
            CASE
                WHEN salary > 100000 THEN 'HIGH'
                WHEN salary BETWEEN 60000 AND 100000 THEN 'Medium'
                ELSE 'Low'
            END AS salary_category 
            FROM employees""")

    expected = pd.DataFrame({"name": ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"],
                             "salary_category": ["HIGH", "Medium", "Low", "Medium", "Medium"]})

    query_result = execute_via_substrait(connection, "SELECT * FROM categorized_employees")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def test_ctas_with_order_by(require):
    connection = require('substrait')
    create_employee_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE sorted_employees AS 
        SELECT * FROM employees ORDER BY salary DESC""")

    expected = pd.DataFrame({"employee_id": pd.Series([1, 4, 2, 5, 3], dtype="int32"),
                             "name": ["John Doe", "Bob Brown", "Jane Smith", "Charlie Black", "Alice Johnson"],
                             "department_id": pd.Series([1, 3, 2, 2, 1], dtype="int32"),
                             "salary":  pd.Series([120000, 95000, 80000, 60000, 50000], dtype="float64")})

    query_result = execute_via_substrait(connection, "SELECT * FROM sorted_employees")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def test_ctas_with_subquery(require):
    connection = require('substrait')
    create_employee_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE high_salary_employees AS 
            SELECT * FROM (
                SELECT employee_id, name, salary
                FROM employees 
                WHERE salary > 100000)
    """)

    expected = pd.DataFrame({"employee_id": pd.Series([1], dtype="int32"),
                             "name": ["John Doe"], "salary":  pd.Series([120000], dtype="float64")})
    query_result = execute_via_substrait(connection, "SELECT * FROM high_salary_employees")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def test_ctas_with_aggregation(require):
    connection = require('substrait')
    create_employee_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE department_summary AS 
        SELECT department_id, COUNT(*) AS employee_count 
        FROM employees
        GROUP BY department_id
    """)

    expected = pd.DataFrame({"department_id":  pd.Series([1, 2, 3], dtype="int32"),
                             "employee_count": [2, 2, 1]})
    query_result = execute_via_substrait(connection, "SELECT * FROM department_summary")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def test_ctas_with_join(require):
    connection = require('substrait')
    create_employee_table(connection)
    create_departments_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE employee_department AS 
        SELECT e.employee_id, e.name, d.name AS department_name
        FROM employees e
        JOIN departments d
        ON e.department_id = d.department_id
    """)

    expected = pd.DataFrame({"employee_id": pd.Series([1, 2, 3, 4, 5], dtype="int32"),
                             "name": ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"],
                             "department_name": ["HR", "Engineering", "HR", "Finance", "Engineering"]})

    query_result = execute_via_substrait(connection, "SELECT * FROM employee_department")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def test_ctas_with_union(require):
    connection = require('substrait')
    create_employee_table(connection)
    create_part_time_employee_table(connection)
    _ = execute_via_substrait(connection, """CREATE TABLE all_employees AS 
        SELECT employee_id, name, department_id, salary
        FROM employees
        UNION ALL
        SELECT id as employee_id, name, department_id, hourly_rate * 2000 AS salary
        FROM part_time_employees 
        ORDER BY employee_id
    """)

    expected = pd.DataFrame({"employee_id": pd.Series([1, 2, 3, 4, 5, 6, 7], dtype="int32"),
                             "name": ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black",
                                      "David White", "Eve Green"],
                             "department_id": pd.Series([1, 2, 1, 3, 2, 1, 2], dtype="int32"),
                             "salary":  pd.Series([120000, 80000, 50000, 95000, 60000, 30000, 40000], dtype="float64")})

    query_result = execute_via_substrait(connection, "SELECT * FROM all_employees")
    pd.testing.assert_frame_equal(query_result.df(), expected)


def execute_via_substrait(connection, query):
    res = connection.get_substrait(query)
    proto_bytes = res.fetchone()[0]
    con = connection.from_substrait(proto_bytes)
    con.fetchall() # this is needed to force the execution
    return con


def create_employee_table(connection):
    connection.execute("""
        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            department_id INTEGER,
            salary DECIMAL(10, 2)
        )
    """)

    connection.execute("""
        INSERT INTO employees VALUES
            (1, 'John Doe', 1, 120000),
            (2, 'Jane Smith', 2, 80000),
            (3, 'Alice Johnson', 1, 50000),
            (4, 'Bob Brown', 3, 95000),
            (5, 'Charlie Black', 2, 60000)
    """)


def create_part_time_employee_table(connection):
    connection.execute("""
        CREATE TABLE part_time_employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            department_id INTEGER,
            hourly_rate DECIMAL(10, 2)
        )
    """)

    connection.execute("""
        INSERT INTO part_time_employees VALUES
            (6, 'David White', 1, 15),
            (7, 'Eve Green', 2, 20)
    """)


def create_departments_table(connection):
    connection.execute("""
        CREATE TABLE departments (
            department_id INTEGER PRIMARY KEY,
            name VARCHAR(100)
        )
    """)

    connection.execute("""
        INSERT INTO departments VALUES
            (1, 'HR'),
            (2, 'Engineering'),
            (3, 'Finance'),
    """)
