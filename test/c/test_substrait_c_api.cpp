#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "test_substrait_c_utils.hpp"

#include <chrono>
#include <thread>
#include <iostream>

using namespace duckdb;
using namespace std;

struct DataDirectoryFixture {
	DataDirectoryFixture() {
		TestChangeDirectory("data");
	}

	~DataDirectoryFixture() {
		TestChangeDirectory("..");
	}
};

TEST_CASE("Test C Get and To Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);
  con.EnableQueryVerification();
  // create the database
  REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
  REQUIRE_NO_FAIL(
      con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

  auto proto = GetSubstrait(con, "select * from integers limit 2");
  auto result = FromSubstrait(con,proto);

  REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

  REQUIRE_THROWS(GetSubstrait(con, "select * from p"));

  REQUIRE_THROWS(FromSubstrait(con,"this is not valid"));
}

TEST_CASE("Test C Get and To Json-Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);
  con.EnableQueryVerification();
  // create the database
  REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
  REQUIRE_NO_FAIL(
      con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

  auto json = GetSubstraitJSON(con,"select * from integers limit 2");
  auto result = FromSubstraitJSON(con,json);

  REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

  REQUIRE_THROWS(GetSubstraitJSON(con,"select * from p"));

  REQUIRE_THROWS(FromSubstraitJSON(con,"this is not valid"));
}

TEST_CASE("Test C CTAS Select columns with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE employee_salaries AS "
		"SELECT name, salary FROM employees"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from employee_salaries");
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {120000, 80000, 50000, 95000, 60000}));
}

TEST_CASE("Test C CTAS Filter with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE filtered_employees AS "
		"SELECT * FROM employees "
		"WHERE salary > 80000;"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from filtered_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Bob Brown"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {120000, 95000}));
}

TEST_CASE("Test C CTAS Case_When with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE categorized_employees AS "
		"SELECT name, "
		"CASE "
			"WHEN salary > 100000 THEN 'High' "
			"WHEN salary BETWEEN 60000 AND 100000 THEN 'Medium' "
			"ELSE 'Low' "
		"END AS salary_category "
		"FROM employees"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from categorized_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"High", "Medium", "Low", "Medium", "Medium"}));
}

TEST_CASE("Test C CTAS OrderBy with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE ordered_employees AS "
		"SELECT * FROM employees "
		"ORDER BY salary DESC"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from ordered_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 2, 5, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Bob Brown", "Jane Smith", "Charlie Black", "Alice Johnson"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3, 2, 2, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {120000, 95000, 80000, 60000, 50000}));
}

TEST_CASE("Test C CTAS SubQuery with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE high_salary_employees AS "
		"SELECT * "
		"FROM ( "
			"SELECT employee_id, name, salary "
			"FROM employees "
			"WHERE salary > 100000)"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from high_salary_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe"}));
	REQUIRE(CHECK_COLUMN(result, 2, {120000}));
}

TEST_CASE("Test C CTAS Distinct with Substrait API", "[substrait-api]") {
	SKIP_TEST("SKIP: Distinct operator has unsupported child type");  // TODO fix TransformDistinct
	return;

	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	ExecuteViaSubstraitJSON(con, "CREATE TABLE unique_departments AS "
		"SELECT DISTINCT department_id FROM employees"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from unique_departments");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
}

TEST_CASE("Test C CTAS Aggregation with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE department_summary AS "
		"SELECT department_id, COUNT(*) AS employee_count "
		"FROM employees "
		"GROUP BY department_id"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from department_summary");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2, 1}));
}

TEST_CASE("Test C CTAS Join with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	CreateDepartmentsTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE employee_departments AS "
		"SELECT e.employee_id, e.name, d.department_name "
		"FROM employees e "
		"JOIN departments d "
		"ON e.department_id = d.department_id"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from employee_departments");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"HR", "Engineering", "HR", "Finance", "Engineering"}));
}

TEST_CASE("Test C CTAS Union with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	CreatePartTimeEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE all_employees AS "
		"SELECT employee_id, name, department_id, salary "
		"FROM employees "
		"UNION "
		"SELECT id, name, department_id, hourly_rate * 2000 AS salary "
		"FROM part_time_employees "
		"ORDER BY employee_id"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from all_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, 7}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black", "David White", "Eve Green"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 1, 3, 2, 1, 2}));
}

TEST_CASE("Test C InsertRows with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE senior_employees ("
	                          "employee_id INTEGER PRIMARY KEY, "
	                          "name VARCHAR(100), "
	                          "department_id INTEGER, "
	                          "salary DECIMAL(10, 2))"));

	ExecuteViaSubstrait(con, "INSERT INTO senior_employees "
	                         "SELECT * FROM employees WHERE salary > 80000");

	auto result = ExecuteViaSubstrait(con, "SELECT * from senior_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Bob Brown"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {120000, 95000}));
}

TEST_CASE("Test C DeleteRows with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "DELETE FROM employees WHERE salary < 80000");
	auto result = ExecuteViaSubstrait(con, "SELECT * from employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Jane Smith", "Bob Brown"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {120000, 80000, 95000}));
}

TEST_CASE("Test C VirtualTable input Literal", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);

  auto json = GetSubstraitJSON(con,"select * from (values (1, 2),(3, 4))");
  REQUIRE(!json.empty());

  auto result = FromSubstraitJSON(con,json);
  REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
  REQUIRE(CHECK_COLUMN(result, 1, {2, 4}));
}


TEST_CASE("Test C VirtualTable input Expression", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);

  auto json = GetSubstraitJSON(con,"select * from (values (1+1,2+2),(3+3,4+4)) as temp(a,b)");
  REQUIRE(!json.empty());

  auto result = FromSubstraitJSON(con,json);
  REQUIRE(CHECK_COLUMN(result, 0, {2, 6}));
  REQUIRE(CHECK_COLUMN(result, 1, {4, 8}));
}

TEST_CASE_METHOD(DataDirectoryFixture, "Test C Function Varchar Literal", "[substrait-api][literal]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Iceberg requires AVRO.  Remove after AVRO auto-loads.
	REQUIRE_NO_FAIL(con.Query("INSTALL avro;"));
	REQUIRE_NO_FAIL(con.Query("LOAD avro;"));
	REQUIRE_NO_FAIL(con.Query("INSTALL iceberg;"));
	REQUIRE_NO_FAIL(con.Query("LOAD iceberg;"));

	const string plan_json = R"plan(
	{
	  "version": {
	    "minorNumber": 78,
	    "producer": "substrait-go v3.5.0 darwin/arm64"
	  },
	  "extensionUrns": [
	    {
	      "extensionUrnAnchor": 1,
	      "urn": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml"
	    }
	  ],
	  "extensions": [
	    {
	      "extensionFunction": {
	        "extensionUrnReference": 1,
	        "functionAnchor": 1,
	        "name": "equal"
	      }
	    }
	  ],
	  "relations": [
	    {
	      "root": {
	        "input": {
	          "project": {
	            "common": {
	              "emit": {
				    "outputMapping": [ 1 ]
				  }
	            },
	            "input": {
	              "read": {
	                "common": {
	                  "direct": {}
	                },
	                "virtualTable": {
	                  "values": [
	                    {
	                      "fields": [
	                        {
	                          "i32": 42
	                        }
	                      ]
	                    }
	                  ]
	                }
	              }
	            },
	            "expressions": [
	              {
	                "scalarFunction": {
	                  "functionReference": 1,
	                  "arguments": [
	                    {
	                      "value": {
	                        "literal": {
	                          "varChar": {
	                            "value": "a",
	                            "length": 1
	                          }
	                        }
	                      }
	                    },
	                    {
	                      "value": {
	                        "literal": {
	                          "varChar": {
	                            "value": " a",
	                            "length": 2
	                          }
	                        }
	                      }
	                    }
	                  ],
	                  "outputType": {
	                    "bool": {
	                      "nullability": "NULLABILITY_REQUIRED"
	                    }
	                  }
	                }
	              }
	            ]
	          }
	        },
	        "names": [
	          "result"
	        ]
	      }
	    }
	  ]
	}
	)plan";

	auto result = FromSubstraitJSON(con,plan_json);

	REQUIRE(CHECK_COLUMN(result, 0, {false}));
}

TEST_CASE_METHOD(DataDirectoryFixture, "Test C Iceberg Substrait with Substrait API", "[substrait-api][iceberg]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("INSTALL avro;"));
	REQUIRE_NO_FAIL(con.Query("LOAD avro;"));
	REQUIRE_NO_FAIL(con.Query("INSTALL iceberg;"));
	REQUIRE_NO_FAIL(con.Query("LOAD iceberg;"));

	const string plan_json = R"plan(
		{
		  "relations" : [ {
		    "root" : {
		      "input" : {
		        "sort" : {
		          "input" : {
		        "project" : {
	              "common": {
	                "emit": {
				 	    "outputMapping": [ 2, 3 ]
				 	  }
	              },
		          "input" : {
		            "read" : {
		              "baseSchema" : {
		                "names" : [ "fruit", "count" ],
		                "struct" : {
		                  "types" : [ {
		                    "string" : {
		                      "nullability" : "NULLABILITY_NULLABLE"
		                    }
		                   }, {
		                    "decimal" : {
		                      "scale" : 0,
		                      "precision" : 10,
		                      "nullability" : "NULLABILITY_NULLABLE"
		                    }
		                  } ],
		                  "nullability" : "NULLABILITY_REQUIRED"
		                }
		              },
		              "icebergTable" : {
		                "direct" : {
		                  "metadataUri" : "../data/iceberg/metadata/v3.metadata.json",
		                }
		              }
		            }
		          },
		          "expressions" : [ {
		            "selection" : {
		              "directReference" : {
		                "structField" : { }
		              },
		              "rootReference" : { }
		            }
		          }, {
		            "selection" : {
		              "directReference" : {
		                "structField" : {
		                  "field": 1
		                }
		              },
		              "rootReference" : { }
		            }
		          } ]
		        }
		          },
		          "sorts" : [ {
		            "expr" : {
		              "selection" : {
		                "directReference" : { "structField" : { } },
		                "rootReference" : { }
		              }
		            },
		            "direction" : "SORT_DIRECTION_ASC_NULLS_LAST"
		          } ]
		        }
		      },
		      "names" : [ "fruit", "count" ]
		    }
		  } ],
		  "version" : {
		    "minorNumber" : 78,
		    "producer" : "DuckDB"
		  }
		}
		)plan";

	auto result = FromSubstraitJSON(con,plan_json);

	REQUIRE(CHECK_COLUMN(result, 0, {"apple", "banana", "cranberry"}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3}));
}

TEST_CASE_METHOD(DataDirectoryFixture, "Test C Iceberg Substrait Snapshot ID with Substrait API", "[substrait-api][iceberg]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("INSTALL avro;"));
	REQUIRE_NO_FAIL(con.Query("LOAD avro;"));
	REQUIRE_NO_FAIL(con.Query("INSTALL iceberg;"));
	REQUIRE_NO_FAIL(con.Query("LOAD iceberg;"));

	const string plan_json = R"plan(
		{
		  "relations" : [ {
		    "root" : {
		      "input" : {
		        "sort" : {
		          "input" : {
		        "project" : {
	              "common": {
	                "emit": {
				 	    "outputMapping": [ 2, 3 ]
				 	  }
	              },
		          "input" : {
		            "read" : {
		              "baseSchema" : {
		                "names" : [ "fruit", "count" ],
		                "struct" : {
		                  "types" : [ {
		                    "string" : {
		                      "nullability" : "NULLABILITY_NULLABLE"
		                    }
		                   }, {
		                    "decimal" : {
		                      "scale" : 0,
		                      "precision" : 10,
		                      "nullability" : "NULLABILITY_NULLABLE"
		                    }
		                  } ],
		                  "nullability" : "NULLABILITY_REQUIRED"
		                }
		              },
		              "icebergTable" : {
		                "direct" : {
		                  "metadataUri" : "../data/iceberg/metadata/v3.metadata.json",
		                  "snapshotId" : "4118182924578855064",
		                }
		              }
		            }
		          },
		          "expressions" : [ {
		            "selection" : {
		              "directReference" : {
		                "structField" : { }
		              },
		              "rootReference" : { }
		            }
		          }, {
		            "selection" : {
		              "directReference" : {
		                "structField" : {
		                  "field": 1
		                }
		              },
		              "rootReference" : { }
		            }
		          } ]
		        }
		          },
		          "sorts" : [ {
		            "expr" : {
		              "selection" : {
		                "directReference" : { "structField" : { } },
		                "rootReference" : { }
		              }
		            },
		            "direction" : "SORT_DIRECTION_ASC_NULLS_LAST"
		          } ]
		        }
		      },
		      "names" : [ "fruit", "count" ]
		    }
		  } ],
		  "version" : {
		    "minorNumber" : 78,
		    "producer" : "DuckDB"
		  }
		}
		)plan";

	auto result = FromSubstraitJSON(con,plan_json);

	REQUIRE(CHECK_COLUMN(result, 0, {"apple", "banana"}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
}

TEST_CASE_METHOD(DataDirectoryFixture, "Test C Iceberg Substrait Snapshot Timestamp with Substrait API", "[substrait-api][iceberg]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("INSTALL avro;"));
	REQUIRE_NO_FAIL(con.Query("LOAD avro;"));
	REQUIRE_NO_FAIL(con.Query("INSTALL iceberg;"));
	REQUIRE_NO_FAIL(con.Query("LOAD iceberg;"));

	const string plan_json = R"plan(
		{
		  "relations" : [ {
		    "root" : {
		      "input" : {
		        "sort" : {
		          "input" : {
		        "project" : {
	              "common": {
	                "emit": {
				 	    "outputMapping": [ 2, 3 ]
				 	  }
	              },
		          "input" : {
		            "read" : {
		              "baseSchema" : {
		                "names" : [ "fruit", "count" ],
		                "struct" : {
		                  "types" : [ {
		                    "string" : {
		                      "nullability" : "NULLABILITY_NULLABLE"
		                    }
		                   }, {
		                    "decimal" : {
		                      "scale" : 0,
		                      "precision" : 10,
		                      "nullability" : "NULLABILITY_NULLABLE"
		                    }
		                  } ],
		                  "nullability" : "NULLABILITY_REQUIRED"
		                }
		              },
		              "icebergTable" : {
		                "direct" : {
		                  "metadataUri" : "../data/iceberg/metadata/v3.metadata.json",
		                  "snapshotTimestamp" : 1737171409298000,
		                }
		              }
		            }
		          },
		          "expressions" : [ {
		            "selection" : {
		              "directReference" : {
		                "structField" : { }
		              },
		              "rootReference" : { }
		            }
		          }, {
		            "selection" : {
		              "directReference" : {
		                "structField" : {
		                  "field": 1
		                }
		              },
		              "rootReference" : { }
		            }
		          } ]
		        }
		          },
		          "sorts" : [ {
		            "expr" : {
		              "selection" : {
		                "directReference" : { "structField" : { } },
		                "rootReference" : { }
		              }
		            },
		            "direction" : "SORT_DIRECTION_ASC_NULLS_LAST"
		          } ]
		        }
		      },
		      "names" : [ "fruit", "count" ]
		    }
		  } ],
		  "version" : {
		    "minorNumber" : 78,
		    "producer" : "DuckDB"
		  }
		}
		)plan";

	auto result = FromSubstraitJSON(con,plan_json);

	REQUIRE(CHECK_COLUMN(result, 0, {"apple", "banana"}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
}

TEST_CASE("Test C Project SELECT 1", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto expected_json_str = R"({"relations":[{"root":{"input":{"project":{"common":{"emit":{"outputMapping":[1]}},"input":{"read":{"virtualTable":{"values":[{"fields":[{"i32":42}]}]}}},"expressions":[{"literal":{"i32":1}}]}},"names":["1"]}}],"version":{"minorNumber":78,"producer":"DuckDB"}})";
	auto json_str = GetSubstraitJSON(con,"SELECT 1");
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con,json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test C Project on empty virtual table for SELECT 1", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto json_str = R"({"version":{"minorNumber":29, "producer":"substrait-go"}, "relations":[{"root":{"input":{"project":{"common":{"direct":{}}, "input":{"read":{"common":{"direct":{}}, "baseSchema":{"struct":{"nullability":"NULLABILITY_REQUIRED"}}, "virtualTable":{"expressions":[{}]}}}, "expressions":[{"literal":{"decimal":{"value":"AQAAAAAAAAAAAAAAAAAAAA==", "precision":1}, "nullable":true}}]}}, "names":["?column?"]}}]})";
	auto result = FromSubstraitJSON(con,json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test C VirtualTable with bad input Literal", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto json_plan = R"({"version":{"minorNumber":29, "producer":"substrait-go"}, "relations":[{"root":{"input":{"project":{"common":{"direct":{}}, "input":{"read":{"common":{"direct":{}}, "baseSchema":{"struct":{"nullability":"NULLABILITY_REQUIRED"}}, "virtualTable":{"expressions":[{}]}}}, "expressions":[{"literal":{"decimal":{"value":"AQ==", "precision":1}, "nullable":true}}]}}, "names":["?column?"]}}]})";
	REQUIRE_THROWS(FromSubstraitJSON(con,json_plan));
}

TEST_CASE("Test C Project with VirtualTable", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto json_plan = R"({"version":{"minorNumber":29, "producer":"substrait-go darwin/arm64"}, "relations":[{"root":{"input":{"project":{"common":{"emit":{"outputMapping":[2]}}, "input":{"read":{"common":{"direct":{}}, "baseSchema":{"names":["c1", "c2"], "struct":{"types":[{"fp64":{"nullability":"NULLABILITY_NULLABLE"}}, {"fp64":{"nullability":"NULLABILITY_NULLABLE"}}], "nullability":"NULLABILITY_REQUIRED"}}, "virtualTable":{"expressions":[{"fields":[{"literal":{"fp64":1, "nullable":true}}, {"literal":{"fp64":2, "nullable":true}}]}]}}}, "expressions":[{"literal":{"fp64":42}}]}}, "names":["p1"]}}]})";
	auto result = FromSubstraitJSON(con,json_plan);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

// ============================================================================
// Tests for deprecated Substrait types in SubstraitToDuckType
// These verify that the extension handles deprecated type fields without
// crashing (segfault). See: substrait type.proto fields 14, 29, 17.
//
// NOTE: The generated C++ protobuf code excludes deprecated fields entirely,
// so these tests must use pre-serialized binary protobuf (generated by Python's
// protobuf library which does support deprecated fields). The plans are minimal:
// read from a named table + project with a single cast expression.
// ============================================================================

TEST_CASE("Test cast to deprecated timestamp type", "[substrait-api][deprecated-types]") {
	// The deprecated "timestamp" type (proto field 14) should map to DuckDB TIMESTAMP.
	// Previously this caused a segfault because SubstraitToDuckType hit the default
	// handler which dereferenced a null descriptor pointer (FindFieldByNumber returns
	// nullptr for fields not in the generated code).
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE cast_test (val VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO cast_test VALUES ('2024-01-15 10:30:00')"));

	// Binary substrait plan: SELECT CAST(val AS TIMESTAMP) FROM cast_test
	// Generated by Python protobuf which supports the deprecated timestamp field.
	const unsigned char plan_bytes[] = {0x1a, 0x51, 0x12, 0x4f, 0x0a, 0x45, 0x3a, 0x43,
	    0x0a, 0x05, 0x12, 0x03, 0x0a, 0x01, 0x01, 0x12, 0x24, 0x0a, 0x22, 0x0a, 0x02,
	    0x0a, 0x00, 0x12, 0x0f, 0x0a, 0x03, 0x76, 0x61, 0x6c, 0x12, 0x08, 0x0a, 0x04,
	    0x62, 0x02, 0x10, 0x01, 0x18, 0x02, 0x3a, 0x0b, 0x0a, 0x09, 0x63, 0x61, 0x73,
	    0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x5a, 0x12, 0x0a, 0x04, 0x72,
	    0x02, 0x10, 0x01, 0x12, 0x08, 0x12, 0x06, 0x0a, 0x02, 0x12, 0x00, 0x22, 0x00,
	    0x18, 0x01, 0x12, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x32, 0x02, 0x10,
	    0x55};
	string plan_blob(reinterpret_cast<const char *>(plan_bytes), sizeof(plan_bytes));

	auto result = FromSubstrait(con, plan_blob);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIMESTAMP(Timestamp::FromString("2024-01-15 10:30:00", false))}));
}

TEST_CASE("Test cast to deprecated timestamp_tz type", "[substrait-api][deprecated-types]") {
	// The deprecated "timestampTz" type (proto field 29) should map to DuckDB TIMESTAMP_TZ.
	// Same crash mechanism as the timestamp case above.
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE cast_tz_test (val VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO cast_tz_test VALUES ('2024-01-15 10:30:00+00')"));

	// Binary substrait plan: SELECT CAST(val AS TIMESTAMPTZ) FROM cast_tz_test
	const unsigned char plan_bytes[] = {0x1a, 0x55, 0x12, 0x53, 0x0a, 0x49, 0x3a, 0x47,
	    0x0a, 0x05, 0x12, 0x03, 0x0a, 0x01, 0x01, 0x12, 0x27, 0x0a, 0x25, 0x0a, 0x02,
	    0x0a, 0x00, 0x12, 0x0f, 0x0a, 0x03, 0x76, 0x61, 0x6c, 0x12, 0x08, 0x0a, 0x04,
	    0x62, 0x02, 0x10, 0x01, 0x18, 0x02, 0x3a, 0x0e, 0x0a, 0x0c, 0x63, 0x61, 0x73,
	    0x74, 0x5f, 0x74, 0x7a, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x5a, 0x13,
	    0x0a, 0x05, 0xea, 0x01, 0x02, 0x10, 0x01, 0x12, 0x08, 0x12, 0x06, 0x0a, 0x02,
	    0x12, 0x00, 0x22, 0x00, 0x18, 0x01, 0x12, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c,
	    0x74, 0x32, 0x02, 0x10, 0x55};
	string plan_blob(reinterpret_cast<const char *>(plan_bytes), sizeof(plan_bytes));

	auto result = FromSubstrait(con, plan_blob);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIMESTAMPTZ(timestamp_tz_t(Timestamp::FromString("2024-01-15 10:30:00+00", true)))}));
}

TEST_CASE("Test cast to deprecated time type", "[substrait-api][deprecated-types]") {
	// The deprecated "time" type (proto field 17) should map to DuckDB TIME.
	// Same crash mechanism as timestamp/timestamp_tz: the field is excluded from
	// generated C++ protobuf code, appears in UnknownFieldSet, and previously
	// caused a segfault via null pointer dereference in the default handler.
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE cast_time_test (val VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO cast_time_test VALUES ('10:30:00')"));

	// Binary substrait plan: SELECT CAST(val AS TIME) FROM cast_time_test
	// Uses the deprecated "time" type (proto field 17).
	const unsigned char plan_bytes[] = {0x1a, 0x57, 0x12, 0x55, 0x0a, 0x4b, 0x3a, 0x49,
	    0x0a, 0x05, 0x12, 0x03, 0x0a, 0x01, 0x01, 0x12, 0x29, 0x0a, 0x27, 0x0a, 0x02,
	    0x0a, 0x00, 0x12, 0x0f, 0x0a, 0x03, 0x76, 0x61, 0x6c, 0x12, 0x08, 0x0a, 0x04,
	    0x62, 0x02, 0x10, 0x01, 0x18, 0x02, 0x3a, 0x10, 0x0a, 0x0e, 0x63, 0x61, 0x73,
	    0x74, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x1a, 0x15,
	    0x5a, 0x13, 0x0a, 0x05, 0x8a, 0x01, 0x02, 0x10, 0x01, 0x12, 0x08, 0x12, 0x06,
	    0x0a, 0x02, 0x12, 0x00, 0x22, 0x00, 0x18, 0x01, 0x12, 0x06, 0x72, 0x65, 0x73,
	    0x75, 0x6c, 0x74, 0x32, 0x02, 0x10, 0x55};
	string plan_blob(reinterpret_cast<const char *>(plan_bytes), sizeof(plan_bytes));

	auto result = FromSubstrait(con, plan_blob);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::TIME(10, 30, 0, 0)}));
}
