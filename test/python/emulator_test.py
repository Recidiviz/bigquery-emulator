"""Tests capabilities of the BigQuery emulator."""

import base64
import datetime
from datetime import date
from decimal import Decimal

import grpc
import pandas as pd
import pandas_gbq
import pytz
from google.api_core.client_options import ClientOptions
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import SchemaField, TableReference, DatasetReference
from google.cloud.bigquery_storage_v1.services.big_query_read.transports import (
    BigQueryReadGrpcTransport,
)

from utils.big_query_emulator_container import BQ_EMULATOR_PROJECT_ID
from utils.big_query_emulator_test_case import (
    BigQueryAddress,
    BigQueryEmulatorTestCase,
)

_DATASET_1 = "dataset_1"
_TABLE_1 = "table_1"


class TestBigQueryEmulator(BigQueryEmulatorTestCase):
    """Tests capabilities of the BigQuery emulator."""

    def test_no_tables(self) -> None:
        """Run a simple query that does not query any tables."""
        query = """
SELECT *
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b), STRUCT(3 AS a, 4 AS b)
]);
"""
        self.run_query_test(
            query,
            expected_result=[
                {"a": 1, "b": 2},
                {"a": 3, "b": 4},
            ],
        )

    def test_select_except(self) -> None:
        """Run a simple SELECT query with an EXCEPT clause."""
        query = """
SELECT * EXCEPT(b)
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b), STRUCT(3 AS a, 4 AS b)
]);
"""
        self.run_query_test(
            query,
            expected_result=[
                {"a": 1},
                {"a": 3},
            ],
        )

    def test_select_qualify(self) -> None:
        """Run a simple query that has a QUALIFY clause."""

        query = """
SELECT *
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b), STRUCT(3 AS a, 4 AS b)
])
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (ORDER BY b DESC) = 1;
"""

        self.run_query_test(
            query,
            expected_result=[{"a": 3, "b": 4}],
        )

    def test_query_empty_table(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )

        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[],
        )

    def test_query_simple_table(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

    def test_query_truncate_table(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

        self.query(
            f"TRUNCATE TABLE `{self.project_id}.{address.dataset_id}.{address.table_id}`"
        )
        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[],
        )

    def test_array_first_null(self) -> None:
        self.run_query_test(
            f"SELECT GENERATE_DATE_ARRAY(NULL, NULL, INTERVAL 1 DAY) AS result",
            expected_result=[{"result": []}],
        )

    def test_delete_and_recreate_table(self) -> None:
        """Test that confirms https://github.com/goccy/bigquery-emulator/issues/16 has
        been resolved."""
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        schema = [
            bigquery.SchemaField(
                "a",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]
        self.create_mock_table(address, schema=schema)
        self.client.delete_table(table=address.to_str())

        # Should not crash
        self.create_mock_table(address, schema=schema)

    def test_delete_and_recreate_table_different_schema(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        schema_1 = [
            bigquery.SchemaField(
                "a",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]
        self.create_mock_table(address, schema=schema_1)

        self.run_query_test(
            f"SELECT a FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`",
            expected_result=[],
        )

        self.client.delete_table(table=address.to_str())

        schema_2 = [
            bigquery.SchemaField(
                "b",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
        ]
        # Should not crash
        self.create_mock_table(address, schema=schema_2)

        # Should be a valid query now
        self.run_query_test(
            f"SELECT b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`",
            expected_result=[],
        )

    def test_create_two_tables_same_name_different_dataset(self) -> None:
        """Test that confirms https://github.com/goccy/bigquery-emulator/issues/18 has
        been resolved."""
        address_1 = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        schema_1 = [
            bigquery.SchemaField(
                "a",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]
        address_2 = BigQueryAddress(dataset_id="dataset_5", table_id=_TABLE_1)
        schema_2 = [
            bigquery.SchemaField(
                "b",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
        ]

        self.create_mock_table(address_1, schema_1)
        self.create_mock_table(address_2, schema_2)

        table_1 = self.client.get_table(self._table_ref_for_address(address_1))

        self.assertEqual(address_1.dataset_id, table_1.dataset_id)
        self.assertEqual(address_1.table_id, table_1.table_id)
        self.assertEqual(schema_1, table_1.schema)
        table_2 = self.client.get_table(self._table_ref_for_address(address_2))
        self.assertEqual(address_2.dataset_id, table_2.dataset_id)
        self.assertEqual(address_2.table_id, table_2.table_id)
        self.assertEqual(schema_2, table_2.schema)

        self.run_query_test(
            f"SELECT a FROM `{self.project_id}.{address_1.dataset_id}.{address_1.table_id}`",
            expected_result=[],
        )
        self.run_query_test(
            f"SELECT b FROM `{self.project_id}.{address_2.dataset_id}.{address_2.table_id}`",
            expected_result=[],
        )

        self.load_rows_into_table(address_1, data=[{"a": 1}])
        self.load_rows_into_table(address_2, data=[{"b": 2}])

        self.run_query_test(
            f"SELECT a FROM `{self.project_id}.{address_1.dataset_id}.{address_1.table_id}`",
            expected_result=[{"a": 1}],
        )
        self.run_query_test(
            f"SELECT b FROM `{self.project_id}.{address_2.dataset_id}.{address_2.table_id}`",
            expected_result=[{"b": 2}],
        )

    def test_query_min_max_integers(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
        )

        self.run_query_test(
            f"SELECT MIN(a) AS min_a, MAX(b) AS max_b "
            f"FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"min_a": 1, "max_b": 4}],
        )

    def test_query_min_max_dates(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"a": "2022-01-01", "b": "2022-02-02"},
                {"a": "2022-03-03", "b": "2022-04-04"},
            ],
        )

        self.run_query_test(
            f"SELECT MIN(a) AS min_a, MAX(b) AS max_b "
            f"FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"min_a": date(2022, 1, 1), "max_b": date(2022, 4, 4)}],
        )

    def test_query_min_max_dates_with_partition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/31."""
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"a": "2022-01-01", "b": "2022-03-03"},
                {"a": "2022-02-02", "b": "2022-03-03"},
                {"a": "2022-02-02", "b": "2022-04-04"},
            ],
        )

        self.run_query_test(
            f"SELECT MIN(a) OVER (PARTITION BY b) AS min_a, MAX(b) OVER (PARTITION BY a) AS max_b "
            f"FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[
                {
                    "min_a": datetime.date(2022, 1, 1),
                    "max_b": datetime.date(2022, 3, 3),
                },
                {
                    "min_a": datetime.date(2022, 1, 1),
                    "max_b": datetime.date(2022, 4, 4),
                },
                {
                    "min_a": datetime.date(2022, 2, 2),
                    "max_b": datetime.date(2022, 4, 4),
                },
            ],
        )

    def test_query_min_with_parition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/19."""
        self.run_query_test(
            """SELECT MIN(a) OVER (PARTITION BY b) AS min_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"min_a": 1}],
        )

    def test_query_max_with_parition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/19."""
        self.run_query_test(
            """SELECT MAX(a) OVER (PARTITION BY b) AS max_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"max_a": 1}],
        )

    def test_query_count_with_parition(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/19."""
        self.run_query_test(
            """SELECT COUNT(a) OVER (PARTITION BY b) AS count_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"count_a": 1}],
        )

    def test_query_sum_with_parition(self) -> None:
        self.run_query_test(
            """SELECT SUM(a) OVER (PARTITION BY b) AS sum_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"sum_a": 1}],
        )

    def test_query_avg_with_parition(self) -> None:
        self.run_query_test(
            """SELECT AVG(a) OVER (PARTITION BY b) AS avg_a
FROM UNNEST([STRUCT(1 AS a, 2 AS b)]);""",
            expected_result=[{"avg_a": 1.0}],
        )

    def test_array_type(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/20."""
        query = "SELECT [1, 2, 3] as a;"
        self.run_query_test(
            query,
            expected_result=[{"a": [1, 2, 3]}],
        )

    def test_safe_parse_date_valid(self) -> None:
        self.run_query_test(
            """SELECT SAFE.PARSE_DATE("%m/%d/%Y", "12/25/2008") as a;""",
            expected_result=[{"a": date(2008, 12, 25)}],
        )

    def test_safe_parse_date_invalid(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/149."""
        self.run_query_test(
            """SELECT SAFE.PARSE_DATE("%m/%d/%Y", "2008-12-25") as a;""",
            expected_result=[{"a": None}],
        )

    def test_safe_parse_date_on_julian_date(self) -> None:
        """Tests resolution of goccy/go-zetasqlite#196"""
        self.run_query_test(
            """SELECT SAFE.PARSE_DATE('%y%j', '85001') AS a;""",
            expected_result=[{"a": date(1985, 1, 1)}],
        )

    def test_array_to_json(self) -> None:
        # Tests resolution to https://github.com/goccy/bigquery-emulator/issues/24.
        query = "SELECT TO_JSON([1, 2, 3]) as a;"
        self.run_query_test(
            query,
            expected_result=[{"a": [1, 2, 3]}],
        )

    def test_to_json(self) -> None:
        query = """SELECT TO_JSON(
  STRUCT("foo" AS a, 1 AS b)
) AS result;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b": 1}}],
        )

    def test_to_json_nested(self) -> None:
        query = """SELECT TO_JSON(
  STRUCT("foo" AS a, TO_JSON(STRUCT("bar" AS c)) AS b)
) AS result;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b": {"c": "bar"}}}],
        )

    def test_to_json_nested_cte(self) -> None:
        query = """WITH inner_json AS (
  SELECT TO_JSON(STRUCT("bar" AS c)) AS b
)
SELECT TO_JSON(STRUCT("foo" as a, b)) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b": {"c": "bar"}}}],
        )

    def test_to_json_nested_cte_column_rename(self) -> None:
        query = """WITH inner_json AS (
  SELECT TO_JSON(STRUCT("bar" AS c)) AS b
)
SELECT TO_JSON(STRUCT("foo" AS a, b AS b_2)) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": "foo", "b_2": {"c": "bar"}}}],
        )

    def test_to_json_nested_cte_numbers(self) -> None:
        query = """WITH inner_json AS (
    SELECT TO_JSON(STRUCT(1 AS c)) AS b
)
SELECT TO_JSON(STRUCT(2 as a, b)) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": {"a": 2, "b": {"c": 1}}}],
        )

    def test_to_json_nested_outer_array(self) -> None:
        query = """WITH inner_json AS (
    SELECT TO_JSON(STRUCT(1 AS c)) AS b
)
SELECT
TO_JSON([
    TO_JSON(STRUCT('foo' AS a, b))
]) AS result
FROM inner_json;"""

        self.run_query_test(
            query,
            expected_result=[{"result": [{"a": "foo", "b": {"c": 1}}]}],
        )

    def test_to_json_date(self) -> None:
        query = """SELECT TO_JSON(STRUCT(DATE '2025-11-06' AS c)) AS b"""

        self.run_query_test(
            query,
            expected_result=[{"b": {"c": "2025-11-06"}}],
        )

    def test_nested_json_array_agg(self) -> None:
        query = """WITH inner_table AS (
  SELECT * 
  FROM UNNEST([
    STRUCT(
      "foo" AS a, TO_JSON(STRUCT(1 AS b)) AS c
    )
  ])
)
SELECT TO_JSON(ARRAY_AGG(
    TO_JSON(STRUCT(a, c))
    ORDER BY a
)) AS result
FROM inner_table;"""
        self.run_query_test(
            query,
            expected_result=[{"result": [{"a": "foo", "c": {"b": 1}}]}],
        )

    def test_array_agg(self) -> None:
        query = """
SELECT b, ARRAY_AGG(a) AS a_list
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b),
   STRUCT(3 AS a, 2 AS b)
])
GROUP BY b;
"""
        self.run_query_test(
            query,
            expected_result=[{"a_list": [1, 3], "b": 2}],
        )

    def test_array_agg_ignore_nulls_no_nulls(self) -> None:
        query = """
SELECT b, ARRAY_AGG(a IGNORE NULLS) AS a_list
FROM UNNEST([
   STRUCT(1 AS a, 2 AS b),
   STRUCT(3 AS a, 2 AS b)
])
GROUP BY b;
"""
        self.run_query_test(
            query,
            expected_result=[{"a_list": [1, 3], "b": 2}],
        )

    # TODO(https://github.com/goccy/bigquery-emulator/issues/34): File task for this
    def test_array_agg_ignore_nulls(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": None, "b": 2}, {"a": 3, "b": 2}],
        )
        query = f"""
SELECT b, ARRAY_AGG(a IGNORE NULLS) AS a_list
FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
GROUP BY b;
"""
        self.run_query_test(
            query,
            expected_result=[{"a_list": [3], "b": 2}],
        )

    def test_json_load_rows_into_table(self) -> None:
        """
        Tests resolution of https://github.com/goccy/bigquery-emulator/issues/286 loading a mock table with a JSON
        column and using JSON_TYPE, JSON_QUERY, and JSON_VALUE query functions to inspect the JSON elements.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address=address,
            schema=[
                bigquery.SchemaField(
                    "json_column",
                    field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address=address,
            data=[
                {"json_column": {"a": "2024-01-01", "b": 1.2}},
            ],
        )
        test_query = f"""
        SELECT
            JSON_TYPE(json_column) AS json_column_type,
            JSON_TYPE(JSON_QUERY(json_column, "$.a")) AS a_column_type,
            JSON_TYPE(JSON_QUERY(json_column, "$.b")) AS b_column_type,
            SAFE_CAST(JSON_VALUE(json_column, "$.a") AS DATE) AS a,
            SAFE_CAST(JSON_VALUE(json_column, "$.b") AS FLOAT64) AS b,
        FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
        """
        self.run_query_test(
            test_query,
            expected_result=[
                {
                    "json_column_type": "object",
                    "a_column_type": "string",
                    "b_column_type": "number",
                    "a": date(2024, 1, 1),
                    "b": 1.2,
                },
            ],
        )

    def test_invalid_data_load_fails(self) -> None:
        """Tests that an action causing a 500 failure actually causes
        our system to fail, rather than an infinite retry.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.DATE.value,
                    mode="REQUIRED",
                ),
            ],
        )
        with self.assertRaisesRegex(
            RuntimeError, "failed to convert 202-06-06 to time.Time type"
        ):
            self.load_rows_into_table(address, data=[{"a": "202-06-06"}])

    def test_array_agg_with_nulls(self) -> None:
        """Tests fix for https://github.com/goccy/bigquery-emulator/issues/33"""
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"a": None, "b": 2}, {"a": 3, "b": 2}],
        )
        query = f"""
SELECT b, ARRAY_AGG(a) AS a_list
FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
GROUP BY b;
"""
        with self.assertRaisesRegex(
            Exception, r"ARRAY_AGG: input value must be not null"
        ):
            self.run_query_test(
                query,
                expected_result=[{"a_list": [3], "b": 2}],
            )

    def test_null_in_unnest(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/30."""
        query = """
SELECT a
FROM UNNEST([
   STRUCT(NULL AS a)
]);
"""
        self.run_query_test(
            query,
            expected_result=[{"a": None}],
        )

    def test_date_in_unnest(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/30."""
        query = """
SELECT a
FROM UNNEST([
  STRUCT(DATE(2022, 1, 1) AS a)
]);
"""
        self.run_query_test(
            query,
            expected_result=[{"a": datetime.date(2022, 1, 1)}],
        )

    def test_cast_datetime_as_string(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/175."""
        self.run_query_test(
            """SELECT CAST(DATETIME(1987, 1, 25, 0, 0, 0) AS STRING)""",
            expected_result=[{"$col1": "1987-01-25 00:00:00"}],
        )

    def test_cast_datetime_as_string_with_format(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/175."""
        # TODO(goccy/bigquery-emulator#175): Change expected result to "SUNDAY, JANUARY 25 1987 AT 12:00:00" when fixed.
        self.run_query_test(
            """SELECT CAST(DATETIME(1987, 1, 25, 0, 0, 0) AS STRING FORMAT 'DAY"," MONTH DD YYYY "AT" HH":"MI":"SS')""",
            expected_result=[{"$col1": "1987-01-25 00:00:00"}],
        )

    def test_integer_type_alias(self) -> None:
        self.run_query_test(
            "SELECT CAST(null AS INT64)", expected_result=[{"$col1": None}]
        )
        # TODO(#33060) This should not raise an error.
        with self.assertRaises(RuntimeError):
            self.run_query_test(
                "SELECT CAST(null AS INTEGER)", expected_result=[{"$col1": None}]
            )

    def test_drop_view(self) -> None:
        """Ensures we can run DROP VIEW statements on the emulator."""
        client = self.client
        # We need a created dataset and table before we define a view.
        client.create_dataset("example_dataset")
        client.create_table(
            bigquery.Table(
                f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table",
                [
                    bigquery.SchemaField("string_field", "STRING"),
                ],
            )
        )
        _view_definition = bigquery.Table(
            f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_view"
        )
        _view_definition.view_query = (
            f"SELECT * FROM `{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table`"
        )
        example_view = client.create_table(_view_definition)
        client.delete_table(example_view)

    def test_drop_view_created_via_sql(self) -> None:
        """Ensures we can delete views created via CREATE VIEW SQL statements."""
        client = self.client
        # Create dataset and source table
        client.create_dataset("sql_view_dataset")
        client.create_table(
            bigquery.Table(
                f"{BQ_EMULATOR_PROJECT_ID}.sql_view_dataset.source_table",
                [
                    bigquery.SchemaField("id", "INTEGER"),
                    bigquery.SchemaField("name", "STRING"),
                ],
            )
        )

        # Create view using CREATE VIEW SQL statement
        create_view_query = f"""
            CREATE VIEW `{BQ_EMULATOR_PROJECT_ID}.sql_view_dataset.sql_view`
            AS SELECT id, name FROM `{BQ_EMULATOR_PROJECT_ID}.sql_view_dataset.source_table`
        """
        self.query(create_view_query)

        # Verify the view was created and has correct type
        view = client.get_table(f"{BQ_EMULATOR_PROJECT_ID}.sql_view_dataset.sql_view")
        self.assertEqual(view.table_type, "VIEW")

        # Delete the view using delete_table - this should succeed
        client.delete_table(view)

        # Verify the view is gone
        with self.assertRaises(Exception):
            client.get_table(f"{BQ_EMULATOR_PROJECT_ID}.sql_view_dataset.sql_view")

    # TODO(#39819) Update this test when the emulator quotes date values here
    def test_json_string_column(self) -> None:
        query = """
            SELECT 
                TO_JSON_STRING(
                  ARRAY_AGG(
                    STRUCT< str_col string, date_col date, int_col int64 >
                    ('strings', date("2022-01-01"), 42) 
                  )
                )
            FROM unnest([1])
        """
        self.run_query_test(
            query,
            expected_result=[
                {
                    "$col1": '[{"str_col":"strings","date_col":"2022-01-01","int_col":42}]'
                }
            ],
        )

    def test_view_schema(self) -> None:
        """Ensures views return their schema as part of the creation response."""
        client = self.client
        # We need a created dataset and table before we define a view.
        client.create_dataset("example_dataset")
        client.create_table(
            bigquery.Table(
                f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table",
                [
                    bigquery.SchemaField("string_field", "STRING"),
                ],
            )
        )
        _view_definition = bigquery.Table(
            f"{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_view"
        )
        _view_definition.view_query = (
            f"SELECT * FROM `{BQ_EMULATOR_PROJECT_ID}.example_dataset.example_table`"
        )
        example_view = client.create_table(_view_definition)
        self.assertEqual(
            example_view.schema,
            [SchemaField("string_field", "STRING", "NULLABLE")],
        )

    def test_query_from_pandas_call(self) -> None:
        # Query against the emulator, (no tables)
        df = pandas_gbq.read_gbq("SELECT 1 AS one", project_id=self.project_id)
        assert df.shape == (1, 1)
        assert df.one.iloc[0] == 1

        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "b",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="NULLABLE",
                ),
            ],
        )

        # Query against empty table
        df = pandas_gbq.read_gbq(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;"
        )
        assert df.empty

        # Load data to table and query
        self.load_rows_into_table(
            address,
            data=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )
        df = pandas_gbq.read_gbq(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;"
        )
        assert df.a.to_list() == [1, 3]
        assert df.b.to_list() == ["foo", None]

        # Gut check with emulator client
        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`;",
            expected_result=[{"a": 1, "b": "foo"}, {"a": 3, "b": None}],
        )

        # A similar mocking approach didn't quite work. More work to be done.
        with self.assertRaisesRegex(
            RuntimeError,
            "Writing to the emulator from pandas is not currently supported.",
        ):
            more_data = pd.DataFrame(
                [{"a": 42, "b": "bar"}, {"a": 43, "b": "baz"}],
                columns=["a", "b"],
            )
            pandas_gbq.to_gbq(
                more_data,
                destination_table="{address.dataset_id}.{address.table_id}",
                project_id=self.project_id,
                if_exists="append",
            )

    def test_read_bigquery_storage(self) -> None:
        """
        Test reading from BigQuery Storage API in both AVRO and ARROW formats.
        """
        for format_ in ["AVRO", "ARROW"]:
            schema = [
                bigquery.SchemaField("string_col", "STRING"),
                bigquery.SchemaField("int_col", "INTEGER"),
                bigquery.SchemaField("float_col", "FLOAT"),
                bigquery.SchemaField("bool_col", "BOOLEAN"),
                bigquery.SchemaField("bytes_col", "BYTES"),
                bigquery.SchemaField("date_col", "DATE"),
                bigquery.SchemaField("datetime_col", "DATETIME"),
                bigquery.SchemaField("timestamp_col", "TIMESTAMP"),
                bigquery.SchemaField("time_col", "TIME"),
                bigquery.SchemaField("numeric_col", "NUMERIC"),
                bigquery.SchemaField("bignumeric_col", "BIGNUMERIC"),
                bigquery.SchemaField("array_col", "STRING", mode="REPEATED"),
                bigquery.SchemaField(
                    "struct_col",
                    "RECORD",
                    fields=[
                        SchemaField("field1", "INTEGER"),
                        SchemaField("field2", "STRING"),
                    ],
                ),
            ]

            dataset_ref = DatasetReference(
                project=self.project_id, dataset_id="test_dataset"
            )
            self.client.create_dataset(dataset_ref.dataset_id, exists_ok=True)
            table_ref = TableReference(dataset_ref, f"test_table_{format_.lower()}")
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)

            table_ref = f"projects/{self.project_id}/datasets/{table.dataset_id}/tables/{table.table_id}"

            inserted_rows = [
                {
                    "string_col": "hello",
                    "int_col": 42,
                    "float_col": 3.14,
                    "bool_col": True,
                    "bytes_col": base64.b64encode(b"abc").decode("utf-8"),
                    "date_col": "2024-01-01",
                    "datetime_col": "2024-01-01T12:00:00",
                    "timestamp_col": "2024-01-01T12:00:00Z",
                    "time_col": "12:00:00",
                    "numeric_col": "123.456",
                    # BIGNUMERIC maximum value
                    "bignumeric_col": "5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38",
                    "array_col": ["x", "y"],
                    "struct_col": {"field1": 1, "field2": "nested"},
                }
            ]

            errors = self.client.insert_rows_json(table, inserted_rows)
            assert not errors, f"Insert errors: {errors}"

            read_options = bigquery_storage.ReadSession.TableReadOptions()
            parent = f"projects/{self.project_id}"

            session = bigquery_storage.ReadSession(
                table=table_ref,
                data_format=getattr(bigquery_storage.DataFormat, format_),
                read_options=read_options,
            )

            host = f"localhost:{self.emulator.grpc_port}"

            # Create an insecure gRPC channel
            channel = grpc.insecure_channel(host)

            # Build client options pointing to the local endpoint
            client_options = ClientOptions(api_endpoint=host)

            # Construct client with custom transport over the insecure channel
            transport = BigQueryReadGrpcTransport(
                channel=channel,
                host=host,
            )

            bqstorage_client = bigquery_storage.BigQueryReadClient(
                transport=transport, client_options=client_options
            )

            session = bqstorage_client.create_read_session(
                parent=parent,
                read_session=session,
                max_stream_count=1,
            )

            stream = session.streams[0].name
            reader = bqstorage_client.read_rows(stream)

            expected_values = {
                "string_col": "hello",
                "int_col": 42,
                "float_col": 3.14,
                "bool_col": True,
                "bytes_col": b"abc",
                "time_col": datetime.time(12, 0, 0),
                "date_col": datetime.date(2024, 1, 1),
                "datetime_col": datetime.datetime(2024, 1, 1, 12, 0),
                "timestamp_col": datetime.datetime(
                    2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC
                ),
                "numeric_col": Decimal("123.456"),
                "bignumeric_col": Decimal(
                    " 5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38"
                ),
                "array_col": ["x", "y"],
                "struct_col": {"field1": 1, "field2": "nested"},
            }

            frames = reader.rows(session)
            if format_ == "ARROW":
                arrow_table = frames.to_arrow()
                result_dict = arrow_table.to_pydict()
                for key in inserted_rows[0].keys():
                    if key in expected_values:
                        assert result_dict[key][0] == expected_values[key]
            elif format_ == "AVRO":
                avro_expected_overrides = {
                    # There's no concept of datetime in AVRO, so this is serialized as string
                    "datetime_col": "2024-01-01T12:00:00",
                }
                frames = reader.rows(session)
                rows = list(frames)
                for key in inserted_rows[0].keys():
                    if key in expected_values:
                        assert rows[0][key] == avro_expected_overrides.get(
                            key, expected_values[key]
                        )
            else:
                assert False, "Unsupported format"

    def test_timestamp_min_max(self) -> None:
        """Tests resolution of https://github.com/goccy/go-zetasqlite/issues/132
        and https://github.com/goccy/bigquery-emulator/issues/262"""
        self.run_query_test(
            """SELECT TIMESTAMP '0001-01-01 00:00:00.000000+00', TIMESTAMP '9999-12-31 23:59:59.999999+00'""",
            expected_result=[
                {
                    "$col1": datetime.datetime(
                        1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                    ),
                    "$col2": datetime.datetime(
                        9999, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc
                    ),
                }
            ],
        )

    def test_int64_min_max(self) -> None:
        """Tests extreme INT64 values (64-bit signed integer range)"""
        self.run_query_test(
            """SELECT -9223372036854775808 AS min_int64, 9223372036854775807 AS max_int64""",
            expected_result=[
                {
                    "min_int64": -9223372036854775808,
                    "max_int64": 9223372036854775807,
                }
            ],
        )

    def test_numeric_min_max(self) -> None:
        """Tests extreme NUMERIC values (38 digits, 9 after decimal point)"""
        self.run_query_test(
            """SELECT
                NUMERIC '-99999999999999999999999999999.999999999' AS min_numeric,
                NUMERIC '99999999999999999999999999999.999999999' AS max_numeric
            """,
            expected_result=[
                {
                    "min_numeric": Decimal("-99999999999999999999999999999.999999999"),
                    "max_numeric": Decimal("99999999999999999999999999999.999999999"),
                }
            ],
        )

    def test_bignumeric_min_max(self) -> None:
        """Tests extreme BIGNUMERIC values"""
        self.run_query_test(
            """SELECT
                BIGNUMERIC '-578960446186580977117854925043439539266.34992332820282019728792003956564819968' AS min_bignumeric,
                BIGNUMERIC '578960446186580977117854925043439539266.34992332820282019728792003956564819967' AS max_bignumeric
            """,
            expected_result=[
                {
                    "min_bignumeric": Decimal(
                        "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
                    ),
                    "max_bignumeric": Decimal(
                        "578960446186580977117854925043439539266.34992332820282019728792003956564819967"
                    ),
                }
            ],
        )

    def test_date_min_max(self) -> None:
        """Tests extreme DATE values"""
        self.run_query_test(
            """SELECT DATE '0001-01-01' AS min_date, DATE '9999-12-31' AS max_date""",
            expected_result=[
                {
                    "min_date": datetime.date(1, 1, 1),
                    "max_date": datetime.date(9999, 12, 31),
                }
            ],
        )

    def test_datetime_min_max(self) -> None:
        """Tests extreme DATETIME values"""
        self.run_query_test(
            """SELECT
                DATETIME '0001-01-01 00:00:00' AS min_datetime,
                DATETIME '9999-12-31 23:59:59.999999' AS max_datetime
            """,
            expected_result=[
                {
                    "min_datetime": datetime.datetime(1, 1, 1, 0, 0, 0),
                    "max_datetime": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
                }
            ],
        )

    def test_time_min_max(self) -> None:
        """Tests extreme TIME values"""
        self.run_query_test(
            """SELECT TIME '00:00:00' AS min_time, TIME '23:59:59.999999' AS max_time""",
            expected_result=[
                {
                    "min_time": datetime.time(0, 0, 0),
                    "max_time": datetime.time(23, 59, 59, 999999),
                }
            ],
        )

    def test_table_metadata_timestamp_format(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/432

        Table metadata timestamps (creationTime, lastModifiedTime) should be in
        Unix milliseconds (13 digits), not Unix seconds (10 digits), to match
        the real BigQuery API behavior.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "a",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
            ],
        )

        # Get the table metadata
        table = self.client.get_table(self._table_ref_for_address(address))

        # Verify that created timestamp is set and in a reasonable range
        self.assertIsNotNone(table.created)
        self.assertIsNotNone(table.modified)

        # The timestamps should be datetime objects that correspond to a recent time
        # (not 1970 which would happen if milliseconds were interpreted as seconds)
        now = datetime.datetime.now()
        self.assertGreaterEqual(table.created.year, now.year)
        self.assertGreaterEqual(table.modified.year, now.year)

    def test_bytes_field_base64_encoding(self) -> None:
        """Tests resolution of https://github.com/Recidiviz/bigquery-emulator/pull/55

        Verifies that BYTES fields are correctly handled with base64 encoding.

        According to BigQuery documentation:
        - BYTES fields must be base64-encoded when sent via JSON API (tabledata.insertAll)
        - BYTES fields are returned as base64-encoded strings when queried
        - The emulator should NOT double-encode values that are already base64-encoded

        Reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
        The BigQuery client library automatically base64-encodes byte strings before sending.

        Example from the bug report:
        - Original bytes: b'Hello' (bytes [72, 101, 108, 108, 111])
        - Expected base64: 'SGVsbG8='
        - Bug behavior (double-encoded): 'U0dWc2JHOD0=' (base64 of 'SGVsbG8=')
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "id",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "binary_data",
                    field_type=bigquery.enums.SqlTypeNames.BYTES.value,
                    mode="NULLABLE",
                ),
            ],
        )

        # Test case 1: Simple ASCII string "Hello"
        # Original bytes: b'Hello'
        # Expected base64: 'SGVsbG8='
        hello_bytes = b"Hello"
        hello_base64 = base64.b64encode(hello_bytes).decode("utf-8")
        self.assertEqual(hello_base64, "SGVsbG8=")

        # Test case 2: Binary data with various byte values
        binary_bytes = bytes([0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x00, 0xFF, 0xAB])
        binary_base64 = base64.b64encode(binary_bytes).decode("utf-8")

        # Test case 3: Empty bytes
        empty_bytes = b""
        empty_base64 = base64.b64encode(empty_bytes).decode("utf-8")

        # Load data into table
        # The BigQuery Python client expects base64-encoded strings for BYTES fields
        self.load_rows_into_table(
            address,
            data=[
                {"id": 1, "binary_data": hello_bytes},
                {"id": 2, "binary_data": binary_bytes},
                {"id": 3, "binary_data": empty_bytes},
                {"id": 4, "binary_data": None},
            ],
        )

        # Query the data back and verify it's not double-encoded
        # Note: While BigQuery's JSON API returns BYTES as base64-encoded strings,
        # the Python client library represents them as bytes objects for consistency
        self.run_query_test(
            f"SELECT id, binary_data FROM `{self.project_id}.{address.dataset_id}.{address.table_id}` ORDER BY id;",
            expected_result=[
                {"id": 1, "binary_data": hello_bytes},
                {"id": 2, "binary_data": binary_bytes},
                {"id": 3, "binary_data": empty_bytes},
                {"id": 4, "binary_data": None},
            ],
        )

        # Additional verification: use TO_BASE64 function to explicitly convert
        # This should produce the same result as the stored value
        query_with_to_base64 = f"""
            SELECT
                id,
                binary_data,
                TO_BASE64(binary_data) as explicit_base64
            FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
            WHERE binary_data IS NOT NULL
            ORDER BY id;
        """
        self.run_query_test(
            query_with_to_base64,
            expected_result=[
                {"id": 1, "binary_data": hello_bytes, "explicit_base64": hello_base64},
                {
                    "id": 2,
                    "binary_data": binary_bytes,
                    "explicit_base64": binary_base64,
                },
                {"id": 3, "binary_data": empty_bytes, "explicit_base64": empty_base64},
            ],
        )

    def test_insert_unknown_fields_valid_row(self) -> None:
        """
        Tests resolution of https://github.com/goccy/bigquery-emulator/issues/421
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "age",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )

        table = self.client.get_table(self._table_ref_for_address(address))
        valid_rows = [{"name": "Alice", "age": 30}]
        errors = self.client.insert_rows_json(table, valid_rows)

        self.assertEqual(errors, [])

    def test_insert_unknown_fields_one_bad_field(self) -> None:
        """
        Tests resolution of https://github.com/goccy/bigquery-emulator/issues/421
        Test that inserting a row with one unknown field returns an error with the field name.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "age",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )

        table = self.client.get_table(self._table_ref_for_address(address))
        bad_rows = [{"name": "Bob", "age": 25, "unknown_field": "value"}]
        errors = self.client.insert_rows_json(table, bad_rows)

        self.assertEqual(
            errors,
            [
                {
                    "index": 0,
                    "errors": [
                        {
                            "reason": "invalid",
                            "location": "unknown_field",
                            "debugInfo": "",
                            "message": "no such field: unknown_field.",
                        }
                    ],
                }
            ],
        )

    def test_insert_unknown_fields_multiple_bad_fields(self) -> None:
        """
        Tests resolution of https://github.com/goccy/bigquery-emulator/issues/421
        Test that inserting a row with multiple unknown fields returns an error with one field.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "age",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )

        table = self.client.get_table(self._table_ref_for_address(address))
        bad_rows = [
            {"name": "Charlie", "unknown1": "a", "unknown2": "b", "unknown3": "c"}
        ]
        errors = self.client.insert_rows_json(table, bad_rows)

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["index"], 0)
        self.assertEqual(len(errors[0]["errors"]), 1)
        self.assertEqual(errors[0]["errors"][0]["reason"], "invalid")
        # One of the unknown fields should be reported
        self.assertIn(
            errors[0]["errors"][0]["location"], ["unknown1", "unknown2", "unknown3"]
        )
        self.assertIn("no such field:", errors[0]["errors"][0]["message"])

    def test_insert_unknown_fields_multiple_bad_rows(self) -> None:
        """
        Tests resolution of https://github.com/goccy/bigquery-emulator/issues/421
        Test that inserting multiple bad rows returns errors for all of them.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "age",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )

        table = self.client.get_table(self._table_ref_for_address(address))
        bad_rows = [
            {"name": "Invalid1", "bad_field1": "x"},
            {"name": "Invalid2", "bad_field2": "y"},
        ]
        errors = self.client.insert_rows_json(table, bad_rows)

        # Both rows should have errors
        self.assertEqual(len(errors), 2)

        error_indices = {e["index"] for e in errors}
        self.assertEqual(error_indices, {0, 1})

        for error in errors:
            self.assertEqual(len(error["errors"]), 1)
            self.assertEqual(error["errors"][0]["reason"], "invalid")
            self.assertIn("no such field:", error["errors"][0]["message"])

    def test_insert_unknown_fields_mixed_valid_and_invalid(self) -> None:
        """
        Tests resolution of https://github.com/goccy/bigquery-emulator/issues/421

        Test inserting mix of valid and invalid rows.
        Invalid rows should have 'invalid' errors with field location.
        Valid rows should have 'stopped' errors when other rows fail.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "age",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )

        table = self.client.get_table(self._table_ref_for_address(address))
        mixed_rows = [
            {"name": "Valid1", "age": 20},  # row 0: valid
            {"name": "Invalid1", "bad_field": "x"},  # row 1: invalid
            {"name": "Valid2", "age": 30},  # row 2: valid
            {"name": "Invalid2", "bad1": "a", "bad2": "b"},  # row 3: invalid
        ]
        errors = self.client.insert_rows_json(table, mixed_rows)

        self.assertEqual(
            errors,
            [
                {
                    "index": 1,
                    "errors": [
                        {
                            "reason": "invalid",
                            "location": "bad_field",
                            "debugInfo": "",
                            "message": "no such field: bad_field.",
                        }
                    ],
                },
                {
                    "index": 3,
                    "errors": [
                        {
                            "reason": "invalid",
                            "location": "bad1",
                            "debugInfo": "",
                            "message": "no such field: bad1.",
                        }
                    ],
                },
                {
                    "index": 0,
                    "errors": [
                        {
                            "reason": "stopped",
                            "location": "",
                            "debugInfo": "",
                            "message": "",
                        }
                    ],
                },
                {
                    "index": 2,
                    "errors": [
                        {
                            "reason": "stopped",
                            "location": "",
                            "debugInfo": "",
                            "message": "",
                        }
                    ],
                },
            ],
        )

    def test_unnest_with_array_parameter(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/234

        Tests that array parameters work correctly with UNNEST operations.
        The issue reported that UNNEST with parameterized arrays failed with
        "Values referenced in UNNEST must be arrays" error.
        """
        # Test with ARRAY<STRING>
        query = """
        SELECT *
        FROM UNNEST(@states) AS state
        ORDER BY state
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("states", "STRING", ["WA", "WI", "WV", "WY"])
            ]
        )

        self.run_query_test(
            query,
            expected_result=[
                {"state": "WA"},
                {"state": "WI"},
                {"state": "WV"},
                {"state": "WY"},
            ],
            job_config=job_config,
        )

    def test_unnest_with_int_array_parameter(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/234

        Tests UNNEST with integer array parameters.
        """
        query = """
        SELECT *
        FROM UNNEST(@numbers) AS num
        ORDER BY num
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("numbers", "INT64", [1, 2, 3, 4, 5])
            ]
        )

        self.run_query_test(
            query,
            expected_result=[
                {"num": 1},
                {"num": 2},
                {"num": 3},
                {"num": 4},
                {"num": 5},
            ],
            job_config=job_config,
        )

    def test_unnest_array_parameter_with_join(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/234

        Tests UNNEST with array parameters in a JOIN operation.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "id",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        )

        query = f"""
        SELECT t.id, t.name, state
        FROM `{self.project_id}.{address.dataset_id}.{address.table_id}` t
        CROSS JOIN UNNEST(@states) AS state
        ORDER BY t.id, state
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("states", "STRING", ["CA", "NY"])
            ]
        )

        self.run_query_test(
            query,
            expected_result=[
                {"id": 1, "name": "Alice", "state": "CA"},
                {"id": 1, "name": "Alice", "state": "NY"},
                {"id": 2, "name": "Bob", "state": "CA"},
                {"id": 2, "name": "Bob", "state": "NY"},
            ],
            job_config=job_config,
        )

    def test_null_parameter_with_is_null_check(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/312

        Tests that null parameters work correctly in IS NULL conditions.
        The issue reported that null string parameters in WHERE clauses with
        "IS NULL OR parameter = value" patterns caused type inference errors.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "id",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ],
        )

        query = f"""
        SELECT id, name
        FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
        WHERE @parameter IS NULL OR name = @parameter
        ORDER BY id
        """

        # Test with null parameter - should return all rows
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("parameter", "STRING", None)
            ]
        )

        self.run_query_test(
            query,
            expected_result=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ],
            job_config=job_config,
        )

    def test_null_parameter_with_specific_value(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/312

        Tests that the same query works with both null and non-null parameter values.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "id",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "name",
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ],
        )

        query = f"""
        SELECT id, name
        FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
        WHERE @parameter IS NULL OR name = @parameter
        ORDER BY id
        """

        # Test with specific value - should filter
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("parameter", "STRING", "Alice")
            ]
        )

        self.run_query_test(
            query,
            expected_result=[
                {"id": 1, "name": "Alice"},
            ],
            job_config=job_config,
        )

    def test_null_numeric_parameter(self) -> None:
        """Tests resolution of https://github.com/goccy/bigquery-emulator/issues/312

        Tests that null numeric parameters work correctly.
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField(
                    "id",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    "value",
                    field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ],
        )

        query = f"""
        SELECT id, value
        FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
        WHERE @numParam IS NULL OR value = @numParam
        ORDER BY id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("numParam", "INT64", None)
            ]
        )

        self.run_query_test(
            query,
            expected_result=[
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ],
            job_config=job_config,
        )

    def test_positional_query_parameters(self) -> None:
        """Tests resolution of https://github.com/Recidiviz/bigquery-emulator/issues/69

        Tests that positional query parameters (?) work correctly and are not broken
        by allow_undeclared_parameters mode. The issue reported that v0.6.6-recidiviz.3.5
        broke positional parameters because allow_undeclared_parameters was enabled globally.
        According to ZetaSQL docs: "When allow_undeclared_parameters is true, no positional
        parameters may be provided."
        """
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id="positional_params_test")
        self.create_mock_table(
            address,
            schema=[
                bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INTEGER.value, mode="REQUIRED"),
                bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING.value, mode="REQUIRED"),
            ],
        )
        self.load_rows_into_table(
            address,
            data=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ],
        )

        # Test single positional parameter in WHERE clause
        query = f"""
        SELECT id, name
        FROM `{self.project_id}.{address.dataset_id}.{address.table_id}`
        WHERE id = ?
        ORDER BY id
        """

        # Positional parameters are specified using ScalarQueryParameter with name=None
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, "INT64", 2)
            ]
        )

        self.run_query_test(
            query,
            expected_result=[
                {"id": 2, "name": "Bob"},
            ],
            job_config=job_config,
        )
