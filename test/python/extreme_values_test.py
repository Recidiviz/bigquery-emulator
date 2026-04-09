"""Tests for extreme/boundary values in BigQuery types.

This test suite validates that extreme values (min/max) for various BigQuery
data types are correctly handled by both the REST API and Storage Arrow API.
"""
import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable

import grpc
from google.cloud import bigquery_storage
from google.cloud.bigquery_storage_v1.services.big_query_read.transports import (
    BigQueryReadGrpcTransport,
)
from google.api_core.client_options import ClientOptions

from utils.big_query_emulator_test_case import BigQueryEmulatorTestCase


class TestExtremeValues(BigQueryEmulatorTestCase):
    """Tests for extreme/boundary values in BigQuery types."""

    def _run_test_with_both_apis(
        self,
        query_str: str,
        expected_result: Iterable[Dict[str, Any]],
    ) -> None:
        """Run the same query against both REST API and Storage Arrow API.

        Args:
            query_str: SQL query to execute
            expected_result: Expected results as list of dictionaries
        """
        # Test 1: REST API via job.result()
        query_job = self.client.query(query=query_str)
        rest_contents = list(
            {key: row.get(key) for key in row.keys()} for row in query_job.result()
        )
        self.assertEqual(
            list(expected_result),
            rest_contents,
            f"REST API results don't match. Expected: {expected_result}, Got: {rest_contents}"
        )

        # Test 2: Storage Arrow API via query_job.to_dataframe()
        # Create a fresh query for the Storage API test
        query_job = self.client.query(query=query_str)

        # Patch the client to use Storage API
        host = f"localhost:{self.emulator.grpc_port}"
        channel = grpc.insecure_channel(host)
        transport = BigQueryReadGrpcTransport(channel=channel, host=host)
        bqstorage_client = bigquery_storage.BigQueryReadClient(
            transport=transport,
            client_options=ClientOptions(api_endpoint=host),
        )

        # Get DataFrame using Storage API
        df = query_job.to_dataframe(bqstorage_client=bqstorage_client)

        # Convert DataFrame to same format as expected_result
        arrow_contents = df.to_dict(orient='records')

        self.assertEqual(
            list(expected_result),
            arrow_contents,
            f"Storage Arrow API results don't match. Expected: {expected_result}, Got: {arrow_contents}"
        )

        channel.close()

    def test_timestamp_min_max(self) -> None:
        self._run_test_with_both_apis(
            """SELECT TIMESTAMP '0001-01-01 00:00:00.000000+00' AS min_ts, TIMESTAMP '9999-12-31 23:59:59.999999+00' AS max_ts""",
            expected_result=[
                {
                    "min_ts": datetime.datetime(
                        1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                    ),
                    "max_ts": datetime.datetime(
                        9999, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc
                    ),
                }
            ],
        )

    def test_int64_min_max(self) -> None:
        """Tests extreme INT64 values (64-bit signed integer range)"""
        self._run_test_with_both_apis(
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
        self._run_test_with_both_apis(
            """SELECT
                NUMERIC '-99999999999999999999999999999.999999999' AS min_numeric,
                NUMERIC '99999999999999999999999999999.999999999' AS max_numeric
            """,
            expected_result=[
                {
                    "min_numeric": Decimal('-99999999999999999999999999999.999999999'),
                    "max_numeric": Decimal('99999999999999999999999999999.999999999'),
                }
            ],
        )

    def test_bignumeric_min_max(self) -> None:
        """Tests extreme BIGNUMERIC values"""
        self._run_test_with_both_apis(
            """SELECT
                BIGNUMERIC '-578960446186580977117854925043439539266.34992332820282019728792003956564819968' AS min_bignumeric,
                BIGNUMERIC '578960446186580977117854925043439539266.34992332820282019728792003956564819967' AS max_bignumeric
            """,
            expected_result=[
                {
                    "min_bignumeric": Decimal('-578960446186580977117854925043439539266.34992332820282019728792003956564819968'),
                    "max_bignumeric": Decimal('578960446186580977117854925043439539266.34992332820282019728792003956564819967'),
                }
            ],
        )

    def test_date_min_max(self) -> None:
        """Tests extreme DATE values"""
        self._run_test_with_both_apis(
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
        self._run_test_with_both_apis(
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
        self._run_test_with_both_apis(
            """SELECT TIME '00:00:00' AS min_time, TIME '23:59:59.999999' AS max_time""",
            expected_result=[
                {
                    "min_time": datetime.time(0, 0, 0),
                    "max_time": datetime.time(23, 59, 59, 999999),
                }
            ],
        )