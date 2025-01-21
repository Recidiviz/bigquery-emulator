"""
This example creates a dataset, table, and view,
shows that the output of the table and view are expected,
and removes the dataset, table, and view.
"""

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

if __name__ == "__main__":
    client_options = ClientOptions(api_endpoint="http://0.0.0.0:9050")
    client = bigquery.Client(
        "test",
        client_options=client_options,
        credentials=AnonymousCredentials(),
    )

    client.create_dataset("example_dataset")

    print("Setting up table")
    example_table = client.create_table(
        bigquery.Table(
            "test.example_dataset.example_table",
            [
                bigquery.SchemaField("string_field", "STRING"),
            ],
        )
    )
    print("Loading table")
    rows_to_insert = [
        {"string_field": "An example string"},
        {"string_field": "Hello, BigQuery!"},
    ]
    _ = client.insert_rows_json("test.example_dataset.example_table", rows_to_insert)

    print("Setting up view")
    _view_definition = bigquery.Table("test.example_dataset.example_view")
    _view_definition.view_query = "SELECT * FROM `test.example_dataset.example_table`"
    example_view = client.create_table(_view_definition)

    print("Running queries")
    table_data = list(
        client.query(
            query="SELECT * FROM example_dataset.example_table",
            job_config=bigquery.QueryJobConfig(),
        )
    )

    view_data = list(
        client.query(
            query="SELECT * FROM example_dataset.example_view",
            job_config=bigquery.QueryJobConfig(),
        )
    )

    assert table_data == view_data

    print("Cleaning up tables")
    client.delete_table(example_view)
    client.delete_table(example_table)
    client.delete_dataset("example_dataset")
