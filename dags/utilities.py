import logging
from datetime import datetime
from decimal import Decimal

from airflow import DAG
from ReportConnection import get_reportdb_connection

# Define the default_args that are common for all DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 27),  # You can change this to a dynamic date
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "catchup": False,
    "max_active_runs": 1,
    "concurrency": 1,
}


def truncateTable(table_name):
    """Truncate the specified table in the destination database."""
    query = f'TRUNCATE TABLE "{table_name}"'
    return query


def get_stored_procedure_for_table(table_name):
    table_to_sp_map = {
        "TransactionService_Transaction": "usp_Transaction_pipeline"
        # please add more mappings of table names to stored procedures as needed
    }

    try:
        return table_to_sp_map[table_name]
    except KeyError:
        raise KeyError(f"No stored procedure mapped for table: {table_name}")
