from datetime import datetime
from decimal import Decimal
from airflow import DAG

# Define the default_args that are common for all DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),  # You can change this to a dynamic date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1
}

dagsData = DAG(
    "1.01.initial_Transaction",
    default_args=default_args,
    description="Transfer data from mssql.dbo.mirs to postgres.public.report",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["Transaction_Transaction", "Report_UAT"],
)

def convert_value(value, field_name=None):
    """Helper function to convert values to appropriate format"""
    if value is None:
        return None
    elif isinstance(value, datetime):
        return value.isoformat() if value else None
    elif isinstance(value, (bytes, bytearray)):
        return str(value)
    elif isinstance(value, Decimal):
        return str(value)
    elif field_name == 'Id':
        return str(value)
    elif field_name in ['LimitPerDay', 'LimitPerMonth'] and value is not None:
        return str(value)
    return value