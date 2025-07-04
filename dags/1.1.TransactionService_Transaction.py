import csv
import logging
from datetime import datetime
from io import StringIO

import pendulum
import psycopg2
import pyodbc
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from connection import get_reportdb_connection, get_transaction_connection
from utilities import default_args

local_tz = pendulum.timezone("Asia/Kuala_Lumpur")

p_tags = ["Transaction_Transaction", "Report_UAT"]

dag = DAG(
    "1.01.initial_Transaction",
    default_args=default_args,
    description="Transfer data from mssql.dbo.mirs to postgres.public.report",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["Transaction_Transaction", "Report_UAT"],
)


def get_Transaction_columns(dest_cur):
    dest_cur.execute("""
        SELECT column_name
        FROM report_table_list
        WHERE table_name = 'TransactionService_Transaction'
        ORDER BY order_position
    """)
    columns = dest_cur.fetchall()
    column_names = [f'"{col[0]}"' for col in columns]
    logging.info(f"Destination columns: {column_names}")
    return column_names


def get_fromdate_todate(dest_cur):
    dest_cur.execute("""
       selecT p_fromdate,p_todate,p_synchour,p_primarykey ,p_last_sync_date from synchour('TransactionService_Transaction');
    """)
    p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date = (
        dest_cur.fetchone()
    )
    logging.info(
        f"startingQueryDate {p_fromdate}, EndQueryDate: {p_todate},Syncing Hour: {p_synchour}"
    )
    return p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date


def get_rows_for_update(
    source_cur,
    p_fromdate,
    p_todate,
    column_names,
    p_synchour,
    p_primarykey,
    p_last_sync_date,
):
    # Assuming this function returns the rows with the specified columns
    column_list = ", ".join(column_names)
    batch_size = 1000
    offset = 0

    query = """
    EXEC dbo.usp_Transaction_pipeline @Offset=?, @FetchNext=?, @p_fromdate=?, @p_todate=?
    """
    source_cur.execute(query, (offset, batch_size, p_fromdate, p_todate))
    return source_cur.fetchall()


def transfer_data():
    try:
        source_conn = get_transaction_connection()
        logging.info("Connected to source database.")
        dest_conn = get_reportdb_connection()
        logging.info("Connected to destination database.")

        with source_conn.cursor() as source_cur, dest_conn.cursor() as dest_cur:
            column_names = get_Transaction_columns(dest_cur)
            column_str = ", ".join(column_names)

            (p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date) = (
                get_fromdate_todate(dest_cur)
            )

            if p_synchour is None:
                truncate_query = (
                    'TRUNCATE TABLE report_uat.public."TransactionService_Transaction"'
                )
                dest_cur.execute(truncate_query)
                logging.info("Destination table truncated.")

                batch_size = 100
                offset = 0

                while True:
                    query = f"""
                        EXEC dbo.usp_Transaction_pipeline @Offset=?, @FetchNext=?, @p_fromdate=?, @p_todate=?
                    """
                    source_cur.execute(
                        query, (offset, batch_size, p_fromdate, p_todate)
                    )
                    rows = source_cur.fetchall()
                    if not rows:
                        break

                    buffer = StringIO()
                    writer = csv.writer(buffer, delimiter="\t")
                    writer.writerows(rows)
                    buffer.seek(0)

                    # Perform bulk insert using COPY
                    dest_cur.copy_expert(
                        f"""
                        COPY report_uat.public."TransactionService_Transaction" ({column_str})
                        FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')
                        """,
                        buffer,
                    )
                    dest_conn.commit()
                    logging.info(
                        f"Batchs inserted into Transaction table. Rows inserted: {len(rows)}"
                    )

                    offset += batch_size
            else:
                rows = get_rows_for_update(
                    source_cur,
                    p_fromdate,
                    p_todate,
                    column_names,
                    p_synchour,
                    p_primarykey,
                    p_last_sync_date,
                )

                if not rows:
                    logging.info("No new rows to update.")
                else:
                    buffer = StringIO()
                    writer = csv.writer(buffer, delimiter="\t")
                    writer.writerows(rows)
                    buffer.seek(0)

                    dest_cur.execute("""
                    CREATE TEMP TABLE temp_txn_transaction (
                        LIKE report_uat.public."TransactionService_Transaction"
                        INCLUDING DEFAULTS INCLUDING CONSTRAINTS) """)

                    dest_cur.copy_expert(
                        f"""
                        COPY temp_txn_transaction ({column_str})
                        FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')
                        """,
                        buffer,
                    )

                    set_clause = ", ".join(
                        [
                            f"{col} = EXCLUDED.{col}"
                            for col in column_names
                            if col.strip('"') != p_primarykey
                        ]
                    )
                    upsert_sql = f"""
                         INSERT INTO report_uat.public."TransactionService_Transaction" ({column_str})
                         SELECT {column_str} FROM temp_txn_transaction
                         ON CONFLICT ({p_primarykey}) DO UPDATE
                         SET {set_clause}, "dag_updateddate" = NOW()"""

                    dest_cur.execute(upsert_sql)
                    dest_conn.commit()
                    logging.info(
                        f"Rows updated in Transaction table. Rows inserted: {len(rows)}"
                    )

            # Update sync details once all data has been inserted
            current_timestamp = pendulum.now("Asia/Kuala_Lumpur")
            dest_cur.execute(
                """
                UPDATE report_uat.public.data_sync_details
                SET last_sync_date = %s AT TIME ZONE 'Asia/Kuala_Lumpur'
                WHERE table_name = 'TransactionService_Transaction'
                """,
                (current_timestamp,),
            )
            dest_conn.commit()
            logging.info(
                "Data transfer completed successfully and sync details updated."
            )

    except Exception as e:
        logging.error("Error occurred while transferring data: %s", e)
        raise


transfer_data_task = PythonOperator(
    task_id="1.01.initial_Transaction", python_callable=transfer_data, dag=dag
)
