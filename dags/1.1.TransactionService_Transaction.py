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
from ReportConnection import get_reportdb_connection, get_transaction_connection
from TableInformation import get_fromdate_todate, get_Table_columns
from utilities import (
    default_args,
    get_stored_procedure_for_table,
    truncateTable,
    update_data_syncDetails,
    updateInsert,
)

## malaysia ko local timezone
local_tz = pendulum.timezone("Asia/Kuala_Lumpur")
## table ra store procedure ko naam haru
Report_table_name = "TransactionService_Transaction"
temp_table_name = f"temp_{Report_table_name}"
spName = get_stored_procedure_for_table(table_name=Report_table_name)

## basic dag lai chaine kura haru
dag = DAG(
    f"{Report_table_name.split('_', 1)[1]}_Reporting",
    default_args=default_args,
    description="Transfer data from mssql.dbo.mirs to postgres.public.report",
    schedule_interval=None,
    tags=[
        f"{Report_table_name.split('_')[0]}_{Report_table_name.split('_')[1]}",
        "Report_UAT",
    ],
)


## table transfer garna lai chaine kura haru
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

    query = f"""
    EXEC dbo.{spName} @Offset=?, @FetchNext=?, @p_fromdate=?, @p_todate=?
    """
    source_cur.execute(query, (offset, batch_size, p_fromdate, p_todate))
    return source_cur.fetchall()


def transfer_data():
    try:
        source_conn = get_transaction_connection()
        logging.info("Connected to source database.")

        dest_conn, dbName = get_reportdb_connection()
        logging.info("Connected to destination database.")

        with source_conn.cursor() as source_cur, dest_conn.cursor() as dest_cur:
            column_names = get_Table_columns(
                dest_cur=dest_cur,
                table_name=Report_table_name,
            )
            column_str = ", ".join(column_names)

            (p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date) = (
                get_fromdate_todate(dest_cur, table_name=Report_table_name)
            )

            if p_synchour is None:
                query = truncateTable(table_name=Report_table_name)

                dest_cur.execute(query)
                logging.info("Destination table truncated.")

                batch_size = 100
                offset = 0

                print(f"Executing SP query: {spName}")
                while True:
                    query = f"""
                        EXEC dbo.{spName} @Offset=?, @FetchNext=?, @p_fromdate=?, @p_todate=?
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
                        COPY "{Report_table_name}" ({column_str})
                        FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')
                        """,
                        buffer,
                    )
                    dest_conn.commit()
                    logging.info(
                        f"Batchs inserted into '{Report_table_name}'. Rows inserted: {len(rows)}"
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
                    ## yo function le temp table create garne kaam garcha
                    ## ani temp table ma data copy garne kaam garcha
                    dest_cur.execute(
                        f"""
                    CREATE TEMP TABLE "{temp_table_name}" (
                        LIKE "{Report_table_name}"
                        INCLUDING DEFAULTS INCLUDING CONSTRAINTS) """,
                        (Report_table_name,),
                    )
                    ## ani temp table ma data copy garne kaam garcha
                    dest_cur.copy_expert(
                        f"""
                        COPY "{temp_table_name}" ({column_str})
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

                    ## yo fucntion le upsert garne kaam garcha
                    updateInsert(
                        dest_cur=dest_cur,
                        dest_conn=dest_conn,
                        Report_table_name=Report_table_name,
                        temp_table_name=temp_table_name,
                        column_str=column_str,
                        p_primarykey=p_primarykey,
                        set_clause=set_clause,
                        rows=rows,
                    )

            # Update sync details once all data has been inserted
            current_timestamp = pendulum.now("Asia/Kuala_Lumpur")

            update_data_syncDetails(
                dest_cur=dest_cur,
                dest_conn=dest_conn,
                table_name=Report_table_name,
                current_timestamp=current_timestamp,
            )

    except Exception as e:
        logging.error("Error occurred while transferring data: %s", e)
        raise


transfer_data_task = PythonOperator(
    task_id=f"{Report_table_name.split('_', 1)[1]}_Reporting",
    python_callable=transfer_data,
    dag=dag,
)
