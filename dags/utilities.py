import logging
from datetime import datetime
from decimal import Decimal

import pendulum
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


current_timestamp = pendulum.now("Asia/Kuala_Lumpur")


def update_data_syncDetails(dest_cur, dest_conn, table_name, current_timestamp):
    update_query = """
        UPDATE data_sync_details
        SET last_sync_date = %s AT TIME ZONE 'Asia/Kuala_Lumpur'
        WHERE table_name = %s
    """
    dest_cur.execute(update_query, (current_timestamp, table_name))
    dest_conn.commit()
    logging.info("Data transfer completed successfully and sync details updated.")


def updateInsert(
    dest_cur,
    dest_conn,
    Report_table_name,
    temp_table_name,
    column_str,
    p_primarykey,
    set_clause,
    rows,
):
    upsert_sql = f"""
            INSERT INTO "{Report_table_name}" ({column_str})
            SELECT {column_str} FROM "{temp_table_name}"
            ON CONFLICT ({p_primarykey}) DO UPDATE
            SET {set_clause}, "dag_updateddate" = NOW()"""
    print("Upsert operation executed.")
    dest_cur.execute(upsert_sql)
    dest_conn.commit()
    logging.info(
        f"Rows updated in {Report_table_name} table. Rows inserted: {len(rows)}"
    )


def get_fromdate_todate(dest_cur, table_name):
    dest_cur.execute(
        """
       select p_fromdate,p_todate,p_synchour,p_primarykey ,p_last_sync_date from synchour(%s);
    """,
        (table_name,),
    )
    p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date = (
        dest_cur.fetchone()
    )
    logging.info(
        f"starting QueryDate {p_fromdate}, End QueryDate: {p_todate},Syncing Hour: {p_synchour}"
    )
    return p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date


def get_Table_columns(dest_cur, table_name):
    query = """
       SELECT column_name
        FROM report_table_list
        WHERE table_name = %s
        ORDER BY order_position
    """
    dest_cur.execute(query, (table_name,))
    columns = dest_cur.fetchall()
    column_names = [f'"{col[0]}"' for col in columns]
    logging.info(f"Destination columns: {column_names}")
    return column_names
