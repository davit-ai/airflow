import logging

from connection import get_reportdb_connection, get_transaction_connection

## This connection is used for the Destination Database\
dest_conn = get_reportdb_connection()
dest_cur = dest_conn.cursor()


def get_Table_columns(dest_cur):
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
