import logging


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


def get_fromdate_todate(dest_cur, table_name):
    dest_cur.execute(
        """
       selecT p_fromdate,p_todate,p_synchour,p_primarykey ,p_last_sync_date from synchour(%s);
    """,
        (table_name,),
    )
    p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date = (
        dest_cur.fetchone()
    )
    logging.info(
        f"startingQueryDate {p_fromdate}, EndQueryDate: {p_todate},Syncing Hour: {p_synchour}"
    )
    return p_fromdate, p_todate, p_synchour, p_primarykey, p_last_sync_date
