# connections.py
import logging
from datetime import datetime, timedelta

import psycopg2
import pyodbc
from airflow.hooks.base_hook import BaseHook


def get_configuration_connection():
    source_conn = BaseHook.get_connection("MIRS_CONFIGURATION_SERVICE_DB_UAT")
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={source_conn.host};"
        f"DATABASE={source_conn.schema};"
        f"UID={source_conn.login};"
        f"PWD={source_conn.password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def close_sql_server_connection(conn):
    try:
        conn.close()
    except Exception as e:
        print(f"Error closing SQL Server connection: {e}")


def get_customer_connection():
    source_conn = BaseHook.get_connection("MIRS_CUSTOMER_SERVICE_DB_UAT")
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={source_conn.host};"
        f"DATABASE={source_conn.schema};"
        f"UID={source_conn.login};"
        f"PWD={source_conn.password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_transaction_connection():
    source_conn = BaseHook.get_connection("MIRS_TRANSACTION_REPORT")
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={source_conn.host};"
        f"DATABASE={source_conn.schema};"
        f"UID={source_conn.login};"
        f"PWD={source_conn.password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_auth_connection():
    source_conn = BaseHook.get_connection("MIRS_AUTH_SERVICE_DB_UAT")
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={source_conn.host};"
        f"DATABASE={source_conn.schema};"
        f"UID={source_conn.login};"
        f"PWD={source_conn.password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_reportdb_connection():
    dest_conn_info = BaseHook.get_connection("MIRS_REPORT_DB_UAT")

    dest_conn_str = f"dbname={dest_conn_info.schema} user={dest_conn_info.login} password={dest_conn_info.password} host={dest_conn_info.host} port={dest_conn_info.port}"
    return psycopg2.connect(dest_conn_str)
