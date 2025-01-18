# This will create and execute the SQL necessary to manage our database
from typing import List
import os
import argparse
import logging

from duckdb import DuckDBPyConnection
import duckdb as ddb

def connect_to_database(path: str) -> DuckDBPyConnection:
    con = ddb.connect(path)
    con.sql("""
            SET s3_access_key_id='';
            SET s3_secret_access_key='';
            SET s3_region='';
            
            """)


def close_database_connection(con: DuckDBPyConnection) -> None:
    pass


def collect_query_paths(parent_dir: str) -> List[str]:
    pass

def read_query(path: str) -> str:
    pass

def execute_query(con: DuckDBPyConnection, query: str) -> None:
    pass

def setup_database(database_path: str, ddl_query_parent_dir: str) -> None:
    pass

def destroy_database(database_path: str) -> None:
    pass

def main():
    pass

if __name__ == "__main__":
    main()