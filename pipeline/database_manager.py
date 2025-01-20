# This will create and execute the SQL necessary to manage our database
from typing import List
import os
import argparse
import logging

from duckdb import DuckDBPyConnection
import duckdb as ddb

def connect_to_database(path: str) -> DuckDBPyConnection:
    logging.info(f"Connecting to database at {path}")
    con = ddb.connect(path)
    con.sql("""
            SET s3_access_key_id='';
            SET s3_secret_access_key='';
            SET s3_region='';
            
            """)
    return con

# Note: The None is type hinting and lets us know that nothing is returned in this function. 
def close_database_connection(con: DuckDBPyConnection) -> None:
    logging.info(f"Closing database connection")
    con.close()


def collect_query_paths(parent_dir: str) -> List[str]:
    # this will return a list of full paths to the different queries 
    sql_files = []

    # os.walk() finds a parent directory and 'walk' through it
    for root, _, files in os.walk(parent_dir):
            for file in files:
                if file.endswith(".sql"):
                    file_path = os.path.join(root, file)
                    sql_files.append(file_path)

    logging.info(f"Found {len(sql_files)} sql scripts at location {parent_dir}")

    # the sorted with will use the 0 and 1 prefixes to sort. 
    return sorted(sql_files)

def read_query(path: str) -> str:
    with open(path, 'r') as f:
        query = f.read()
        f.close()
    return query

def execute_query(con: DuckDBPyConnection, query: str) -> None:
    con.execute(query)

def setup_database(database_path: str, ddl_query_parent_dir: str) -> None:
    
    query_paths = collect_query_paths(ddl_query_parent_dir)

    con = connect_to_database(database_path)

    for query_path in query_paths:
        query = read_query(query_path)
        execute_query(con, query)
        logging.info(f"Executed query from {query_path}")

    # Ensure we dont' have any hanging db connections
    close_database_connection(con)


# Note: Trent said he made this destroy function for convenience during developement, but he said having a function that destroys the database is not a good practice
def destroy_database(database_path: str) -> None:
    if os.path.exists(database_path):
        os.remove(database_path)
        

def main():

    logging.getLogger().setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser(description="CLI tool to setup or destroy a database")

    group = parser.add_mutually_exclusive_group(required=True)
    
    group.add_argument("--create", action="store_true", help="Create the database")
    group.add_argument("--destroy", action="store_true", help="Destroy the database")

    parser.add_argument("--database-path", type=str, help="Path to the database")
    parser.add_argument("--ddl-query-parent-dir", type=str, help="Path to the parent directory of the ddl queries")

    args = parser.parse_args()

    if args.create:
        # Note: Python knows to remove the -- in --create and changes dashes to underscores. So, from --database-path to .database_path
        setup_database(database_path=args.database_path, ddl_query_parent_dir=args.ddl_query_parent_dir)
    elif args.destroy:
        destroy_database(database_path=args.database_path)


if __name__ == "__main__":
    main()