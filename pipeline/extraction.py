# Goal - Create a query of data file paths, and use those to insert data into our database
import argparse
import json 
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List

from duckdb import IOException
from jinja2 import Template

from database_manager import (
    connect_to_database,
    close_database_connection, 
    execute_query, 
    read_query
)

# Read the varios location ids from locations.json
def read_location_ids(file_path: str) -> List[str]:
    with open(file_path, "r") as f:
        locations = json.load(f)
       # f.close() - close() isn't necessary when using the 'with' statement
    
    location_ids = [str(id) for id in locations.keys()]
    return location_ids


def compile_data_file_paths(
        data_file_path_template: str, location_ids: List[str], start_date: str, end_date: str) -> List[str]:
    
    start_date   = datetime.strptime(start_date, "%Y-%m")
    end_date   = datetime.strptime(end_date, "%Y-%m")

    data_file_paths = []
    for location_id in location_ids:
        index_date = start_date
        while index_date <= end_date:
            data_file_path = Template(data_file_path_template).render(
                location_id = location_id,
                year = str(index_date.year),
                # Month requires a leading zero for single digit months. zfill() - Pad a numeric string with zeros on the left, to fill a field of the given width.The string is never truncated.
                month = str(index_date.month).zfill(2),
           
            )
            data_file_paths.append(data_file_path)
            index_date += relativedelta(months=1)
    return data_file_paths
def compile_data_file_query(
        base_path: str, data_file_path: str, extract_query_template: str
) -> str:
    extract_query = Template(extract_query_template).render(
        data_file_path = f"{base_path}/{data_file_path}"
    )
    return extract_query

def extract_data(args):
    pass

# Test compile_data_file_paths() with dummy data
def main():
    location_ids = ["123", "456"]
    # This creates the file path to the OpenAQ data 
    data_file_path_template = "locationid={{ location_id}}/year={{year}}/month={{month}}"

    data_file_paths = compile_data_file_paths(
        data_file_path_template, location_ids, "2024-10" , "2024-12"
    )

    with open("../sql/dml/raw/0_raw_air_quality_insert.sql") as f:
        query_template = f.read()
    
    for data_file_path in data_file_paths:
        query = compile_data_file_query("s3://some_url_string", data_file_path, query_template)
        print(query)

# Note: This is the kind of url we're trying to create to match OpenAQ's documentation: /records/csv.gz/locationid=2178/year=2022/month=05/location-2178-20220503.csv.gz

# Note: This is the list of data file paths: ['locationid=123/year=2024/month=01', 'locationid=123/year=2024/month=02', 'locationid=123/year=2024/month=03', 'locationid=456/year=2024/month=01', 'locationid=456/year=2024/month=02', 'locationid=456/year=2024/month=03']

# Note: The API's example shows the months with a leading 0, when not double digit



if __name__ == "__main__":
    main()