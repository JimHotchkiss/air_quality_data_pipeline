import argparse
import json 
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List

from duckdb import IOException
from jinja2 import Template

from database_manager import (
    connect_to_database,
    close_database_connection, 
    execute_query, 
    read_query
)

# Read various locations from locations.json, being passed the file_path to said locations.json, and it will return a list of location ids
def read_location_ids(file_path: str) -> List[str]:
    with open(file_path, "r") as f:
        locations = json.load(f)
    # Note: the key, in locations.keys() grabs the key of the dict in locations. For example, "1311": "Laney College", "1311" is a sensor id. [str(id) for id in locations.keys()] iterates over these keys and converts each key to a string. The function will return a list, locations_ids, of strings
    location_ids = [str(id) for id in locations.keys()]
    return location_ids


# Note: The reason we're not using asynchronous patterns is because DuckDb does not like congruent connections opened. 
def compile_data_file_paths(data_file_path_template: str, location_ids: List[str], start_date: str, end_date: str) -> List[str]:
    # 4. Bring in the temple (needed for the API url), the location_ids and start and end dates
    # Note: Trent is recreating the path to the data on the API - location - {locationid}-{year}{month}{day}(which will omit day).csv.gz

    # 4.a - Convert start and end date into year and month. strptime will configure the datetime if passed a string and given the format: Year-month, in this case, like 2024-01
    start_date   = datetime.strptime(start_date, "%Y-%m")
    end_date   = datetime.strptime(end_date, "%Y-%m")

    data_file_paths = []
    # 4.b - We want to iterate through the locations and the start date and end date. The outer loop is the different loctions, and the inner loop in the start and end dates. locations_ids comes from the argument that we passed to this function. 
    for location_id in location_ids:
        index_date = start_date
        while index_date <= end_date:
            # The Template() function is being used to dynamically construct file paths by replacing placeholders in a string template -  data_file_path_template = "locationid={{location_id}}/year={{year}}/month={{month}}/*" - with actual values: the location_id from the location_ids, start_date and end_date (which we've defined in our loop as index_date). .render(...) replaces those placeholders with actual values (location_id, year, and month), generating a formatted file path. 
            data_file_path = Template(data_file_path_template).render(
                location_id = location_id,
                year = str(index_date.year),
                # Month requires a leading zero for single digit months. zfill() - Pad a numeric string with zeros on the left, to fill a field of the given width.The string is never truncated.
                month = str(index_date.month).zfill(2),
           
            )
            # Then we append this newly compiled string to the data_file_paths list, and we 
            data_file_paths.append(data_file_path)
            # Then increment our index_date, moving the date forward by 1 month 
            index_date += relativedelta(months=1)
    return data_file_paths

def compile_data_file_query(base_path: str, data_file_path: str, extract_query_template: str) -> str:
    extract_query = Template(extract_query_template).render(
        data_file_path = f"{base_path}/{data_file_path}"
    )
    return extract_query

def extract_data(args):
    # 3. We gather the elements needed for the API url input, like this - locationid=2009/year=2024/month=01/*
    location_ids = read_location_ids(args.locations_file_path)

    data_file_path_template = "locationid={{location_id}}/year={{year}}/month={{month}}/*"

    data_file_paths = compile_data_file_paths(
        # data_file_path_template is recreating - locationid=2009/year=2024/month=01/* for the API url
        data_file_path_template=data_file_path_template,
        location_ids=location_ids,
        start_date=args.start_date,
        end_date=args.end_date
    )

    extract_query_template = read_query(path=args.extract_query_template_path)
    print(f"Extracted query template: {extract_query_template}")

    con = connect_to_database(path=args.database_path)

    for data_file_path in data_file_paths:
        logging.info(f"Extracting data from {data_file_path}")
        query = compile_data_file_query(
            base_path=args.source_base_path,
            data_file_path=data_file_path,
            extract_query_template=extract_query_template
        )

        try:
            execute_query(con, query)
            logging.info(f"Extracted data from {data_file_path}")
        except IOException as e:
            # I think {data_file_path: {e}} was the issue
            logging.warning(f"Could not find data from {data_file_path}: {e}")
    close_database_connection(con)
    

# Test compile_data_file_paths() with dummy data
def main():
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description="CLI for ELT Extraction")
    # Path to the JSON file
    parser.add_argument(
        "--locations_file_path",
        type=str,
        required=True,
        help="Path to the locations JSON file"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        required=True,
        help="Start date in YYYY-MM format"
    )
    parser.add_argument(
        "--end_date",
        type=str,
        required=True,
        help="End date in YYYY-MM format"
    )
    # This is the path to the sql query
    parser.add_argument(
        "--extract_query_template_path",
        type=str,
        required=True,
        help="Path to the SQL extraction query template"
    )
    # This is the path to the database
    parser.add_argument(
        "--database_path",
        type=str,
        required=True,
        help="Path to database"
    )
    # Note this is the API url
    parser.add_argument(
        "--source_base_path",
        type=str,
        required=True,
        help="Base path for the remote data files"
    )

    args = parser.parse_args()
    # 2. Parses the users input and passes them as args to extract_data()
    extract_data(args)

# Example usage: python extraction.py --locations_file_path ../locations.json --start_date 2024-01 --end_date 2024-03 --database_path ../air_quality.db --extract_query_template_path ../sql/dml/raw/0_raw_air_quality_insert.sql --source_base_path s3://openaq-data-archive/records/csv.gz

if __name__ == "__main__":
    # 1. main() is called
    main()