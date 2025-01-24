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
        locations: Dict[str, str] = json.load(f)
       # f.close() - close() isn't necessary when using the 'with' statement
    
    location_ids: List[str] = [str(id) for id in locations.keys()]
    return location_ids