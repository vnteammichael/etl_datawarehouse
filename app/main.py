"""Script entry point."""
from datetime import datetime
import os
# os.chdir(os.path.dirname(__file__))
import sys
from dotenv import load_dotenv

from argparse import ArgumentParser
import yaml

parser = ArgumentParser()
parser.add_argument("-env", "--env-file", dest="FILENAME",default=".env", help="ENV config")
parser.add_argument("-a", "--action", dest="ACTION", help="Action: daily/dim", required=True)
parser.add_argument("-s", "--start-date", dest="START_DATE", help="Start date or special date")
parser.add_argument("-e", "--end-date", dest="END_DATE", help="End date")

args = parser.parse_args()

runner = args.ACTION
start_date = args.START_DATE
end_date = args.END_DATE
load_dotenv(args.FILENAME)
# def read_config_from_file(filename: str = args.FILENAME):
#     try:
#         with open(filename, "r") as file:
#             config = yaml.safe_load(file)
#         return config
#     except Exception as e:
#         print(f"Error reading configuration: {str(e)}")

# config = read_config_from_file()

from config import (
    MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_NAME
)
from src import init_script

from src.modules.db_clickhouse import DatabaseClickHouse
from src.modules.db_mariadb import MariaDBConnector
# Create database class
# db_clickhouse = DatabaseClickHouse(CLICKHOUSE_HOST, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD, CLICKHOUSE_PORT, CLICKHOUSE_NAME)
# print(type(MYSQL_HOST), type(MYSQL_USERNAME), type(MYSQL_PASSWORD), type(MYSQL_PORT), type(MYSQL_NAME))
db = MariaDBConnector(host=MYSQL_HOST, user=MYSQL_USERNAME, password=MYSQL_PASSWORD, port=MYSQL_PORT, database=MYSQL_NAME)

if runner == None:
    runner = "daily"

if start_date == None and end_date == None:
    start_date = end_date = datetime.today().strftime('%Y-%m-%d')
if start_date and end_date == None:
    end_date = start_date

if __name__ == "__main__":
    init_script(runner, start_date, end_date, db)