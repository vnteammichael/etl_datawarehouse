# """Load config from environment variables."""
from os import environ

# Database config
MYSQL_HOST = environ.get('ETL_MYSQL_HOST')
MYSQL_USERNAME = environ.get('ETL_MYSQL_USERNAME')
MYSQL_PASSWORD = environ.get('ETL_MYSQL_PASSWORD')
MYSQL_PORT = environ.get('ETL_MYSQL_PORT')
MYSQL_NAME = environ.get('ETL_MYSQL_NAME')

MYSQL_XSN_HOST = environ.get('MYSQL_XSN_HOST')
MYSQL_XSN_USERNAME = environ.get('MYSQL_XSN_USERNAME')
MYSQL_XSN_PASSWORD = environ.get('MYSQL_XSN_PASSWORD')
MYSQL_XSN_PORT = environ.get('MYSQL_XSN_PORT')
MYSQL_XSN_NAME = environ.get('MYSQL_XSN_NAME')




