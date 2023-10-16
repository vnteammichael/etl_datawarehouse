"""Script entry point."""
import sys
sys.path.append(r'src')

from src import transform
from src import transform_dim_update

def init_script(runner, start_date, end_date, db):
    """Transform and Load to Data Warehouse."""

    if runner=='daily':
        transform.run(start_date, end_date, db)
    elif runner=='dim_update':
        transform_dim_update.run(start_date, end_date, db)
    else:
        print('Wrong option')
        exit(1)