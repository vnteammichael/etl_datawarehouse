# type: ignore
"""Transform & Load from Log DB to Data Warehouse."""

from src.utils.main import display_results, date_range
from src.transforms import *
from src.dim_updates import *

def run(start_date, end_date, db):
    """Start run all transforms."""
    # Run more transforms here
    display_results(["Transforms starting..."]);

    for single_date in date_range(start_date, end_date):
        display_results(["=== Transform date {date} ===".format(date=single_date.strftime("%Y-%m-%d"))])

        logs = {
            'actions': [],
            'execute_time': []
        }
        get_date = single_date.strftime("%Y-%m-%d")
        # Clear data by date
        clear_data_by_date.run(get_date, db)


        # Start transforms fact_daily_measure
        logs = a2_snapshot_a1.run(get_date, db, logs)
        

        # Update log transform
        db.load_logs_transform(logs)
