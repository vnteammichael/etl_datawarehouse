from src.utils.main import display_results
import pandas as pd
import datetime

def run(get_date, db):
    """Transform: Clear data by Date."""
    transform_name = "clear_data_by_date"; # Note: change name of Transform

    # Load Date
    date_id = db.load_date(get_date)

    db.clear_fact_daily(date_id, "fact_snapshot")
    # db.clear_fact_daily(date_id, "fact_payment")


    # Show info logs
    logs = []
    logs.append(transform_name + " " + get_date + " " + 'done');
    display_results(logs);
