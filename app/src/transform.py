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

        if get_date=='2022-08-24':
            continue;

        # Start transforms fact_daily_measure
        # logs = m1_daily_measure.run(get_date, db_clickhouse, db, logs)
        # logs = m2_churn_measure.run(get_date, db_clickhouse, db, logs)
        # # logs = m3_payment_measure.run(get_date, db_clickhouse, db, logs)

        # Start transforms fact_snapshot
        # logs = a1_snapshot_a1.run(get_date, db_clickhouse, db, logs)
        # logs = a2_snapshot_n1.run(get_date, db_clickhouse, db, logs)
        # logs = b1_snapshot_average_game_per_user.run(get_date, db_clickhouse, db, logs)
        # logs = b2_snapshot_average_game_by_user_age.run(get_date, db_clickhouse, db, logs)
        # logs = b3_snapshot_game_count_by_user_age.run(get_date, db_clickhouse, db, logs)
        # logs = b4_snapshot_a1_dont_play.run(get_date, db_clickhouse, db, logs)
        # logs = b6_snapshot_n1_dont_play.run(get_date, db_clickhouse, db, logs)
        # logs = c1_snapshot_n1_do_tutorial.run(get_date,db_clickhouse,db, logs)

        # Update log transform
        db.load_logs_transform(logs)
