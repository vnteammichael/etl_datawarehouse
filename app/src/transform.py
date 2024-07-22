# type: ignore
"""Transform & Load from Log DB to Data Warehouse."""

from src.utils.main import display_results, date_range
from src.transforms import *
from src.dim_updates import *

def run(start_date, end_date, db_xsn, db):
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
        logs = a1_user_login.run(get_date, db_xsn, db, logs)
        logs = a2_new_user.run(get_date, db_xsn, db, logs)

        logs = b1_user_play_by_game.run(get_date, db_xsn, db, logs)
        logs = b2_snapshot_a1_dont_play.run(get_date, db_xsn, db, logs)
        logs = b3_snapshot_n1_dont_play.run(get_date, db_xsn, db, logs)
        logs = b4_user_play_trial.run(get_date, db_xsn, db, logs)
        
        logs = c1_total_bet_per_game.run(get_date, db_xsn, db, logs)
        logs = c2_user_win_per_game.run(get_date, db_xsn, db, logs)
        logs = c3_revenue_per_game.run(get_date, db_xsn, db, logs)

        logs = d1_bonus_used_per_game.run(get_date, db_xsn, db, logs)

        logs = m1_churn_measure.run(get_date, db_xsn, db, logs)

        

        # Update log transform
        db.load_logs_transform(logs)
