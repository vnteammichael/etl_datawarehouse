import os
from src.utils.main import display_results, currentMillisecondsTime, generateCaseSegment
import pandas as pd
import numpy as np

import time

def run(get_date, db_xsn, db, logs):
    startTime = currentMillisecondsTime()
    query = """
            WITH Users AS (
                SELECT department_id, game_id,
                    SUM(bet_amount - win_amount) AS metric_value
                FROM dim_user
                WHERE
                    date = '{get_date}' 
                GROUP BY department_id, game_id
            )
            SELECT department_id, '{get_date}' AS full_date,
                metric_value, game as dimension_1
            FROM Users u
            JOIN dim_game g ON u.game_id = g.id
            GROUP BY department_id,dimension_1
            
    """.format(get_date=get_date)
    records = db.select_rows(query)
    dailyLogsDf = pd.DataFrame(records, columns =['department_id','full_date','metric_value','dimension_1'])

    # Insert results to Data warehouse
    action_dict = {
        "name": "c3_revenue_per_game", # Required
        "description": "C.3	Tiền lợi nhuận từng game", # Required
        "metric_value": "SUM(bet_amount - win_amount)", # Required
        "metric_value_2": "",
        "dimension_1": "game",
        "dimension_2": "",
        "dimension_3": "",
    }
    # Validate duplicate action name
    if action_dict["name"] in logs["actions"]:
        display_results(["ERROR: Duplicate action name"])
        exit(1)
    
    dailyLogsDf["metric"] = action_dict["name"]
    
    totalInserted = db.load_fact_snapshot(df=dailyLogsDf)
    
    # Show info logs
    spendTime = currentMillisecondsTime() - startTime
    transform = os.path.basename(__file__).split('.')
    display_results(["{name} inserted {numRows} with {seconds}s".format(name=transform[0],numRows=totalInserted, seconds=round(spendTime/1000,2))])

    logs["actions"].append(action_dict["name"])
    logs["execute_time"].append(spendTime)
    return logs