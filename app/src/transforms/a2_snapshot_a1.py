import os
from src.utils.main import display_results, currentMillisecondsTime, generateCaseSegment
from src.modules.log_table_schema import log_table_name
import pandas as pd
import numpy as np

import time

def run(get_date, db, logs):
    startTime = currentMillisecondsTime()
    query = f"""
            SELECT full_date,game,department,COUNT(DISTINCT user_name ) 
            FROM
                (SELECT
                    D.full_date,
                    D.quarter,
                    D.day,
                    D.week,
                    D.month,
                    D.year,
                    G.game,
                    U.user_name,
                    E.department,
                    H.valid_bet_amount,
                    H.recharge_amount,
                    H.times,
                    H.scores
                FROM
                    user_history H
                    INNER JOIN dim_date D ON (H.date_id = D.id)
                    LEFT JOIN dim_game G ON (H.game_id = G.id)
                    LEFT JOIN dim_user U ON (H.user_id = U.id)
                    LEFT JOIN dim_department E ON (U.department_id = E.id)) a WHERE  full_date='{get_date}'
            GROUP BY full_date,game,department;
    """
    records = db.select_rows(query)
    dailyLogsDf = pd.DataFrame(records,columns=["full_date","game","department","metric_value"])


    # Insert results to Data warehouse
    action_name = "a2_snapshot_a1"

    
    dailyLogsDf["metric"] = action_name


    # Validate duplicate action name
    if action_name in logs["actions"]:
        display_results(["ERROR: Duplicate action name"])
        exit(1)
    db.clear_fact_daily(get_date,"fact_snapshot",action_name)
    totalInserted = db.load_fact_snapshot(df=dailyLogsDf)
    
    # Show info logs
    spendTime = currentMillisecondsTime() - startTime;
    transform = os.path.basename(__file__).split('.')
    display_results(["{name} inserted {numRows} with {seconds}s".format(name=transform[0],numRows=totalInserted, seconds=round(spendTime/1000,2))])

    logs["actions"].append(action_name)
    logs["execute_time"].append(spendTime)
    return logs