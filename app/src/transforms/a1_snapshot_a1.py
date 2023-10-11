import os
from src.utils.main import display_results, currentMillisecondsTime, generateCaseSegment
from src.modules.log_table_schema import log_table_name
import pandas as pd
import numpy as np

import time

def run(get_date, db_clickhouse, db, logs):
    startTime = currentMillisecondsTime()
    query = f"""
            SELECT
                market, social, platform, '{get_date}' AS full_date,
                COUNT(DISTINCT user_id) AS metric_value
            FROM {log_table_name}
            WHERE
                toDate(log_time, 'UTC') = '{get_date}' AND
                log_action = 'LOGIN'
            GROUP BY market, social, platform
    """
    records = db_clickhouse.select_rows_dict(query)
    dailyLogsDf = pd.DataFrame(records)

    # Insert results to Data warehouse
    action_dict = {
        "name": "a1_snapshot_a1", # Required
        "description": "A.1	A1", # Required
        "metric_value": "COUNT(DISTINCT user_id)", # Required
        "metric_value_2": "",
        "log_name": "LOGIN",
        "dimension_1": "",
        "dimension_2": "",
        "dimension_3": "",
    }
    # Validate duplicate action name
    if action_dict["name"] in logs["actions"]:
        display_results(["ERROR: Duplicate action name"])
        exit(1)
    
    totalInserted = db.load_fact_snapshot(df=dailyLogsDf, db_clickhouse=db_clickhouse,action=action_dict)
    
    # Show info logs
    spendTime = currentMillisecondsTime() - startTime;
    transform = os.path.basename(__file__).split('.')
    display_results(["{name} inserted {numRows} with {seconds}s".format(name=transform[0],numRows=totalInserted, seconds=round(spendTime/1000,2))])

    logs["actions"].append(action_dict["name"])
    logs["execute_time"].append(spendTime)
    return logs