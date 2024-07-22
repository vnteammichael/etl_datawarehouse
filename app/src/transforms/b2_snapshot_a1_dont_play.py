import os
from src.utils.main import display_results, currentMillisecondsTime, generateCaseSegment
from src.modules.log_table_schema import log_table_name
import pandas as pd
import numpy as np

import time

def run(get_date, db_xsn, db, logs):
    startTime = currentMillisecondsTime()

    query = """
            WITH Users AS (
                SELECT 
                    u.id,
                    b.name as department
                FROM users u
                JOIN bookmaker b ON u.bookmakerId = b.id
            ),
            UserLoginNotPlay AS (
                SELECT
                    distinct userId
                FROM wallet_history
                WHERE
                    date(createdAt) = '{get_date}' and typeTransaction = 'Chuyển tiền' and subOrAdd = 1 and userId not in (
                        SELECT
                            distinct userId
                        FROM wallet_history
                        WHERE
                            date(createdAt) = '{get_date}' and typeTransaction = 'Chơi game'
                    )
            )
            SELECT
                department, '{get_date}' AS full_date,
                COUNT(DISTINCT u.id) AS metric_value
            FROM Users u
            JOIN UserLoginNotPlay ul ON u.id = ul.userId
            GROUP BY department
    """.format(get_date=get_date)

    records = db_xsn.select_rows(query)
    dailyLogsDf = pd.DataFrame(records, columns =['department','full_date','metric_value'])

    # Insert results to Data warehouse
    action_dict = {
        "name": "b2_snapshot_a1_dont_play", # Required
        "description": "B.2 A1 count that don't play", # Required
        "metric_value": "COUNT(DISTINCT user_id)", # Required
        "metric_value_2": "",
        "dimension_1": "",
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