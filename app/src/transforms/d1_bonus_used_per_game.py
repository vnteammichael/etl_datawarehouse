import os
from src.utils.main import display_results, currentMillisecondsTime, generateCaseSegment
import pandas as pd
import numpy as np

import time

def run(get_date, db_xsn, db, logs):
    startTime = currentMillisecondsTime()
    query = ("""

        WITH Orders AS (
            SELECT 
                bookMakerId,
                userId,
                paymentWin,
                revenue,
                seconds,
                type
            FROM orders
            WHERE isTestPlayer = 0 AND  date(created_at)  = '{date}'  and status = 'closed'
        ),
        Bookmakers AS (
            SELECT 
                id, 
                name
            FROM bookmaker
        ),
        OrderDetails AS (
            SELECT
                o.bookMakerId,
                o.paymentWin,
                o.revenue,
                o.seconds,
                CASE
                    WHEN o.type = 'xsmb' THEN 'Miền Bắc'
                    WHEN o.type = 'xsmt' THEN 'Miền Trung'
                    WHEN o.type = 'xsmn' THEN 'Miền Nam'
                    WHEN o.type = 'xsspl' THEN 'Super Rich Lottery'
                END AS type
            FROM Orders o
        )
        SELECT
            b.name AS department,
            CONCAT(od.type, ' ', od.seconds, ' giây') AS dimension_1,
            sum(od.revenue) as revenue , sum(od.revenue  + od.paymentWin) as win,
            '{date}' AS full_date
        FROM OrderDetails od
        JOIN Bookmakers b ON od.bookMakerId = b.id
        group by department,dimension_1;

    """.format( date=get_date))

    records = db_xsn.select_rows(query)
    dailyLogsDf = pd.DataFrame(records, columns =['department','dimension_1','revenue','win','full_date'])

    dailyLogsDf["bonus"] = dailyLogsDf['win'] - dailyLogsDf['revenue']

    dailyLogsDf['metric_value'] = dailyLogsDf['bonus'].apply(lambda x: x if x>0 else 0)

    dailyLogsDf = dailyLogsDf.groupby(['department','dimension_1','full_date']).agg({
        'metric_value':'sum'
    }).reset_index()

    # Insert results to Data warehouse
    action_dict = {
        "name": "d1_bonus_used_per_game", # Required
        "description": "D.1	Bonus được dùng từng game", # Required
        "metric_value": "", # Required
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