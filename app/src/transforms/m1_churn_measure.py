import os
from src.utils.main import display_results, currentMillisecondsTime
from src.modules.log_table_schema import log_table_name
from psycopg2 import sql
import datetime

import pandas as pd
import numpy as np
from src.segments.general import level_list


def run(get_date, db_xsn, db, logs):
    """Transform: m1_churn_measure"""
    startTime = currentMillisecondsTime()

    # Insert results to Data warehouse
    action_dict = {
        "name": "m1_churn_measure", # Required
        "description": "Churn measure", # Required
    }
    # Validate duplicate action name
    if action_dict["name"] in logs["actions"]:
        display_results(["ERROR: Duplicate action name"])
        exit(1)

    #### 1 - churn_3 ####
    df_churn_1 = cal_churn(get_date, 1, db_xsn, db);
    df_churn_3 = cal_churn(get_date, 3, db_xsn, db);
    df_churn_7 = cal_churn(get_date, 7, db_xsn, db);
    df_churn_15 = cal_churn(get_date, 15, db_xsn, db);

    # Merge dataframes
    frames = [df_churn_1, df_churn_3, df_churn_7,df_churn_15]
    df = pd.concat(frames)
    

    # Fix data
    df.fillna(0, inplace=True)
    print(df.head())
    df = db.load_dim("department", "department_id", "department", "dim_department", df)
    
    # Stop this task
    if len(df)==0:
        return logs

    # Define fields of fact table
    fact_df = df[['date_id','department_id','churn_type','churn_users','total_daily_active_users']]
    fact_data_list = fact_df.values.tolist()

    queryText = "INSERT INTO fact_churn_measure (date_id, department_id, churn_type, churn_users, total_daily_active_users) VALUES %s"
    total_inserted = db.load_data_batch(queryText, fact_data_list,'fact_churn_measure')

    # Show info logs
    spend_time = currentMillisecondsTime() - startTime;
    transform = os.path.basename(__file__).split('.')
    display_results(["{name} inserted {numRows} with {seconds}s".format(name=transform[0],numRows=total_inserted, seconds=round(spend_time/1000,2))])

    logs["actions"].append(action_dict["name"])
    logs["execute_time"].append(spend_time)
    return logs

def cal_churn(get_date, churnDay, db_xsn, db):
    df = None
    query = """
        WITH DailyLog AS (
                    SELECT
                        DISTINCT  userId
                    FROM wallet_history
                    WHERE
                        date(createdAt) = DATE_SUB('{get_date}',INTERVAL {churnDay} DAY) AND typeTransaction = 'Chuyển tiền' and subOrAdd = 1
                ),
        LastUserAction AS (SELECT DailyLog.*
                FROM DailyLog
                WHERE userId NOT IN (
                    SELECT
                            DISTINCT userId
                    FROM wallet_history
                    WHERE
                        date(createdAt) BETWEEN DATE_SUB('{get_date}',INTERVAL {churnDay} - 1 DAY) AND '{get_date}' AND typeTransaction = 'Chuyển tiền' and subOrAdd = 1
                )
        ),
        Users AS (
                SELECT
                    u.id,
                    b.name as department
                FROM users u
                JOIN bookmaker b ON u.bookmakerId = b.id
        )
        SELECT
                department,
                COUNT(DISTINCT u.id) AS churn_users
        FROM Users u
        JOIN LastUserAction dl ON u.id = dl.userId
        GROUP BY department
    """.format(get_date=get_date,churnDay=churnDay)


    records = db_xsn.select_rows(query)
    
    df1 = pd.DataFrame(records, columns =['department', 'churn_users'])
    
    churn_date_obj = datetime.datetime.strptime(get_date, '%Y-%m-%d') - datetime.timedelta(days=churnDay+1)

    df2 = cal_daily_active_users(churn_date_obj.strftime("%Y-%m-%d"), db_xsn, db)
    # Merge dataframes
    df = pd.merge(df1, df2, how="outer", on=['department'])
    df['churn_type'] = 'churn_'+str(churnDay)
    df["date_id"] = db.load_date(churn_date_obj.strftime("%Y-%m-%d"))

    return df


def cal_daily_active_users(get_date, db_xsn, db):
    # Because social got empty, so we find in user_name to get social
    df = None
    query = """
            WITH Users AS (
                SELECT 
                    u.id,
                    b.name as department
                FROM users u
                JOIN bookmaker b ON u.bookmakerId = b.id
            ),
            UserLogin AS (
                SELECT
                    DISTINCT userId
                FROM wallet_history
                WHERE
                    date(createdAt) = '{get_date}' and typeTransaction = 'Chuyển tiền' and subOrAdd = 1
            )
            SELECT
                department,
                COUNT(DISTINCT u.id) AS metric_value
            FROM Users u
            JOIN UserLogin wh ON u.id = wh.userId
            GROUP BY department
    """.format(get_date=get_date)

    records = db_xsn.select_rows(query)
    df = pd.DataFrame(records, columns =['department', 'total_daily_active_users'])

    df["date_id"] = db.load_date(get_date)
    return df
