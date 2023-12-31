import os
from src.utils.main import display_results, currentMillisecondsTime
from src.utils.api import get_valid_bet_amount
from psycopg2 import sql
import pandas as pd
import numpy as np
from config import (
    LUCKY_WHEEL_8D_HOST, LUCKY_WHEEL_8D_USERNAME, LUCKY_WHEEL_8D_PASSWORD, LUCKY_WHEEL_8D_PORT, LUCKY_WHEEL_8D_NAME,
    DTC_8D_API_URL,SIGN_KEY_DTC_8D
)

from src.modules.db_mysql import MySQLConnector


def run(get_date, db, logs):
    """Update DIM: lucky wheel 8d"""
    startTime = currentMillisecondsTime()
    total_updated = 0
    total_inserted = 0

    # Insert results to Data warehouse
    action_dict = {
        "name": "lucky wheel 8d", # Required
        "description": "DIM - lucky wheel 8d ", # Required
    }
    # Validate duplicate action name
    if action_dict["name"] in logs["actions"]:
        display_results(["ERROR: Duplicate action name"])
        exit(1)

    #### 1 - Query data ####
    
    db_8d = MySQLConnector(host=LUCKY_WHEEL_8D_HOST, user=LUCKY_WHEEL_8D_USERNAME, password=LUCKY_WHEEL_8D_PASSWORD, port=LUCKY_WHEEL_8D_PORT, database=LUCKY_WHEEL_8D_NAME)
    query = ("""

        SELECT username, '{department}' AS department FROM {table};

    """.format(department='8d',table="users", date=get_date))

    df = None
    try:
        records = db_8d.select_rows(query)
        df = pd.DataFrame(records, columns =['user_name','department'])
    except Exception as e:
        print(str(e))
    # Stop this task
    if len(df)==0:
        return logs

    
    df["department_id"] = df['department'].apply(lambda x : db.load_department(x)) 
    df = df[["user_name","department_id"]]



    
    if len(df.values)>0:
        # records_data = df.to_records(index=False)
        dim_user_table_insert = "INSERT IGNORE INTO dim_user (user_name,department_id) VALUES"
        total_inserted = db.load_data_bulk(part_query=dim_user_table_insert,
                                        format_str='(%s, %s)',
                                        dataframe=df,
                                        on_conflict = "")


    #Get hisory data play
    query = ("""
                with users as (select id, username from users),
                    history_daily as (select userId, money
                                    from collections
                                    where date(createdAt)  = '{date}' and isDeleted = 0 and typeName = 'received')
                SELECT id, username, '{game}' AS game, money
                FROM history_daily
                        INNER JOIN users ON users.id = history_daily.userId;


    """.format(game='lucky_wheel', date=get_date))

    try:
        records = db_8d.select_rows(query)
        df = pd.DataFrame(records, columns =['id','user_name','game','value'])
    except Exception as e:
        print(str(e))
    # Stop this task
    if len(df)==0:
        return logs
    
    df["times"] = df['user_name'].apply(lambda x:x)
    stats_user = df.groupby(['id','user_name','game']).agg({
        "value":"sum",
        "times":"count"
    }).reset_index()


    stats_user.rename(columns={"id": "user_id", "value": "scores"},inplace=True)
    stats_user['game_id'] = stats_user['game'].apply(lambda x: db.load_game(x))
    
    stats_user["temp"] = stats_user["user_name"].apply(lambda x: get_valid_bet_amount(x,get_date,get_date,DTC_8D_API_URL,SIGN_KEY_DTC_8D))
    stats_user = stats_user[stats_user["temp"].str.len() == 2]
    stats_user["recharge_amount"] = stats_user["temp"].apply(lambda x: x[0])
    stats_user["valid_bet_amount"] = stats_user["temp"].apply(lambda x: x[1])
    stats_user["date"] = get_date

    stats_user["date_id"] = stats_user['date'].apply(lambda x: db.load_date(x))
    
    stats_user = stats_user[["date_id","user_name","game_id","valid_bet_amount","recharge_amount","times","scores"]]
    
    dep_id = db.load_department("8d")
    
    query = """
        SELECT id, user_name FROM dim_user where department_id = {dep_id};
    """.format(dep_id=dep_id)

    try:
        records = db.select_rows(query)
        dim_user = pd.DataFrame(records, columns =['id','user_name'])
    except Exception as e:
        print(str(e))

    query = ("""
                with users as (select id, username from users ),
                    history_daily as (select userId, money
                                    from collections
                                    where date(createdAt)  = '{date}' and isDeleted = 0 and typeName = 'redeem')
                SELECT username, '{game}' AS game, money
                FROM history_daily
                        INNER JOIN users ON history_daily.userId = users.id;


    """.format(game='lucky_wheel', date=get_date))

    try:
        records = db_8d.select_rows(query)
        df_redeem = pd.DataFrame(records, columns =['user_name','game','value'])
    except Exception as e:
        print(str(e))
    # Stop this task
    if len(df_redeem)==0:
        return logs
    
    df_redeem["times"] = df_redeem['user_name'].apply(lambda x:x)
    stats_user_redeem = df_redeem.groupby(['user_name']).agg({
        "value":"sum"
    }).reset_index()
    stats_user_redeem.rename(columns={"value":"redeem_amount"},inplace=True)

    stats_user = stats_user.merge(dim_user,how="inner",on="user_name")
    stats_user = stats_user.merge(stats_user_redeem,how="inner",on="user_name")
    
    stats_user.rename(columns={"id": "user_id"},inplace=True)
    stats_user = stats_user[["date_id","user_id","game_id","valid_bet_amount","recharge_amount","redeem_amount","times","scores"]]


    if len(stats_user.values)>0:
        # records_data = df.to_records(index=False)
        dim_user_history_table_insert = "INSERT IGNORE INTO user_history (date_id, user_id, game_id, valid_bet_amount, recharge_amount, redeem_amount, times, scores) VALUES"
        total_inserted += db.load_data_bulk(part_query=dim_user_history_table_insert,
                                        format_str='(%s, %s, %s, %s, %s, %s, %s, %s)',
                                        dataframe=stats_user,
                                        on_conflict = "")
        
    
    fact_awards_stat = df.groupby(["value"]).agg({
        "id":"count"
    }).reset_index()
    fact_awards_stat.rename(columns={"id":"metric_value","value":"dimension_1"},inplace=True)
    total_times = df['id'].count()
    fact_awards_stat["metric_value_2"] = total_times
    fact_awards_stat["full_date"] = get_date
    fact_awards_stat["game"] = "lucky_wheel"
    fact_awards_stat["department"] = "8d"
    fact_awards_stat["metric"] = "a1_fact_awards_stat"

    fact_awards_stat = fact_awards_stat[["full_date","game","department","metric","metric_value","metric_value_2","dimension_1"]]

    if len(fact_awards_stat.values)>0:
        # records_data = df.to_records(index=False)
        db.load_fact_snapshot(df = fact_awards_stat)
   
 
    # Show info logs
    spend_time = currentMillisecondsTime() - startTime
    transform = os.path.basename(__file__).split('.')
    display_results(["{name} updated {numRows} with {seconds}s".format(name=transform[0],numRows=total_inserted, seconds=round(spend_time/1000,2))])

    logs["actions"].append(action_dict["name"])
    logs["execute_time"].append(spend_time)
    return logs
