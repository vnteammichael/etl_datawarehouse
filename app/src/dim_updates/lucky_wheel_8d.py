import os
from src.utils.main import display_results, currentMillisecondsTime
from psycopg2 import sql
import pandas as pd
import numpy as np


def run(get_date, db, logs):
    """Update DIM: users_info"""
    startTime = currentMillisecondsTime()
    total_updated = 0
    total_inserted = 0

    # Insert results to Data warehouse
    action_dict = {
        "name": "users_info", # Required
        "description": "DIM - update info", # Required
    }
    # Validate duplicate action name
    if action_dict["name"] in logs["actions"]:
        display_results(["ERROR: Duplicate action name"])
        exit(1)

    #### 1 - query data ####
    query = ("""

        SELECT username, '{department}' AS department, '{game}' AS game FROM {table} WHERE date(updatedAt) = '{date}'  LIMIT 20;

    """.format(department='8b',game='luck_wheel',table="users_1", date=get_date))

    df = None
    try:
        records = db.select_rows(query)
        df = pd.DataFrame(records, columns =['user_name','department','game'])
    except Exception as e:
        print(str(e))
    # Stop this task
    if len(df)==0:
        return logs


    # Clear data
    # df = df.convert_dtypes()
    
    df["department_id"] = df['department'].apply(lambda x : db.load_department(x)) 
    df = df[["user_name","department_id"]]

    
    if len(df.values)>0:
        # records_data = df.to_records(index=False)
        dim_user_table_insert = "INSERT IGNORE INTO dim_user (user_name,department_id) VALUES"
        total_inserted = db.load_data_bulk(part_query=dim_user_table_insert,
                                        format_str='(%s, %s)',
                                        dataframe=df)

        
        
 
    # Show info logs
    spend_time = currentMillisecondsTime() - startTime
    transform = os.path.basename(__file__).split('.')
    display_results(["{name} updated {numRows} with {seconds}s".format(name=transform[0],numRows=total_inserted, seconds=round(spend_time/1000,2))])

    logs["actions"].append(action_dict["name"])
    logs["execute_time"].append(spend_time)
    return logs