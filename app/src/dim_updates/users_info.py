import os
from src.utils.main import display_results, currentMillisecondsTime
from psycopg2 import sql
import pandas as pd
import numpy as np
from src.segments.general import level_list, gold_list


def run(get_date, db_xsn, db, logs):
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

    #### 1 - Update Info ####
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
            WHERE isTestPlayer = 0 AND  date(created_at)  = '{date}' and status = 'closed'
        ),
        Users AS (
            SELECT 
                id,
                username
            FROM users
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
                u.username AS username,
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
            JOIN Users u ON o.userId = u.id
        )
        SELECT 
            od.username,
            b.name AS bookmakerName,
            CONCAT(od.type, ' ', od.seconds, ' giây') AS game,
            od.paymentWin,
            od.revenue,
            '{date}' AS date
        FROM OrderDetails od
        JOIN Bookmakers b ON od.bookMakerId = b.id;

    """.format( date=get_date))

    df = None
    
    try:
        records = db_xsn.select_rows(query)
        df = pd.DataFrame(records, columns =['username','department','game','paymentWin','revenue','date'])
    except Exception as e:
        print(str(e))

    # Stop this task
    if len(df)==0:
        return logs


    # Clear data
    df = df.convert_dtypes()
    df["department_id"] = df['department'].apply(lambda x : db.load_department(x))
    df["win_amount"] = df["revenue"] + df["paymentWin"]
    df["game_id"] = df['game'].apply(lambda x : db.load_game(x))
    df["win_match"] = df["win_amount"].apply(lambda x : 1 if x>0 else 0)
    df["total_match"] = df["username"]

    stats = df.groupby(['date','username','department_id','game_id']).agg({
        "win_amount":"sum",
        "revenue":"sum",
        "win_match":"sum",
        "total_match":"count"
    }).reset_index()
    stats.rename(columns={"revenue": "bet_amount"},inplace=True)
    stats = stats[['date','username','department_id','game_id','bet_amount','win_amount','win_match','total_match']]
    stats["win_amount"] = stats["win_amount"].astype(int)
    stats["bet_amount"] = stats["bet_amount"].astype(int)
    
    if len(stats.values)>0:
        dim_user_table_insert = "INSERT INTO dim_user (date, username, department_id, game_id, bet_amount, win_amount, win_match, total_match ) VALUES"
        total_inserted = db.load_data_bulk(part_query=dim_user_table_insert,
                                        format_str='(%s, %s, %s, %s, %s, %s, %s, %s)',
                                        dataframe=stats,
                                        on_conflict='ON DUPLICATE KEY UPDATE bet_amount=bet_amount,win_amount=win_amount,win_match=win_match,total_match=total_match')

        
        
 
    # Show info logs
    spend_time = currentMillisecondsTime() - startTime
    transform = os.path.basename(__file__).split('.')
    display_results(["{name} updated {numRows} with {seconds}s".format(name=transform[0],numRows=total_inserted, seconds=round(spend_time/1000,2))])

    logs["actions"].append(action_dict["name"])
    logs["execute_time"].append(spend_time)
    return logs