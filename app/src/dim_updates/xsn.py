# import os
# from src.utils.main import display_results, currentMillisecondsTime
# from src.utils.api import get_valid_bet_amount
# ""
# import pandas as pd
# import numpy as np
# from config import (
#     MYSQL_XSN_HOST, MYSQL_XSN_USERNAME, MYSQL_XSN_PASSWORD, MYSQL_XSN_PORT, MYSQL_XSN_NAME
# )

# from src.modules.db_mysql import MySQLConnector


# def run(get_date, db, logs):
#     """Update DIM: users_info"""
#     startTime = currentMillisecondsTime()
#     total_updated = 0
#     total_inserted = 0

#     # Insert results to Data warehouse
#     action_dict = {
#         "name": "Xo So nhanh VNTOP", # Required
#         "description": "DIM - update xsn", # Required
#     }
#     # Validate duplicate action name
#     if action_dict["name"] in logs["actions"]:
#         display_results(["ERROR: Duplicate action name"])
#         exit(1)

#     #### 1 - Query data ####
    
#     db_xsn = MySQLConnector(host=MYSQL_XSN_HOST, user=MYSQL_XSN_USERNAME, password=MYSQL_XSN_PASSWORD, port=MYSQL_XSN_PORT, database=MYSQL_XSN_NAME)
#     query = ("""

#         WITH Orders AS (
#             SELECT 
#                 bookmakerId,
#                 userId,
#                 paymentWin,
#                 revenue,
#                 childBetTypeName,
#                 betTypeName,
#                 turnIndex,
#                 seconds,
#                 type,
#                 created_at,
#                 updated_at
#             FROM orders
#             WHERE isTestPlayer = 0 AND  date(created_at)  = '{date}' and status = 'closed'
#         ),
        # Users AS (
        #     SELECT 
        #         id,
        #         name,
        #         usernameFromAgent
        #     FROM users
        # ),
        # Bookmakers AS (
        #     SELECT 
        #         id, 
        #         name
        #     FROM bookmaker
        # ),
#         OrderDetails AS (
#             SELECT
#                 u.usernameFromAgent,
#                 o.bookmakerId,
#                 u.name AS username,
#                 o.paymentWin,
#                 o.revenue,
#                 o.childBetTypeName,
#                 o.betTypeName,
#                 o.turnIndex,
#                 o.seconds,
#                 CASE
#                     WHEN o.type = 'xsmb' THEN 'Miền Bắc'
#                     WHEN o.type = 'xsmt' THEN 'Miền Trung'
#                     WHEN o.type = 'xsmn' THEN 'Miền Nam'
#                     WHEN o.type = 'xsspl' THEN 'Super Rich Lottery'
#                 END AS type,
#                 o.created_at,
#                 o.updated_at
#             FROM Orders o
#             JOIN Users u ON o.userId = u.id
#         )
#         SELECT 
#             od.usernameFromAgent,
#             b.name AS bookmakerName,
#             od.username,
#             od.paymentWin,
#             od.revenue,
#             CONCAT(od.betTypeName, ' ', od.childBetTypeName) AS typeBet,
#             CONCAT(od.type, ' ', od.seconds, ' giây') AS type,
#             od.turnIndex,
#             od.created_at,
#             od.updated_at
#         FROM OrderDetails od
#         JOIN Bookmakers b ON od.bookmakerId = b.id;

#     """.format( date=get_date))

#     df = None
#     try:
#         records = db_xsn.select_rows(query)
#         df = pd.DataFrame(records, columns =['usernameFromAgent','bookmakerName','username','paymentWin','revenue','typeBet','type','turnIndex','created_at','updated_at'])
#     except Exception as e:
#         print(str(e))
#     # Stop this task
#     if len(df)==0:
#         return logs

    
#     df["department_id"] = df['bookmakerName'].apply(lambda x : db.load_department(x))
#     dim_user = dim_user[[]] 
#     df = df[['usernameFromAgent','department_id','username','paymentWin','revenue','typeBet','type','turnIndex','created_at','updated_at']]



   
    
#     stats_user = df.groupby(['username','type','type','type']).agg({
#         "value":"sum",
#         "times":"count"
#     }).reset_index()

#     stats_user.rename(columns={"id": "user_id", "value": "scores"},inplace=True)
#     stats_user['game_id'] = stats_user['game'].apply(lambda x: db.load_game(x))
    





#     if len(stats_user.values)>0:
#         # records_data = df.to_records(index=False)
#         dim_user_history_table_insert = "INSERT IGNORE INTO user_history (date_id, user_id, game_id, valid_bet_amount, recharge_amount, redeem_amount, times, scores) VALUES"
#         total_inserted += db.load_data_bulk(part_query=dim_user_history_table_insert,
#                                         format_str='(%s, %s, %s, %s, %s, %s, %s, %s)',
#                                         dataframe=stats_user,
#                                         on_conflict = "")  
 
#     # Show info logs
#     spend_time = currentMillisecondsTime() - startTime
#     transform = os.path.basename(__file__).split('.')
#     display_results(["{name} updated {numRows} with {seconds}s".format(name=transform[0],numRows=total_inserted, seconds=round(spend_time/1000,2))])

#     logs["actions"].append(action_dict["name"])
#     logs["execute_time"].append(spend_time)
#     return logs
