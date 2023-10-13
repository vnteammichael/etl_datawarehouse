"""Database client"""
from ..utils.log import LOGGER
# import psycopg2
# from psycopg2.extras import DictCursor, execute_values
# from psycopg2 import sql
import mysql.connector
from .sql_queries import *
import pandas as pd
import numpy as np

class MySQLConnector:
    """Mysql Database class"""

    
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cache = {}

    def connect(self):
        if self.conn is None:
            try:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
            except Exception as e:
                LOGGER.error(e)
                raise e
    

    def select_rows(self, query):
        records = []
        """Run a SQL query to select rows from a table."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                records = cur.fetchall()
        except mysql.connector.Error as e:
            LOGGER.error(e)
            raise e
        finally:
            if cur:
                cur.close()
        return records

    def update_rows(self, query):
        """Run a SQL query to update rows in table"""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()
            cur.close()
            return f"{cur.rowcount}"

    def load_data(self, query, data):
        """Load data to warehouse"""
        self.connect()
        with self.conn.cursor() as cur:
            cur.executemany(query, data)
            self.conn.commit()
            cur.close()
            return cur.rowcount
        
    def load_data_batch(self, queryText, data, tableName, batch_size=50000):
        """Load data to warehouse"""
        self.connect()
        try:

            with self.conn.cursor() as cur:
                # Create a placeholder for the INSERT statement
                placeholders = ', '.join(['(%s, %s, %s)' for _ in range(len(data[0]))])
                # Construct the INSERT statement with placeholders
                query = f"{queryText} VALUES {placeholders}"

                # Split the data into batches
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    # Execute the INSERT statement for the batch
                    cur.executemany(query, batch)
                    self.conn.commit()
                
                
                return len(data)

        except mysql.connector.Error as e:
            LOGGER.error(e)
            raise e
        finally:
            if cur:
                cur.close()

    def load_data_bulk(self, part_query, format_str, dataframe):
        """Load data to warehouse"""
        self.connect()
        with self.conn.cursor() as cur:
            data = dataframe.values.tolist()
            # Define the INSERT query
            insert_query = f"{part_query} {format_str} "


            # Execute the INSERT query with executemany
            cur.executemany(insert_query, data)
            self.conn.commit()
            cur.close()
            return cur.rowcount
    
    def adjust_query_to_format(self, query):
        """Fit query to required format"""

        format_query = """
            SELECT full_date, platformId, socialType, market, metric_value, dimension_1, dimension_2, dimension_3
            FROM ({select_sql}) src
        """.format(select_sql=query)

        return format_query


    # Custom method to load data to warehouse
    # Inputs:
    #   - db_conn_obj
    #   - sql query (with specific format)
    #   - or dataFrame
    #   - metric being query (or action)
    # The query must return data with correct format (columns' name must be accurate):
    #     + full_date (date)
    #     + metric_value (float)
    #     + dimension_1 (string)
    #     + dimension_2 (string)
    #     + dimension_3 (string)
    # Columns of values and contexts that are not used must also be include in result (can return NULL or empty string)
    def load_fact_snapshot(self, db_clickhouse, action, sql_query = "", df = None):

        if sql_query != "":
            df = self.load_df_data_from_sql(db_clickhouse, sql_query)

        if df is None:
            print('Missing DataFrame')
            exit(1)

        try:
            metric_name = action.get("name", "")
            if metric_name == "":
                raise Exception("Missing action name")
            metric_description = action.get("description", "")
            if metric_description == "":
                raise Exception("Missing action description")

            metric_id = self.load_action(action)
        except Exception as e:
            LOGGER.warning("Passing Action/Metric variables must include it's description. (i.e pass a dictionary with \{'name': name; 'description': description\} format)")
            LOGGER.error(str(e))
            exit(1)


        fact_snapshot = []
        for _, row in df.iterrows():
            game_id = dimension_1_id = dimension_2_id = dimension_3_id = None

            if 'metric_value_2' not in row:
                row['metric_value_2'] = 0

            try:
                date_id = self.load_date(row['full_date'])
                department_id = self.load_department(row['department'])
                game_id = self.load_game(row['game'])
                dimension_1_id = self.load_context(row['dimension_1'], 1)
                dimension_2_id = self.load_context(row['dimension_2'], 2)
                dimension_3_id = self.load_context(row['dimension_3'], 3)
            except KeyError:
                pass
            except Exception as e:
                LOGGER.error(str(e))
                exit(1)

            row_added = [date_id, game_id, department_id, metric_id, dimension_1_id, dimension_2_id, dimension_3_id, row['metric_value'], row['metric_value_2']]

            fact_snapshot.append(tuple(row_added))

        query = 'INSERT INTO fact_snapshot (date_id, game_id, department_id, metric_id, dimension_1_id, dimension_2_id, dimension_3_id, metric_value, metric_value_2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.executemany(query, fact_snapshot)
                self.conn.commit()
                cur.close()
                return f"{cur.rowcount} rows"
        except(Exception, mysql.connector.Error) as error:
            print(error)

    # def load_df_data_from_sql(self, db_clickhouse, sql_query):
    #     format_query = self.adjust_query_to_format(sql_query)
    #     df = None
    #     try:
    #         records = db_clickhouse.select_rows(format_query)
    #         df = pd.DataFrame(records, columns =['full_date', 'market_id', 'social_id', 'metric_value', 'dimension_1','dimension_2','dimension_3'])
    #     except Exception as e:
    #         LOGGER.warning("The query must return data with correct format (full_date, market_id, platform_id, social_id, metric_value, ..., dimension_3")
    #         LOGGER.error(str(e))

    #     return df


    def load_logs_transform(self, logs):
        """
        It takes a dictionary of lists, and inserts the data into a Postgres table

        :param logs: a dictionary with two keys: actions and execute_time
        :return: The number of rows inserted.
        """
        if len(logs["actions"])==0:
            return

        # Clear logs
        query = "DELETE FROM log_transform_daily";
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()
            cur.close()

        record_data = []
        i = 0;
        for n in logs["actions"]:
            row_added = [n, logs["execute_time"][i]]
            record_data.append(tuple(row_added))
            i = i+1

        query = 'INSERT INTO log_transform_daily (transform_name, execute_time) VALUES (%s, %s)'
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.executemany(query, record_data)
                self.conn.commit()
                cur.close()
                return f"{cur.rowcount}"
        except(Exception, mysql.connector.Error) as error:
            print(error)

    def clear_fact_daily(self, date_id, table_name):
        """Clear fact_daily_measure at warehouse"""

        query = "DELETE FROM " + table_name + " WHERE date_id = " + str(date_id);
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()
            cur.close()
            return f"{cur.rowcount}"
    def clear_fact_churn_measure(self, date_id, churn_type):
        """Clear fact_churn_measure at warehouse"""

        query = "DELETE FROM fact_churn_measure WHERE date_id = " + str(date_id) + " AND churn_type='"+ churn_type +"'";
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()
            cur.close()
            return f"{cur.rowcount}"
    # def clear_fact_payment(self, date_id):
    #     """Clear fact_payment at warehouse"""

    #     query = "DELETE FROM fact_payment WHERE date_id = " + str(date_id);
    #     self.connect()
    #     with self.conn.cursor() as cur:
    #         cur.execute(query)
    #         self.conn.commit()
    #         cur.close()
    #         return f"{cur.rowcount}"


    def load_action(self, action):
        """
        It takes a dictionary of action data, and if the action is already in the
        database, it updates the description, else it inserts the action into the
        database and returns the id of the action

        :param action: a dictionary containing the following keys:
        :return: The id of the action.
        """
        if action["name"] is None:
            return None
        metric_name = str(action["name"])
        description = str(action["description"])
        metric_value = action.get("metric_value", "")
        metric_value_2 = action.get("metric_value_2", "")
        dimension_1 = action.get("dimension_1", "")
        dimension_2 = action.get("dimension_2", "")
        dimension_3 = action.get("dimension_3", "")
        
        # return id if action already in dim_metric table (and update its description)
        # else insert new action to dim_metric table and return id
        self.connect()
        query = ("""
            SELECT id FROM dim_metric WHERE metric_name=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (metric_name,))
            row = cur.fetchone()
            if row:
                id = row[0]
                cur.execute(dim_metric_table_update_description, (description, metric_value, metric_value_2, dimension_1, dimension_2, dimension_3, id, ))
                self.conn.commit()
            else:
                cur.execute(dim_metric_table_insert, (metric_name, description, metric_value, metric_value_2, dimension_1, dimension_2, dimension_3, ))
                self.conn.commit()
                id = cur.fetchone()[0]
        cur.close()
        return id

    def load_group(self, group_name):
        """
        It takes a group name, checks if it exists in the database, if it does, it
        returns the id, if it doesn't, it inserts it and returns the id

        :param group_name: The name of the group to load
        :return: The id of the group.
        """
        if group_name is None:
            return None
        group_name = str(group_name)

        self.connect()
        # cur = self.conn.cursor()
        query = ("""
            SELECT id FROM dim_group WHERE group_name=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (group_name,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                cur.execute(dim_group_table_insert, (group_name,))
                self.conn.commit()
                id = cur.fetchone()[0]
        cur.close()
        return id

    def load_utm_source(self, utm_source):
        if utm_source is None:
            return None
        utm_source = str(utm_source)

        # Cache
        if (self.cache.get('utm_source') != None and self.cache.get('utm_source').get(utm_source) != None):
            return self.cache.get('utm_source').get(utm_source)


        self.connect()
        query = ("""
            SELECT id FROM dim_utm_source WHERE utm_source=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (utm_source,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                cur.execute(dim_utm_source_table_insert, (utm_source,))
                self.conn.commit()
                id = cur.fetchone()[0]
        cur.close()


        if self.cache.get('utm_source')==None:
            self.cache.update({"utm_source": {}})
        self.cache['utm_source'][utm_source] = id
        return id

    def load_utm_medium(self, utm_medium):
        if utm_medium is None:
            return None
        utm_medium = str(utm_medium)

        # Cache
        if (self.cache.get('utm_medium') != None and self.cache.get('utm_medium').get(utm_medium) != None):
            return self.cache.get('utm_medium').get(utm_medium)


        self.connect()
        query = ("""
            SELECT id FROM dim_utm_medium WHERE utm_medium=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (utm_medium,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                cur.execute(dim_utm_medium_table_insert, (utm_medium,))
                self.conn.commit()
                id = cur.fetchone()[0]
        cur.close()


        if self.cache.get('utm_medium')==None:
            self.cache.update({"utm_medium": {}})
        self.cache['utm_medium'][utm_medium] = id
        return id

    def load_utm_campaign(self, utm_campaign):
        if utm_campaign is None:
            return None
        utm_campaign = str(utm_campaign)

        # Cache
        if (self.cache.get('utm_campaign') != None and self.cache.get('utm_campaign').get(utm_campaign) != None):
            return self.cache.get('utm_campaign').get(utm_campaign)


        self.connect()
        query = ("""
            SELECT id FROM dim_utm_campaign WHERE utm_campaign=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (utm_campaign,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                cur.execute(dim_utm_campaign_table_insert, (utm_campaign,))
                self.conn.commit()
                id = cur.fetchone()[0]
        cur.close()


        if self.cache.get('utm_campaign')==None:
            self.cache.update({"utm_campaign": {}})
        self.cache['utm_campaign'][utm_campaign] = id
        return id

    def load_game(self, game_name):
        """
        It checks if the game name is in the cache, if not it checks if it's in
        the database, if not it inserts it into the database and returns the id

        :param gsme_name: The name of the game
        :return: The id of the game.
        """
        if game_name is None:
            return None
        game_name = str(game_name)

        # Cache
        if (self.cache.get('game') != None and self.cache.get('game').get(game_name) != None):
            return self.cache.get('game').get(game_name)


        self.connect()
        query = ("""
            SELECT id FROM dim_game WHERE game=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (game_name,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                cur.execute(dim_game_table_insert, (game_name,))
                self.conn.commit()
                cur.execute(query, (game_name,))
                row = cur.fetchone()
                id = row[0]
        cur.close()


        if self.cache.get('game')==None:
            self.cache.update({"game": {}})
        self.cache['game'][game_name] = id
        return id

    def load_department(self, department_name):
        """
        It checks if the department name is in the cache, if not it checks if it's in
        the database, if not it inserts it into the database and returns the id

        :param department_name: The name of the department
        :return: The id of the department.
        """
        if department_name is None:
            return None
        department_name = str(department_name)
        # Cache
        if (self.cache.get('department') != None and self.cache.get('department').get(department_name) != None):
            return self.cache.get('department').get(department_name)


        self.connect()
        query = ("""
            SELECT id FROM dim_department WHERE department=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (department_name,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                cur.execute(dim_department_table_insert, (department_name,))
                self.conn.commit()
                cur.execute(query, (department_name,))
                row = cur.fetchone()
                id = row[0]
        cur.close()


        if self.cache.get('department')==None:
            self.cache.update({"department": {}})
        self.cache['department'][department_name] = id
        return id

    def load_date(self, full_date):
        """
        It takes a timestamp, converts it to a datetime object, then inserts it into
        the database if it doesn't already exist

        :param full_date: The date to be inserted into the table
        :return: The id of the date.
        """
        self.connect()

        # convert timestamp column to datetime
        t = pd.to_datetime([full_date])
        # insert time data records
        time_data = [(full_date, tt.day, tt.week, tt.month, tt.quarter, tt.year, tt.weekday()) for tt in t]
        column_labels = ('full_date', 'day', 'week', 'month', 'quarter', 'year', 'weekday')
        time_df = pd.DataFrame(data=time_data, columns=column_labels)
        query = ("""
            SELECT id FROM dim_date WHERE full_date=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (full_date,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                for i, row in time_df.iterrows():
                    cur.execute(dim_date_table_insert, list(row))
                self.conn.commit()
                cur.execute(query, (full_date,))
                row = cur.fetchone()
                id = row[0]
        cur.close()
        return id

    def load_context(self, dimension_name, dimension_level):
        """
        It takes a dimension name and dimension level, and returns the id of the
        dimension in the database

        :param dimension_name: The name of the dimension
        :param dimension_level: This is the level of the dimension. For example, if
        you have a dimension called "Country", then the level would be 1. If you
        have a dimension called "State", then the level would be 2
        :return: The id of the dimension.
        """

        dimension_name = str(dimension_name)

        if dimension_name is None:
            return None
        elif dimension_name == "":
            return None

        # Cache
        if (self.cache.get('dimension_' + str(dimension_level)) and self.cache.get('dimension_' + str(dimension_level)).get(dimension_name) != None):
            return self.cache.get('dimension_' + str(dimension_level)).get(dimension_name)

        self.connect()
        query = ("""
            SELECT id FROM dim_dimension WHERE dimension_name=%s AND dimension_level=%s LIMIT 1;
        """)
        with self.conn.cursor() as cur:
            cur.execute(query, (dimension_name, dimension_level,))
            row = cur.fetchone()
            if row:
                id = row[0]
            else:
                cur.execute(dim_dimension_table_insert, (dimension_name, dimension_level,))
                self.conn.commit()
                id = cur.fetchone()[0]
        cur.close()

        if self.cache.get('dimension_' + str(dimension_level))==None:
            self.cache.update({'dimension_' + str(dimension_level): {}})
        self.cache['dimension_' + str(dimension_level)][dimension_name] = id

        return id

    def execute(self, query):
        """
        It connects to the database, executes the query, commits the changes, closes
        the cursor, and returns the number of rows affected

        :param query: The query to execute
        :return: The number of rows affected by the query.
        """
        if query is None:
            return None

        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()
        cur.close()
        return cur.rowcount


    def load_dim(self, column_name, fact_field, dim_field, dim_table, df):
        if column_name is None:
            return None
        if df is None:
            return None
        if dim_table is None:
            return None

        list_dim = df.groupby([column_name]).median().index.get_level_values(0).astype(str).tolist()

        self.connect()
        query = f"""
            SELECT id, {dim_field} FROM {dim_table};
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

        dim_table_insert = f"""
            INSERT INTO {dim_table} ({dim_field}) VALUES (%s) RETURNING id;
        """
        with self.conn.cursor() as cur:
            for v in list_dim:
                if v == '':
                    continue
                found = False
                for row in records:
                    if row[1] == v:
                        found = True

                if found == False:
                    cur.execute(dim_table_insert, (v,))
                    self.conn.commit()
                    id = cur.fetchone()[0]
                    records.append([id,v])
            cur.close()

        convert_list = dict(records)
        df[fact_field] = df[column_name].apply(lambda x: next((k for k, v in convert_list.items() if str(x) == str(v)), None))
        df.drop(column_name, axis=1, inplace=True)

        df = df.convert_dtypes()

        return df

    def load_context_df(self, column_name, fact_field, level, df):
        if column_name is None:
            return None
        if df is None:
            return None


        list_dim = df.groupby([column_name]).median().index.get_level_values(0).astype(str).tolist()

        self.connect()
        query = f"""
            SELECT id, dimension_name FROM dim_dimension WHERE dimension_level = {level};
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

        dim_table_insert = f"""
            INSERT INTO dim_dimension (dimension_name, dimension_level) VALUES (%s, %s) RETURNING id;
        """
        with self.conn.cursor() as cur:
            for v in list_dim:
                if v == '':
                    continue
                found = False
                for row in records:
                    if row[1] == v:
                        found = True

                if found == False:
                    cur.execute(dim_table_insert, (v,level))
                    self.conn.commit()
                    id = cur.fetchone()[0]
                    records.append([id,v])
            cur.close()

        convert_list = dict(records)
        df[fact_field] = df[column_name].apply(lambda x: next((k for k, v in convert_list.items() if str(x) == str(v)), None))
        df.drop(column_name, axis=1, inplace=True)

        df = df.convert_dtypes()

        return df
