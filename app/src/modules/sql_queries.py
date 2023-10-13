# INSERT RECORDS
dim_group_table_insert = ("""
INSERT INTO dim_group (group_name) VALUES (%s) RETURNING id;
""")

dim_department_table_insert = ("""
INSERT IGNORE INTO dim_department (department) VALUES (%s);
""")

dim_game_table_insert = ("""
INSERT IGNORE INTO dim_game (game) VALUES (%s);
""")

dim_utm_source_table_insert = ("""
INSERT INTO dim_utm_source (utm_source) VALUES (%s) RETURNING id;
""")
dim_utm_medium_table_insert = ("""
INSERT INTO dim_utm_medium (utm_medium) VALUES (%s) RETURNING id;
""")
dim_utm_campaign_table_insert = ("""
INSERT INTO dim_utm_campaign (utm_campaign) VALUES (%s) RETURNING id;
""")

dim_metric_table_insert = """
INSERT INTO dim_metric (metric_name, log_name, metric_description, metric_value, metric_value_2, dimension_1, dimension_2, dimension_3) VALUES (%s, %s,  %s, %s, %s, %s, %s, %s) RETURNING id;
"""

dim_metric_table_update_description = """
UPDATE dim_metric SET
log_name = %s,
metric_description = %s,
metric_value = %s,
metric_value_2 = %s,
dimension_1 = %s,
dimension_2 = %s,
dimension_3 = %s
WHERE id=%s;
"""

dim_dimension_table_insert = ("""
INSERT INTO dim_dimension (dimension_name, dimension_level)
VALUES (%s, %s)
ON CONFLICT (dimension_name, dimension_level)
DO NOTHING
RETURNING id;
""")

dim_date_table_insert = ("""
INSERT IGNORE INTO dim_date (full_date, day, week, month, quarter, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s);
""")