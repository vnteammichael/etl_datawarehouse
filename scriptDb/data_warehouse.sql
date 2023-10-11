CREATE TABLE dim_date
(
  id BIGSERIAL PRIMARY KEY,
  full_date date DEFAULT NOW() NOT NULL,
  quarter int2 DEFAULT 0 NOT NULL,
  day int2 DEFAULT 0 NOT NULL,
  week int2 DEFAULT 0 NOT NULL,
  month int2 DEFAULT 0 NOT NULL,
  year int4 DEFAULT 0 NOT NULL,
  weekday int2 DEFAULT 0 NOT NULL,
  UNIQUE(full_date)
);
CREATE TABLE dim_metric
(
  id SERIAL PRIMARY KEY,
  metric_name varchar,
  log_name varchar,
  metric_description varchar,
  metric_value varchar,
  metric_value_2 varchar,
  dimension_1 varchar,
  dimension_2 varchar,
  dimension_3 varchar,
  UNIQUE(metric_name)
);

CREATE TABLE dim_dimension
(
  id SERIAL PRIMARY KEY,
  dimension_name varchar,
  dimension_level int2 DEFAULT 1 NOT NULL,
  UNIQUE(dimension_name, dimension_level)
);

CREATE TABLE IF NOT EXISTS log_transform_daily
(
  id SERIAL PRIMARY KEY,
  transform_name varchar,
  execute_time int4 DEFAULT 0 NOT NULL,
  created_date date DEFAULT NOW() NOT NULL
);

CREATE TABLE dim_user
(
  id SERIAL PRIMARY KEY,
  market varchar,
  social varchar,
  platform varchar,
  user_id int8 DEFAULT 0 NOT NULL,
  paid_amount int8 DEFAULT 0 NOT NULL,
  purchases int4 DEFAULT 0 NOT NULL,
  level int4 ,
  gold int8 ,
  register_date date DEFAULT NOW() NOT NULL,
  r_quantile int2 DEFAULT 1 NOT NULL,
  f_quantile int2 DEFAULT 1 NOT NULL,
  m_quantile int2 DEFAULT 1 NOT NULL,
  UNIQUE(user_id)
);

CREATE TABLE dim_user_daily
(
  id SERIAL PRIMARY KEY,
  full_date date DEFAULT NOW() NOT NULL,
  market varchar,
  social varchar,
  platform varchar,
  level int4 DEFAULT 0 NOT NULL,
  gold int8 DEFAULT 0 NOT NULL,
  user_id int8 DEFAULT 0 NOT NULL,
  paid_amount int8 DEFAULT 0 NOT NULL,
  purchases int4 DEFAULT 0 NOT NULL,
  level int4,
  gold int8,
  UNIQUE(full_date,user_id)
);

CREATE TABLE dim_gold_range
(
  id SERIAL PRIMARY KEY,
  gold_range varchar,
  UNIQUE(gold_range)
);

CREATE TABLE dim_level_range
(
  id SERIAL PRIMARY KEY,
  level_range varchar,
  UNIQUE(level_range)
);
CREATE TABLE dim_platform
(
  id SERIAL PRIMARY KEY,
  platform_name varchar,
  UNIQUE(platform_name)
);
CREATE TABLE dim_market
(
  id SERIAL PRIMARY KEY,
  market_name varchar,
  UNIQUE(market_name)
);
CREATE TABLE dim_social
(
  id SERIAL PRIMARY KEY,
  social_name varchar,
  UNIQUE(social_name)
);

CREATE TABLE fact_snapshot
(
  id BIGSERIAL PRIMARY KEY,
  date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
  market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
  social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
  platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
  metric_id int4 REFERENCES dim_metric(id) ON DELETE RESTRICT,
  metric_value int8 DEFAULT 0 NOT NULL,
  metric_value_2 int8 DEFAULT 0 NOT NULL,
  dimension_1_id int4 REFERENCES dim_dimension(id) ON DELETE RESTRICT,
  dimension_2_id int4 REFERENCES dim_dimension(id) ON DELETE RESTRICT,
  dimension_3_id int4 REFERENCES dim_dimension(id) ON DELETE RESTRICT
);

CREATE TABLE fact_daily_measure
(
  id BIGSERIAL PRIMARY KEY,
  date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
  market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
  social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
  platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
  level_range_id int4 REFERENCES dim_level_range(id) ON DELETE RESTRICT,
  total_daily_active int4 DEFAULT 0 NOT NULL,
  total_daily_active_users int4 DEFAULT 0 NOT NULL,
  total_new_register_users int4 DEFAULT 0 NOT NULL,
  total_paying_users int4 DEFAULT 0 NOT NULL,
  total_purchases int4 DEFAULT 0 NOT NULL,
  total_revenue int8 DEFAULT 0 NOT NULL,
  total_first_paying_users int4 DEFAULT 0 NOT NULL,
  total_gold int8 DEFAULT 0 NOT NULL
);
CREATE TABLE fact_churn_measure
(
  id BIGSERIAL PRIMARY KEY,
  date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
  market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
  social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
  platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
  level_range_id int4 REFERENCES dim_level_range(id) ON DELETE RESTRICT,
  churn_type varchar,
  churn_users int4 DEFAULT 0 NOT NULL,
  total_daily_active_users int4 DEFAULT 0 NOT NULL
);
CREATE TABLE fact_payment_measure
(
  id BIGSERIAL PRIMARY KEY,
  date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
  market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
  social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
  platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
  level_range_id int4 REFERENCES dim_level_range(id) ON DELETE RESTRICT,
  total_paying_users int4 DEFAULT 0 NOT NULL,
  total_paying_users int4 DEFAULT 0 NOT NULL,

);
