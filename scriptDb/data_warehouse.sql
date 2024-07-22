-- CREATE TABLE dim_date
-- (
--   id BIGSERIAL PRIMARY KEY,
--   full_date date DEFAULT NOW() NOT NULL,
--   quarter int2 DEFAULT 0 NOT NULL,
--   day int2 DEFAULT 0 NOT NULL,
--   week int2 DEFAULT 0 NOT NULL,
--   month int2 DEFAULT 0 NOT NULL,
--   year int4 DEFAULT 0 NOT NULL,
--   weekday int2 DEFAULT 0 NOT NULL,
--   UNIQUE(full_date)
-- );
-- CREATE TABLE dim_metric
-- (
--   id SERIAL PRIMARY KEY,
--   metric_name varchar,
--   log_name varchar,
--   metric_description varchar,
--   metric_value varchar,
--   metric_value_2 varchar,
--   dimension_1 varchar,
--   dimension_2 varchar,
--   dimension_3 varchar,
--   UNIQUE(metric_name)
-- );

-- CREATE TABLE dim_dimension
-- (
--   id SERIAL PRIMARY KEY,
--   dimension_name varchar,
--   dimension_level int2 DEFAULT 1 NOT NULL,
--   UNIQUE(dimension_name, dimension_level)
-- );

-- CREATE TABLE IF NOT EXISTS log_transform_daily
-- (
--   id SERIAL PRIMARY KEY,
--   transform_name varchar,
--   execute_time int4 DEFAULT 0 NOT NULL,
--   created_date date DEFAULT NOW() NOT NULL
-- );

-- CREATE TABLE dim_user
-- (
--   id SERIAL PRIMARY KEY,
--   market varchar,
--   social varchar,
--   platform varchar,
--   user_id int8 DEFAULT 0 NOT NULL,
--   paid_amount int8 DEFAULT 0 NOT NULL,
--   purchases int4 DEFAULT 0 NOT NULL,
--   level int4 ,
--   gold int8 ,
--   register_date date DEFAULT NOW() NOT NULL,
--   r_quantile int2 DEFAULT 1 NOT NULL,
--   f_quantile int2 DEFAULT 1 NOT NULL,
--   m_quantile int2 DEFAULT 1 NOT NULL,
--   UNIQUE(user_id)
-- );

-- CREATE TABLE dim_user_daily
-- (
--   id SERIAL PRIMARY KEY,
--   full_date date DEFAULT NOW() NOT NULL,
--   market varchar,
--   social varchar,
--   platform varchar,
--   level int4 DEFAULT 0 NOT NULL,
--   gold int8 DEFAULT 0 NOT NULL,
--   user_id int8 DEFAULT 0 NOT NULL,
--   paid_amount int8 DEFAULT 0 NOT NULL,
--   purchases int4 DEFAULT 0 NOT NULL,
--   level int4,
--   gold int8,
--   UNIQUE(full_date,user_id)
-- );

-- CREATE TABLE dim_gold_range
-- (
--   id SERIAL PRIMARY KEY,
--   gold_range varchar,
--   UNIQUE(gold_range)
-- );

-- CREATE TABLE dim_level_range
-- (
--   id SERIAL PRIMARY KEY,
--   level_range varchar,
--   UNIQUE(level_range)
-- );
-- CREATE TABLE dim_platform
-- (
--   id SERIAL PRIMARY KEY,
--   platform_name varchar,
--   UNIQUE(platform_name)
-- );
-- CREATE TABLE dim_market
-- (
--   id SERIAL PRIMARY KEY,
--   market_name varchar,
--   UNIQUE(market_name)
-- );
-- CREATE TABLE dim_social
-- (
--   id SERIAL PRIMARY KEY,
--   social_name varchar,
--   UNIQUE(social_name)
-- );

-- CREATE TABLE fact_snapshot
-- (
--   id BIGSERIAL PRIMARY KEY,
--   date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
--   market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
--   social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
--   platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
--   metric_id int4 REFERENCES dim_metric(id) ON DELETE RESTRICT,
--   metric_value int8 DEFAULT 0 NOT NULL,
--   metric_value_2 int8 DEFAULT 0 NOT NULL,
--   dimension_1_id int4 REFERENCES dim_dimension(id) ON DELETE RESTRICT,
--   dimension_2_id int4 REFERENCES dim_dimension(id) ON DELETE RESTRICT,
--   dimension_3_id int4 REFERENCES dim_dimension(id) ON DELETE RESTRICT
-- );

-- CREATE TABLE fact_daily_measure
-- (
--   id BIGSERIAL PRIMARY KEY,
--   date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
--   market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
--   social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
--   platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
--   level_range_id int4 REFERENCES dim_level_range(id) ON DELETE RESTRICT,
--   total_daily_active int4 DEFAULT 0 NOT NULL,
--   total_daily_active_users int4 DEFAULT 0 NOT NULL,
--   total_new_register_users int4 DEFAULT 0 NOT NULL,
--   total_paying_users int4 DEFAULT 0 NOT NULL,
--   total_purchases int4 DEFAULT 0 NOT NULL,
--   total_revenue int8 DEFAULT 0 NOT NULL,
--   total_first_paying_users int4 DEFAULT 0 NOT NULL,
--   total_gold int8 DEFAULT 0 NOT NULL
-- );
-- CREATE TABLE fact_churn_measure
-- (
--   id BIGSERIAL PRIMARY KEY,
--   date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
--   market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
--   social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
--   platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
--   level_range_id int4 REFERENCES dim_level_range(id) ON DELETE RESTRICT,
--   churn_type varchar,
--   churn_users int4 DEFAULT 0 NOT NULL,
--   total_daily_active_users int4 DEFAULT 0 NOT NULL
-- );
-- CREATE TABLE fact_payment_measure
-- (
--   id BIGSERIAL PRIMARY KEY,
--   date_id int8 REFERENCES dim_date(id) ON DELETE RESTRICT,
--   market_id int4 REFERENCES dim_market(id) ON DELETE RESTRICT,
--   social_id int4 REFERENCES dim_social(id) ON DELETE RESTRICT,
--   platform_id int4 REFERENCES dim_platform(id) ON DELETE RESTRICT,
--   level_range_id int4 REFERENCES dim_level_range(id) ON DELETE RESTRICT,
--   total_paying_users int4 DEFAULT 0 NOT NULL,
--   total_paying_users int4 DEFAULT 0 NOT NULL,

-- );
create table dim_date
(
    id        bigint auto_increment
        primary key,
    full_date date     default current_timestamp() not null,
    quarter   smallint default 0                   not null,
    day       smallint default 0                   not null,
    week      smallint default 0                   not null,
    month     smallint default 0                   not null,
    year      int      default 0                   not null,
    weekday   smallint default 0                   not null,
    constraint full_date
        unique (full_date)
);

create table dim_department
(
    id         int auto_increment
        primary key,
    department varchar(50)                           null,
    detail     varchar(100)                          null,
    create_at  timestamp default current_timestamp() null
);

create table dim_game
(
    id        int auto_increment
        primary key,
    create_at timestamp default current_timestamp() null,
    game      varchar(200)                          not null
);

create table dim_user
(
    id            bigint auto_increment
        primary key,
    date          date      default curdate()           not null,
    username      varchar(50)                           not null,
    department_id int                                   not null,
    game_id       int                                   not null,
    bet_amount    bigint    default 0                   not null,
    win_amount    bigint    default 0                   not null,
    win_match     int       default 0                   not null,
    total_match   int       default 0                   not null,
    detail        text      default ''                  not null,
    create_at     timestamp default current_timestamp() not null,
    constraint dim_user_dim_department_id_fk
        foreign key (department_id) references dim_department (id),
    constraint user_history_dim_game_id_fk
        foreign key (game_id) references dim_game (id)
);

create index user_history_date_user_id_index
    on dim_user (date, username, game_id);

create index user_history_dim_user_id_fk2
    on dim_user (username);

create table fact_churn_measure
(
    id                       int auto_increment
        primary key,
    date_id                  bigint                                null,
    department_id            int                                   null,
    churn_type               varchar(50)                           null,
    churn_users              int       default 0                   not null,
    total_daily_active_users int       default 0                   not null,
    create_at                timestamp default current_timestamp() null,
    constraint fact_churn_measure_dim_date_id_fk
        foreign key (date_id) references dim_date (id),
    constraint fact_churn_measure_dim_department_id_fk
        foreign key (department_id) references dim_department (id)
);

create table fact_snapshot
(
    id             bigint auto_increment
        primary key,
    date_id        bigint       default 0                   not null,
    department_id  int          default 0                   not null,
    metric         varchar(100) default '0'                 not null,
    metric_value   int          default 0                   not null,
    metric_value_2 int          default 0                   not null,
    dimension_1    varchar(50)  default '0'                 not null,
    dimension_2    varchar(50)  default '0'                 not null,
    dimension_3    varchar(50)  default '0'                 not null,
    create_at      timestamp    default current_timestamp() not null,
    constraint FK_fact_snapshot_dim_date
        foreign key (date_id) references dim_date (id),
    constraint FK_fact_snapshot_dim_department
        foreign key (department_id) references dim_department (id)
);

create table log_transform_daily
(
    id             int auto_increment
        primary key,
    transform_name varchar(100)                null,
    execute_time   int       default 0         not null,
    created_at     timestamp default curtime() not null
);

