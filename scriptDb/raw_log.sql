CREATE TABLE IF NOT EXISTS raw_log (
  `log_time` DateTime64(3, 'UTC'),
  `market` Nullable(String),
  `social` Nullable(String),
  `platform` Nullable(String),
  `user_id` Int64,
  `user_name` Nullable(String),
  `log_action` Nullable(String),
  `level` Int32,
  `user_age` Int32,
  `gold` Int64,
  `extra_1` Nullable(String),
  `extra_2` Nullable(String),
  `extra_3` Nullable(String),
  `extra_4` Nullable(String),
  `extra_5` Nullable(String),
  `extra_6` Nullable(String),
  `extra_7` Nullable(String),
  `extra_8` Nullable(String),
  `extra_9` Nullable(String),
  `extra_10` Nullable(String),
  `extra_11` Nullable(String),
  `extra_12` Nullable(String),
  `extra_13` Nullable(String),
  `extra_14` Nullable(String),
  `extra_15` Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(log_time)
ORDER BY log_time
TTL toDateTime(log_time) + INTERVAL 1 YEAR DELETE
SETTINGS index_granularity = 8192;


CREATE TABLE log_file
(
    `file_name` String,
    `imported_rows` UInt32,
    `import_at` DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree
ORDER BY import_at
TTL toDateTime(import_at) + INTERVAL 1 YEAR DELETE
SETTINGS index_granularity = 8192;
