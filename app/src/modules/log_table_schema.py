"""Set raw logs table config."""
from numpy import int64

log_table_name = "raw_log"
log_file_table_name = "log_file"
dim_user_daily_table_name = "dim_user_daily"


schema = {
    "log_time": int64,
    "market": str,
    "social": str,
    "platform": str,
    "user_id": int,
    "user_name": str,
    "log_action": str,
    "level": int,
    "user_age": int,
    "gold": int,
    "extra_1": str,
    "extra_2": str,
    "extra_3": str,
    "extra_4": str,
    "extra_5": str,
    "extra_6": str,
    "extra_7": str,
    "extra_8": str,
    "extra_9": str,
    "extra_10": str,
    "extra_11": str,
    "extra_12": str,
    "extra_13": str,
    "extra_14": str,
    "extra_15": str
}

