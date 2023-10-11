"""Database client."""
from ..utils.log import LOGGER
from clickhouse_driver import Client

class DatabaseClickHouse:
    """Clickhouse Database class."""

    def __init__(
            self,
            CLICKHOUSE_HOST,
            CLICKHOUSE_USERNAME,
            CLICKHOUSE_PASSWORD,
            CLICKHOUSE_PORT,
            CLICKHOUSE_NAME,
            settings = {}
        ):
        self.host = CLICKHOUSE_HOST
        self.username = CLICKHOUSE_USERNAME
        self.password = CLICKHOUSE_PASSWORD
        self.dbname = CLICKHOUSE_NAME
        self.port = CLICKHOUSE_PORT
        self.settings = settings
        self.conn = None

    def connect(self):
        """Connect to a Clickhouse database."""
        if self.conn is None:
            try:
                self.conn = Client(self.host,
                    user=self.username,
                    password=self.password,
                    database=self.dbname,
                    port=self.port,
                    settings=self.settings
                    # settings={'use_numpy': True}
                )
            except Exception as e:
                LOGGER.error(e)
                raise e
            # finally:
                # LOGGER.info('Connection ClickHouse opened successfully.')

    def select_rows(self, query):
        records = []
        """Run a SQL query to select rows from table."""
        self.connect()
        try:
            result = self.conn.execute(query)
            for row in result:
                records.append(tuple(row))
        except Exception as e:
                LOGGER.error(str(e)[:250])
        return records

    def select_rows_dict(self, query):
        records = []
        """Run a SQL query to select rows from table."""
        self.connect()

        try:
            df = self.conn.query_dataframe(query)
            records = df.to_dict('records')
        except Exception as e:
                LOGGER.error(str(e)[:250])
        return records

    def execute(self, query, data, types_check=True):
        """Execute a SQL query."""
        self.connect()
        try:
            self.conn.execute(query, data, types_check)
        except Exception as e:
                LOGGER.error(str(e)[:250])

    def insert_dataframe(self, query, df):
        """Execute a SQL query."""
        self.connect()
        try:
            self.conn.insert_dataframe(query, df)
        except Exception as e:
                LOGGER.error(str(e)[:250])