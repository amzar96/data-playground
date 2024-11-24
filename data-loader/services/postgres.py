import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class PostgresHandler:
    def __init__(self, configs: dict):
        db_cfg = configs.get("database")

        self.db_name = db_cfg["db_name"]
        self.user = db_cfg["db_user"]
        self.password = db_cfg["db_password"]
        self.host = db_cfg["db_host"]
        self.port = db_cfg["db_port"]
        self.connection = None

    def create_connection(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            logger.info("database connection established successfully")
        except psycopg2.Error as e:
            logger.info(f"error connecting to the database: {e}")

    def execute_query(self, query, params=None, fetch=False):
        if self.connection is None:
            error_msg = (
                "database connection not established. Call create_connection() first"
            )
            logger.error(error_msg)
            raise Exception(error_msg)

        try:
            logger.debug(f"query: {query}")

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                else:
                    self.connection.commit()
                    logger.info("query executed successfully.")
        except psycopg2.Error as e:
            logger.error(f"error executing query: {e}")
            self.connection.rollback()

    def close_connection(self):
        if self.connection:
            self.connection.close()
            logger.info("database connection closed")
