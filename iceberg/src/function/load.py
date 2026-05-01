import duckdb

from src.function.ingest import CatalogClient
from src.utils.settings import settings


class DuckDBClient(CatalogClient):
    def __init__(self, db_path: str = ":memory:") -> None:
        super().__init__()
        self._db_path = db_path
        self._con: duckdb.DuckDBPyConnection | None = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(self._db_path)
            self._bootstrap()
        return self._con

    def register_table(self, namespace: str, table_name: str, view_name: str | None = None) -> str:
        table = self.catalog.load_table(f"{namespace}.{table_name}")
        view = view_name or table_name
        self.connection.execute(
            f"CREATE OR REPLACE VIEW {view} AS "
            f"SELECT * FROM iceberg_scan('{table.location()}')"
        )
        return view

    def query(self, sql: str) -> duckdb.DuckDBPyRelation:
        return self.connection.sql(sql)

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def _bootstrap(self) -> None:
        endpoint = settings.s3_endpoint.removeprefix("https://").removeprefix("http://")
        use_ssl = settings.s3_endpoint.startswith("https://")
        self._con.execute("INSTALL iceberg; LOAD iceberg;")
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        self._con.execute("SET unsafe_enable_version_guessing = true;")
        self._con.execute(f"""
            CREATE OR REPLACE SECRET s3_homelab (
                TYPE S3,
                KEY_ID '{settings.s3_key}',
                SECRET '{settings.s3_secret}',
                ENDPOINT '{endpoint}',
                REGION '{settings.s3_region}',
                USE_SSL {str(use_ssl).lower()},
                URL_STYLE 'path'
            );
        """)
