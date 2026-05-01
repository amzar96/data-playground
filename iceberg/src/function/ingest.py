import pyarrow as pa
import pyarrow.csv as pv
from pyiceberg.catalog.sql import SqlCatalog

from src.utils.constants import CATALOG_NAME, RAW_NAMESPACE
from src.utils.settings import settings


class CatalogClient:
    def __init__(self) -> None:
        self._catalog: SqlCatalog | None = None

    @property
    def catalog(self) -> SqlCatalog:
        if self._catalog is None:
            self._catalog = SqlCatalog(CATALOG_NAME, **settings.catalog_properties())
        return self._catalog

    def ensure_namespace(self, namespace: str) -> None:
        if (namespace,) not in self.catalog.list_namespaces():
            self.catalog.create_namespace(namespace)


class IngestFunction(CatalogClient):
    def ingest(
        self,
        csv_path: str,
        table_name: str,
        namespace: str = RAW_NAMESPACE,
    ) -> str:
        arrow_table = self._read_csv(csv_path)
        self.ensure_namespace(namespace)
        full_id = f"{namespace}.{table_name}"

        if self.catalog.table_exists(full_id):
            iceberg_table = self.catalog.load_table(full_id)
            iceberg_table.append(arrow_table)
            return iceberg_table.metadata_location

        iceberg_table = self.catalog.create_table(full_id, schema=arrow_table.schema)
        iceberg_table.append(arrow_table)
        return iceberg_table.metadata_location

    def _read_csv(self, csv_path: str) -> pa.Table:
        return pv.read_csv(csv_path)
