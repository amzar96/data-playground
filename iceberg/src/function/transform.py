import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import FloatType, TimestamptzType

from src.function.ingest import CatalogClient
from src.utils.constants import (
    ENRICHED_TABLE,
    PARTITIONED_TABLE,
    RAW_NAMESPACE,
    RAW_TABLE,
    TRANSFORM_NAMESPACE,
)


class TransformFunction(CatalogClient):
    def enrich(
        self,
        source_namespace: str = RAW_NAMESPACE,
        source_table: str = RAW_TABLE,
        dest_namespace: str = TRANSFORM_NAMESPACE,
        dest_table: str = ENRICHED_TABLE,
    ) -> str:
        self.ensure_namespace(dest_namespace)
        src = self.catalog.load_table(f"{source_namespace}.{source_table}")
        arrow = src.scan().to_arrow()

        arrow = self._add_email_domain(arrow)
        arrow = self._cast_price_to_double(arrow)

        return self._write_table(dest_namespace, dest_table, arrow)

    def repartition_by_country(
        self,
        source_namespace: str = TRANSFORM_NAMESPACE,
        source_table: str = ENRICHED_TABLE,
        dest_namespace: str = TRANSFORM_NAMESPACE,
        dest_table: str = PARTITIONED_TABLE,
    ) -> str:
        self.ensure_namespace(dest_namespace)
        src = self.catalog.load_table(f"{source_namespace}.{source_table}")
        arrow = src.scan().to_arrow()
        full_id = f"{dest_namespace}.{dest_table}"

        if self.catalog.table_exists(full_id):
            self.catalog.drop_table(full_id)

        iceberg_schema = src.schema()
        country_field_id = iceberg_schema.find_field("country").field_id
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=country_field_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="country",
            )
        )
        dest = self.catalog.create_table(
            full_id, schema=iceberg_schema, partition_spec=partition_spec
        )
        dest.append(arrow)
        return dest.metadata_location

    def demonstrate_schema_evolution(
        self, namespace: str = TRANSFORM_NAMESPACE, table_name: str = ENRICHED_TABLE
    ) -> None:
        table = self.catalog.load_table(f"{namespace}.{table_name}")
        existing = {f.name for f in table.schema().fields}
        new_cols = {"processed_at", "data_quality_score"} - existing
        if not new_cols:
            return

        with table.update_schema() as update:
            if "processed_at" in new_cols:
                update.add_column("processed_at", TimestamptzType())
            if "data_quality_score" in new_cols:
                update.add_column("data_quality_score", FloatType())

        arrow = table.scan().to_arrow()
        processed_at = pa.array([None] * len(arrow), type=pa.timestamp("us", tz="UTC"))
        quality_score = pa.array([None] * len(arrow), type=pa.float32())
        patch = pa.table({"processed_at": processed_at, "data_quality_score": quality_score})
        table.append(patch)

    def demonstrate_time_travel(
        self, namespace: str = TRANSFORM_NAMESPACE, table_name: str = ENRICHED_TABLE
    ) -> dict[int, int]:
        table = self.catalog.load_table(f"{namespace}.{table_name}")
        return {
            snap.snapshot_id: len(table.scan(snapshot_id=snap.snapshot_id).to_arrow())
            for snap in table.snapshots()
        }

    def _write_table(self, namespace: str, table_name: str, arrow: pa.Table) -> str:
        full_id = f"{namespace}.{table_name}"
        if self.catalog.table_exists(full_id):
            self.catalog.drop_table(full_id)
        dest: Table = self.catalog.create_table(full_id, schema=arrow.schema)
        dest.append(arrow)
        return dest.metadata_location

    def _add_email_domain(self, arrow: pa.Table) -> pa.Table:
        if "email" not in arrow.schema.names:
            return arrow
        parts = pc.split_pattern(arrow.column("email"), "@")
        domain = pc.list_slice(parts, 1, 2).combine_chunks()
        domain_flat = pa.chunked_array([pc.list_flatten(domain)])
        return arrow.append_column("email_domain", domain_flat)

    def _cast_price_to_double(self, arrow: pa.Table) -> pa.Table:
        if "price" not in arrow.schema.names:
            return arrow
        idx = arrow.schema.get_field_index("price")
        return arrow.set_column(idx, "price", arrow.column("price").cast(pa.float64()))
