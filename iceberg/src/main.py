import os
from pathlib import Path
from typing import Annotated

os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import typer

from src.function.ingest import IngestFunction
from src.function.load import DuckDBClient
from src.function.transform import TransformFunction
from src.utils.constants import (
    ENRICHED_TABLE,
    PARTITIONED_TABLE,
    RAW_NAMESPACE,
    RAW_TABLE,
    TRANSFORM_NAMESPACE,
)

app = typer.Typer(name="pipeline", no_args_is_help=True)


@app.command("ingest")
def cmd_ingest(
    csv_path: Annotated[Path, typer.Argument(help="Path to source CSV file")],
    table: Annotated[str, typer.Option("--table", "-t")] = RAW_TABLE,
    namespace: Annotated[str, typer.Option("--namespace", "-n")] = RAW_NAMESPACE,
) -> None:
    fn = IngestFunction()
    loc = fn.ingest(str(csv_path), table, namespace)
    typer.echo(f"Ingested → {loc}")


@app.command("transform")
def cmd_transform() -> None:
    fn = TransformFunction()
    loc = fn.enrich()
    typer.echo(f"Enriched  → {loc}")
    loc = fn.repartition_by_country()
    typer.echo(f"Partitioned → {loc}")


@app.command("schema-evolve")
def cmd_schema_evolve(
    namespace: Annotated[str, typer.Option()] = TRANSFORM_NAMESPACE,
    table: Annotated[str, typer.Option()] = ENRICHED_TABLE,
) -> None:
    fn = TransformFunction()
    fn.demonstrate_schema_evolution(namespace, table)
    typer.echo(f"Schema evolved on {namespace}.{table}")


@app.command("time-travel")
def cmd_time_travel(
    namespace: Annotated[str, typer.Option()] = TRANSFORM_NAMESPACE,
    table: Annotated[str, typer.Option()] = ENRICHED_TABLE,
) -> None:
    fn = TransformFunction()
    iceberg_table = fn.catalog.load_table(f"{namespace}.{table}")
    location = iceberg_table.location()
    counts = fn.demonstrate_time_travel(namespace, table)
    typer.echo(f"table location: {location}")
    typer.echo("")
    for snap_id, count in counts.items():
        typer.echo(f"  snapshot {snap_id}: {count:,} rows")
        typer.echo(f"    → iceberg_scan('{location}', snapshot_from_id={snap_id})")


@app.command("query")
def cmd_query(
    sql: Annotated[str, typer.Argument(help="SQL to run against Iceberg tables")],
) -> None:
    fn = DuckDBClient()
    fn.register_table(RAW_NAMESPACE, RAW_TABLE)
    fn.register_table(TRANSFORM_NAMESPACE, ENRICHED_TABLE)
    fn.register_table(TRANSFORM_NAMESPACE, PARTITIONED_TABLE)
    result = fn.query(sql)
    typer.echo(result.df().to_string(index=False))
    fn.close()


@app.command("run")
def cmd_run(
    csv_path: Annotated[Path, typer.Argument(help="Path to source CSV file")],
) -> None:
    cmd_ingest(csv_path)
    cmd_transform()


if __name__ == "__main__":
    app()
