import dagster as dg
from dagster_duckdb import DuckDBResource

bronze_group_name = "bronze"
silver_group_name = "silver"
gold_group_name = "gold"
base_silver_group_name = "base_silver"


def create_fact_dim(
    query, table_name, schema_name, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    table_name = table_name
    schema_name = base_silver_group_name

    with duckdb.get_connection() as conn:
        conn.execute(query)

        preview_query = f"select * from {schema_name}.{table_name} limit 10"
        preview_df = conn.execute(preview_query).fetchdf()

        row_count = conn.execute(
            f"select count(*) from {schema_name}.{table_name}"
        ).fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )


def create_bronze_table(
    table_name: str, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    schema_name = bronze_group_name
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {schema_name}.{table_name} as (
                select *, current_timestamp as etl_dt from read_csv_auto('parking/data/{table_name}.csv')
            )
            """
        )

        preview_df = conn.execute(
            f"select * from {schema_name}.{table_name} limit 10"
        ).fetchdf()
        count = conn.execute(
            f"select count(*) from {schema_name}.{table_name}"
        ).fetchone()[0]

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )


def create_silver_table(
    table_name: str, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    schema_name = silver_group_name
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {schema_name}.{table_name} as (
                with cte as (
                    select * from {bronze_group_name}.{table_name}
                )
                select *, current_timestamp as etl_dt from cte
            )
            """
        )

        preview_df = conn.execute(
            f"select * from {schema_name}.{table_name} limit 10"
        ).fetchdf()
        count = conn.execute(
            f"select count(*) from {schema_name}.{table_name}"
        ).fetchone()[0]
        last_etl_dt = conn.execute(
            f"select CAST(MAX(etl_dt) AS VARCHAR)  from {schema_name}.{table_name}"
        ).fetchone()[0]

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "last_etl_dt": dg.MetadataValue.text(last_etl_dt),
            }
        )


def create_missing_value_check(
    table_name: str, column_name: str, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        query_result = conn.execute(
            f"""
                select count(*) from {table_name}
                where {column_name} is null
                """
        ).fetchone()

        count = query_result[0] if query_result else 0
        return dg.AssetCheckResult(
            passed=count == 0, metadata={"missing column value": count}
        )
