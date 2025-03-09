import json
import dagster as dg
from utils.api import API
from utils.function import *
from utils.common import now_date
from dagster_duckdb import DuckDBResource
from dagster import op, job, In, Out, Config


class JobConfig(Config):
    date: str = now_date
    full_refresh: bool = False


@op
def create_duckdb_schema(context, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        for i in ["bronze", "silver", "base_silver", "gold"]:
            query = f"create schema if not exists {i}"
            conn.execute(query)

    context.log.info("Schema created in DuckDB.")


@op(out=Out(list))
def fetch_data_from_api(context, config: JobConfig):
    context.log.info(f"Config {config}")

    now_date = config.date if config.date else now_date

    if config.full_refresh:
        endpoint = f"?id=exchangerates"
    else:
        endpoint = f"?id=exchangerates&limit=1&date_start={now_date}@date"

    api = API("data.gov")
    try:
        response = api.make_request(
            method="get",
            endpoint=endpoint,
        )
        context.log.info(f"Successfully fetched data from {api.url}")
        return response
    except Exception as e:
        context.log.error(f"Failed to fetch data from {api.url}: {e}")
        return []


@op(ins={"api_data": In(list)}, out=Out(str))
def process_and_insert_to_duckdb(context, api_data, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        try:
            query = """
            CREATE TABLE IF NOT EXISTS bronze.exchange_rates (
                date DATE,
                result JSON,
                etl_dt TIMESTAMP
            )
            """
            conn.execute(query)

            for item in api_data:
                date = item["date"]
                result = json.dumps(
                    {key: value for key, value in item.items() if key != "date"}
                )
                insert_query = f"INSERT INTO bronze.exchange_rates (date, result, etl_dt) VALUES ('{date}', '{result}', current_timestamp)"
                conn.execute(insert_query)

            context.log.info("Data inserted into DuckDB successfully")
            return "Data processing and insertion completed"
        except Exception as e:
            context.log.error(f"Error processing data: {e}")
            return "Error"


@job
def get_today_exchange_rate():
    create_duckdb_schema()
    api_data = fetch_data_from_api()
    process_and_insert_to_duckdb(api_data)


@dg.asset(compute_kind="duckdb", group_name="silver")
def silver_exchange_rates(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_silver_table("exchange_rates", duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=base_silver_group_name,
    deps=[silver_exchange_rates],
)
def dim_exchange_rates(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "dim_exchange_rates"
    schema_name = base_silver_group_name

    query = f"""
            create or replace table {schema_name}.{table_name} as (
            with ranked_data as (
                select
                    base.date::date as "date",
                    json_extract(result, '$.myr_usd') AS myr_usd,
                    json_extract(result, '$.myr_sgd') AS myr_sgd,
                    etl_dt as source_dt,
                    current_timestamp as etl_dt,
                    rank() over (partition by base.date::date order by etl_dt) as rank
                from {silver_group_name}.exchange_rates as base
            )
            select
                "date",
                myr_usd,
                myr_sgd,
                source_dt,
                etl_dt
            from ranked_data
            where rank = 1
            )
        """
    return create_fact_dim(query, table_name, schema_name, duckdb)
