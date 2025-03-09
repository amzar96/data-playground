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
                result JSON
            )
            """
            conn.execute(query)

            for item in api_data:
                date = item["date"]
                result = json.dumps(
                    {key: value for key, value in item.items() if key != "date"}
                )
                insert_query = f"INSERT INTO bronze.exchange_rates (date, result) VALUES ('{date}', '{result}')"
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
