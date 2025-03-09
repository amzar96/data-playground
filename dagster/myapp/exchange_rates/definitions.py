import dagster as dg
from dagster_duckdb import DuckDBResource
from exchange_rates.schedules import daily_exchange_rate
from exchange_rates.assets import get_today_exchange_rate, silver_exchange_rates

defs = dg.Definitions(
    jobs=[get_today_exchange_rate],
    schedules=[daily_exchange_rate],
    assets=[silver_exchange_rates],
    resources={"duckdb": DuckDBResource(database="mydb.duckdb")},
)
