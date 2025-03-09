import dagster as dg
from exchange_rates.assets import get_today_exchange_rate

defs = dg.Definitions(jobs=[get_today_exchange_rate])
