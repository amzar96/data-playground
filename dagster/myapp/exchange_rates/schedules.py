import dagster as dg
from exchange_rates.assets import get_today_exchange_rate

daily_exchange_rate = dg.ScheduleDefinition(
    name="get_exchange_rate_today",
    target=get_today_exchange_rate,
    cron_schedule="0 0 * * 1",  # every Monday at midnight
)
