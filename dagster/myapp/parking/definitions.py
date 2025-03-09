import dagster as dg

from dagster_duckdb import DuckDBResource
from parking.assets import (
    bronze_cars,
    bronze_transactions,
    bronze_parking_lots,
    bronze_customers,
    silver_cars,
    silver_customers,
    silver_parking_lots,
    silver_transactions,
    silver_transactions_total_fare__missing_value,
    silver_customers_mobile_no__missing_value,
    fact_cars_customer,
    dim_cars,
    dim_customers,
    dim_transactions,
    dim_parking_lots,
    fact_transactions,
    gold_customer_report,
)

defs = dg.Definitions(
    assets=[
        bronze_cars,
        bronze_transactions,
        bronze_parking_lots,
        bronze_customers,
        silver_cars,
        silver_customers,
        silver_parking_lots,
        silver_transactions,
        dim_cars,
        dim_customers,
        dim_transactions,
        dim_parking_lots,
        fact_cars_customer,
        fact_transactions,
        gold_customer_report,
    ],
    asset_checks=[
        silver_transactions_total_fare__missing_value,
        silver_customers_mobile_no__missing_value,
    ],
    resources={"duckdb": DuckDBResource(database="mydb.duckdb")},
)
