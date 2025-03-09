import dagster as dg
from dagster import op, job
from utils.function import *
from dagster_duckdb import DuckDBResource
from exchange_rates.assets import dim_exchange_rates


@dg.asset(compute_kind="duckdb", group_name=bronze_group_name)
def bronze_cars(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_bronze_table("cars", duckdb)


@dg.asset(compute_kind="duckdb", group_name=bronze_group_name)
def bronze_customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_bronze_table("customers", duckdb)


@dg.asset(compute_kind="duckdb", group_name=bronze_group_name)
def bronze_parking_lots(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_bronze_table("parking_lots", duckdb)


@dg.asset(compute_kind="duckdb", group_name=bronze_group_name)
def bronze_transactions(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_bronze_table("transactions", duckdb)


@dg.asset(compute_kind="duckdb", group_name=silver_group_name, deps=[bronze_cars])
def silver_cars(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_silver_table("cars", duckdb)


@dg.asset(compute_kind="duckdb", group_name=silver_group_name, deps=[bronze_customers])
def silver_customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_silver_table("customers", duckdb)


@dg.asset(
    compute_kind="duckdb", group_name=silver_group_name, deps=[bronze_parking_lots]
)
def silver_parking_lots(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_silver_table("parking_lots", duckdb)


@dg.asset(
    compute_kind="duckdb", group_name=silver_group_name, deps=[bronze_transactions]
)
def silver_transactions(duckdb: DuckDBResource) -> dg.MaterializeResult:
    return create_silver_table("transactions", duckdb)


@dg.asset_check(asset=silver_transactions)
def silver_transactions_total_fare__missing_value(
    duckdb: DuckDBResource,
) -> dg.AssetCheckResult:
    create_missing_value_check(
        f"{silver_group_name}.transactions", "total_fare", duckdb
    )


@dg.asset_check(asset=silver_customers)
def silver_customers_mobile_no__missing_value(
    duckdb: DuckDBResource,
) -> dg.AssetCheckResult:
    create_missing_value_check(f"{silver_group_name}.customers", "mobile_no", duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=base_silver_group_name,
    deps=[silver_customers],
)
def dim_customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "dim_customers"
    schema_name = base_silver_group_name

    query = f"""
            create or replace table {schema_name}.{table_name} as (
            select 
                base.id_no as customer_id,
                base.name,
                base.is_active,
                base.mobile_no,
                base.id_no,
                base.country,
                base.created_at,
                base.updated_at,
                current_timestamp as etl_dt
            from {silver_group_name}.customers as base
        )

        """
    return create_fact_dim(query, table_name, schema_name, duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=base_silver_group_name,
    deps=[silver_parking_lots],
)
def dim_parking_lots(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "dim_parking_lots"
    schema_name = base_silver_group_name

    query = f"""
            create or replace table {schema_name}.{table_name} as (
    select 
        base.lot_id,
        base.name,
        base.address,
        base.fare_per_hour,
        base.created_at,
        base.updated_at,
        current_timestamp as etl_dt
    from {silver_group_name}.parking_lots as base
    )
        """
    return create_fact_dim(query, table_name, schema_name, duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=base_silver_group_name,
    deps=[silver_cars],
)
def dim_cars(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "dim_cars"
    schema_name = base_silver_group_name

    query = f"""
        create or replace table {schema_name}.{table_name} as (
    select 
        no_plat as car_id,
        customer_id,
        current_timestamp as etl_dt
    from {silver_group_name}.cars as base
    )
        """
    return create_fact_dim(query, table_name, schema_name, duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=base_silver_group_name,
    deps=[silver_transactions],
)
def dim_transactions(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "dim_transactions"
    schema_name = base_silver_group_name

    query = f"""
        create or replace table {schema_name}.{table_name} as (
    select 
        trans.id as transaction_id,
        trans.total_fare,
        trans.entry_time,
        trans.out_time,
        trans.customer_id,
        trans.lot_id,
        car.car_id as car_no_plat,
        current_timestamp as etl_dt
    from {silver_group_name}.transactions trans
    left join {schema_name}.dim_cars car on car.car_id = trans.car_id
    )
        """
    return create_fact_dim(query, table_name, schema_name, duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=base_silver_group_name,
    deps=[dim_customers, dim_cars],
)
def fact_cars_customer(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "fact_master_customers"
    schema_name = base_silver_group_name

    query = f"""
            create or replace table {schema_name}.fact_master_customers as (
                select 
                    base.id_no,
                    count(distinct car.no_plat) as total_cars,
                    current_timestamp as etl_dt
                from {silver_group_name}.customers as base
                left join {silver_group_name}.cars car on car.customer_id = base.id_no
                group by base.id_no
            )
        """
    return create_fact_dim(query, table_name, schema_name, duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=base_silver_group_name,
    deps=[dim_transactions, dim_customers, dim_exchange_rates],
)
def fact_transactions(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "fact_transactions"
    schema_name = base_silver_group_name

    query = f"""
            create or replace table {schema_name}.fact_transactions as (
                select 
                    trans.customer_id,
                    trans.lot_id,
                    car.car_id as car_no_plat,
                    count(trans.transaction_id) as total_transactions,
                    sum(trans.total_fare) as total_fare,
                    (cast(rt.myr_usd as float) * sum(trans.total_fare)) as total_fare_usd,
                    avg(trans.total_fare) as avg_fare,
                    max(trans.entry_time) as last_entry_time,
                    min(trans.out_time) as first_out_time,
                    current_timestamp as etl_dt
                from {schema_name}.dim_transactions trans
                left join {schema_name}.dim_cars car on car.car_id = trans.car_no_plat
                left join {schema_name}.dim_exchange_rates rt on rt.date = trans.entry_time::date
                group by 
                    trans.customer_id, 
                    trans.lot_id, 
                    car.car_id,
                    rt.myr_usd
            )
        """
    return create_fact_dim(query, table_name, schema_name, duckdb)


@dg.asset(
    compute_kind="duckdb",
    group_name=gold_group_name,
    deps=[fact_cars_customer, fact_transactions],
)
def gold_customer_report(duckdb: DuckDBResource) -> dg.MaterializeResult:
    table_name = "customer_report"
    schema_name = base_silver_group_name

    query = f"""
        create or replace table {schema_name}.{table_name} as (
            select 
                trans.*,
                current_timestamp as etl_dt
            from {base_silver_group_name}.fact_transactions trans
        )
        """
    return create_fact_dim(query, table_name, schema_name, duckdb)


@op
def create_duckdb_schema(duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        for i in ["bronze", "silver", "base_silver", "gold"]:
            query = f"create schema if not exists {i}"
            conn.execute(query)


@job
def init_job():
    create_duckdb_schema()
