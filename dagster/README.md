# ETL using Dagster
ETL stands for Extract, Transform, and Load. It is a process that combines data from multiple sources into a single database. There are many ETL tools available that facilitate the creation of an efficient and seamless ETL process, one of which is Dagster.

## Dagster
Dagster is a tool that focuses on data orchestration and is mainly built for data engineers to create data platforms. We can integrate many connectors (dbt, k8s, etc.) with Dagster or even use Dagster itself for end-to-end processing.

It not only has capabilities to develop and schedule jobs, but it can also ensure data quality, freshness, data observability, and data asset lineage.

## Docker Image
You can find the Docker image for this Dagster ETL job here:  
[amzar96/dagster-data-job](https://hub.docker.com/r/amzar96/dagster-data-job)
