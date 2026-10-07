# dlt-dbt-analytics-templates

Open-source dlt + dbt templates that land SaaS API data in a datalake, then transform it with warehouse-agnostic models for analytics engineering.

## Approach

- **Ingestion:** [dlt](https://dlthub.com) pipelines extract data from SaaS APIs into a datalake on S3 or GCS.
- **Storage:** raw API responses are kept as files; curated layers are stored as Apache Iceberg tables.
- **Transformation:** [dbt](https://www.getdbt.com) models (staging, intermediate, marts) run on DuckDB and read from and write to the lake.

## Connectors

Connectors are built and released one at a time.

| Connector  | Status      |
|------------|-------------|
| Pipedrive  | In progress |
| HubSpot    | Planned     |
| Salesforce | Planned     |

## Status

Early development. Interfaces may change until the first connector is released.
