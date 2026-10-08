# Pipedrive connector

Loads Pipedrive CRM data into an Iceberg lake and models it with dbt on DuckDB.

Version 0.1.0, unreleased. Built against the Pipedrive API v2, with v1 only where v2 has
no equivalent (users).

## How it works

1. **Raw** (`pipedrive-ingest raw`): calls the API and appends every record verbatim, as
   the JSON column `payload`, to
   `<lake_root>/raw/pipedrive/<entity>/load_date=YYYY-MM-DD/<load_id>.<file_id>.jsonl.gz`.
2. **Bronze** (`pipedrive-ingest bronze`): reads the raw loads not yet applied and writes
   one Iceberg table per entity in the namespace `pipedrive`, upserted on the primary key.
   Each row carries `_load_id`, `_raw_load_id`, `_loaded_at`, `_source_updated_at`, and,
   where deletions are tracked, `_is_deleted`.
3. **dbt** (`transform/`): staging, intermediate, and mart models, written as Iceberg
   tables to the namespaces `staging`, `intermediate`, and `marts` of the same catalog.

Bronze is derived only from raw, so it can be rebuilt from raw at any time.

## Entities

| Entity                | Endpoint                                   | Sync         | Deletions                          |
|-----------------------|--------------------------------------------|--------------|------------------------------------|
| deals                 | `GET /api/v2/deals`, `/api/v2/deals/archived` | incremental | flagged by the API (see below)   |
| persons               | `GET /api/v2/persons`                      | incremental  | reconciliation                     |
| organizations         | `GET /api/v2/organizations`                | incremental  | reconciliation                     |
| activities            | `GET /api/v2/activities`                   | incremental  | reconciliation                     |
| pipelines             | `GET /api/v2/pipelines`                    | full refresh | replaced; `is_deleted` flag        |
| stages                | `GET /api/v2/stages`                       | full refresh | replaced; `is_deleted` flag        |
| users                 | `GET /api/v1/users`                        | full refresh | replaced; `is_deleted` flag        |
| deal_fields           | `GET /api/v2/dealFields`                   | full refresh | replaced                           |
| person_fields         | `GET /api/v2/personFields`                 | full refresh | replaced                           |
| organization_fields   | `GET /api/v2/organizationFields`           | full refresh | replaced                           |

Bronze columns are declared in
[`ingest/src/pipedrive_ingest/schemas/`](ingest/src/pipedrive_ingest/schemas/). Fields the
API returns beyond those stay in raw until they are declared.

**Incremental sync.** The cursor is `update_time`, passed as `updated_since` (inclusive).
Raw stores the newest `update_time` seen per entity in the dlt state; records updated in
that same second are fetched again and deduplicated in bronze.

**Deletions.**

- Deals are requested with `status=open,won,lost,deleted`, which returns deals deleted in
  the last 30 days with `is_deleted: true`; bronze sets `_is_deleted` from it. Archived
  deals are listed by a separate endpoint and kept, with `is_archived: true`.
- The list endpoints for persons, organizations, and activities do not return deleted
  records. `pipedrive-ingest raw --reconcile` lists these entities, and deals, completely;
  bronze then flags rows missing from that listing as deleted. Schedule it often enough
  for your reporting needs, for example daily.
- Staging excludes rows with `_is_deleted`. Bronze keeps them.

## Authentication and permissions

The connector authenticates with an API token, sent in the `x-api-token` header. A token
belongs to a user and sees only what that user can see, so use the token of an admin
user (or a dedicated admin user) to load the whole account. Deletion reconciliation
relies on the same visibility: a record the user stops seeing is treated as deleted.

OAuth is not supported yet. An OAuth app would need the scopes `deals:read`,
`contacts:read`, `activities:read`, and `users:read`.

## Rate limits

Pipedrive meters requests per company with a daily token budget (30,000 tokens times a
plan multiplier times the number of seats) and limits bursts per token over a rolling
two-second window. Each call has a token cost: 10 per v2 list page (20 for archived
deals), 5 for pipelines and stages, 20 for v1 users. Pages hold up to 500 records, so a
run without changes costs about 120 tokens, and a reconciliation run costs about 10
tokens per 500 records listed. Requests answered with `429` or `5xx` are retried with
exponential backoff; an exhausted daily budget fails the run.

## Configuration

Settings come from environment variables (or `.dlt/config.toml` and
`.dlt/secrets.toml`, which are git-ignored). `dev` and `prod` differ only in these values.

| Variable | Purpose |
|----------|---------|
| `LAKE_ROOT` | Lake root, for example `s3://my-bucket/lake` or `gs://my-bucket/lake` |
| `SOURCES__PIPEDRIVE__API_TOKEN` | Pipedrive API token |
| `SOURCES__PIPEDRIVE__BASE_URL` | Optional; defaults to `https://api.pipedrive.com` |
| `DESTINATION__FILESYSTEM__CREDENTIALS__*` | Object store credentials, as for dlt's filesystem destination: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `REGION_NAME` (S3), or `PROJECT_ID`, `CLIENT_EMAIL`, `PRIVATE_KEY` (GCS) |
| `ICEBERG_CATALOG__ICEBERG_CATALOG_TYPE` | `rest` |
| `ICEBERG_CATALOG__ICEBERG_CATALOG_CONFIG` | pyiceberg REST catalog properties as JSON, for example `{"uri": "https://catalog.example.com", "warehouse": "dev", "credential": "<client-id>:<client-secret>"}` |

Raw and bronze are read and written with the same storage credentials.

## Running

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```sh
cd connectors/pipedrive/ingest
uv sync
uv run pipedrive-ingest raw              # API to raw
uv run pipedrive-ingest raw --reconcile  # API to raw, complete listings
uv run pipedrive-ingest bronze           # raw to bronze
```

`raw` and `bronze` are independent: `bronze` applies every raw load written since its
last run, in order. A typical schedule runs `raw`, then `bronze`, then dbt, with
`raw --reconcile` in place of `raw` once a day.

Then dbt, with DuckDB attaching the Iceberg REST catalog:

```sh
cd connectors/pipedrive/transform
cp profiles.example.yml profiles.yml   # then set the environment variables it reads
uv tool install dbt-core==1.11.15 --with dbt-duckdb==1.11.0   # or any env with both
dbt deps
dbt build --target dev
```

## dbt models

| Layer        | Models |
|--------------|--------|
| staging      | `stg_pipedrive__deals`, `__persons`, `__organizations`, `__activities`, `__pipelines`, `__stages`, `__users`, `__deal_fields`, `__person_fields`, `__organization_fields` |
| intermediate | `int_pipedrive__pipeline_stages` |
| marts        | `fct_deals`, `fct_activities`, `dim_persons`, `dim_organizations`, `dim_users` |

Every model selects its complete current state and replaces its rows on each run (the
`overwrite` incremental strategy from `packages/shared`), because dbt-duckdb cannot yet
create or replace Iceberg tables with `table` or `--full-refresh`. Each run adds two
snapshots per table; expire old snapshots with your catalog's maintenance tooling.

Variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `lake_catalog` | `lake` | Alias of the attached Iceberg catalog |
| `output_locations` | `{}` | Per layer, `lake` or `engine`; `engine` writes into the DuckDB database instead, for example `{marts: engine}` |
| `pipedrive_custom_fields` | `{}` | Custom field allow-list, see below |

Foreign keys from deals and activities to persons, organizations, users, and deals are
tested with severity `warn`: a record deleted in Pipedrive leaves its dimension while
records that reference it remain.

### Custom fields

In API v2, custom field values sit in a `custom_fields` object keyed by each field's
40-character code. Bronze keeps that object as JSON, so bronze columns never change when
fields are added, renamed, or removed in Pipedrive.

Staging decodes custom fields of deals, persons, and organizations into columns, using
the bronze `*_fields` tables at compile time. Option fields are decoded to labels;
monetary, date range, time, and time range fields produce companion columns (`_currency`,
`_until`, `_timezone`). By default every custom field becomes `custom_<field name in
snake_case>`, so staging columns follow the account's configuration. To pin a reviewed
set of columns, list them per entity with the column name to use:

```yaml
vars:
  pipedrive_custom_fields:
    deals:
      dcf558aac1ae4e8c4f849ba5e668430d8df9be12: lead_source
```

A custom field added in Pipedrive becomes a new column of the staging and mart tables on
the next run. A removed or renamed field leaves its old column in place, holding null.

## Limitations

- Deal history (stage changes), products, leads, notes, files, emails, goals, and
  webhooks are not loaded.
- Activities have no custom fields in API v2.
- Deletions of persons, organizations, and activities are only detected by
  `raw --reconcile`, as are deal deletions older than 30 days at the time of the next run.
- A bronze table is created with its entity's first record; until then staging reads it
  as empty.
- Fields the API adds to a record are not in bronze until declared in the bronze schema;
  raw keeps them.
- Concurrent dbt threads conflict on Iceberg commits; profiles use `threads: 1`.

## API references

- [API v2 OpenAPI specification](https://developers.pipedrive.com/docs/api/v2/openapi.yaml) and
  [v1 specification](https://developers.pipedrive.com/docs/api/v1/openapi.yaml)
- [Migration guide to API v2](https://pipedrive.readme.io/docs/pipedrive-api-v2-migration-guide):
  custom field format, cursor pagination, `updated_since`
- [Authentication](https://pipedrive.readme.io/docs/core-api-concepts-authentication) and
  [finding the API token](https://pipedrive.readme.io/docs/how-to-find-the-api-token)
- [Rate limiting](https://pipedrive.readme.io/docs/core-api-concepts-rate-limiting)
- [Pagination](https://pipedrive.readme.io/docs/core-api-concepts-pagination)
- [Custom fields](https://pipedrive.readme.io/docs/core-api-concepts-custom-fields)
- [Changelog: deleted records no longer returned by list endpoints](https://developers.pipedrive.com/changelog/post/bug-fix-in-data-returned-by-three-endpoints)
