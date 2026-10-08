# Changelog

All notable changes to the Pipedrive connector. Versions follow semver as defined in the
connector interface: bronze schema and mart grain changes are major.

## [0.1.0] - Unreleased

### Added

- Ingest step 1: Pipedrive API to raw JSONL with verbatim payloads, incremental on
  `update_time` for deals, persons, organizations, and activities; full refresh for
  pipelines, stages, users, and deal, person, and organization fields.
- Ingest step 2: raw to bronze Iceberg, upserted on primary key with a cursor over raw
  loads; deletions flagged from the API (deals) and from complete listings
  (`raw --reconcile`).
- Staging models for every bronze table, with custom field decoding and an optional
  allow-list.
- Marts: `fct_deals`, `fct_activities`, `dim_persons`, `dim_organizations`, `dim_users`.
