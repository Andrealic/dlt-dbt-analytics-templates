{#
    Incremental strategy `overwrite`: replaces every row of the existing table with the
    model's result, in the run's transaction.

    For models that select their complete current state on every run and are written to
    an Iceberg REST catalog. DuckDB can neither rename nor replace tables there, which
    rules out `table` and `--full-refresh`, and its Iceberg MERGE accepts a single UPDATE
    or DELETE action, so rows no longer produced cannot be removed by a merge. Connector
    projects expose it as `get_incremental_overwrite_sql`, the name dbt resolves for the
    strategy.
#}
{% macro incremental_overwrite_sql(arg_dict) -%}
    {%- set target = arg_dict['target_relation'] -%}
    {%- set columns = get_quoted_csv(arg_dict['dest_columns'] | map(attribute='name')) -%}
    delete from {{ target }};

    insert into {{ target }} ({{ columns }})
    select {{ columns }}
    from {{ arg_dict['temp_relation'] }}
{%- endmacro %}
