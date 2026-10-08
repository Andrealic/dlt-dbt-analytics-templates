{# Entry points dbt resolves only in the root project, implemented in dlt_dbt_shared. #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ dlt_dbt_shared.lake_schema_name(custom_schema_name, node) }}
{%- endmacro %}

{% macro generate_database_name(custom_database_name, node) -%}
    {{ dlt_dbt_shared.lake_database_name(custom_database_name, node) }}
{%- endmacro %}

{% macro get_incremental_overwrite_sql(arg_dict) -%}
    {{ dlt_dbt_shared.incremental_overwrite_sql(arg_dict) }}
{%- endmacro %}
