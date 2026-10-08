{#
    Relation naming for the lake contract. Connector projects call these from their own
    generate_schema_name and generate_database_name, which dbt only resolves in the root
    project.
#}

{#
    Models land in exactly the namespace set by their layer config (staging,
    intermediate, marts), without dbt's default target-schema prefix.
#}
{% macro lake_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{#
    Output location per layer. With `lake` (the default), a model is written to the
    attached Iceberg catalog named by `lake_catalog`. With `engine`, it is written to
    the DuckDB database of the target and is not part of the lake.

        vars:
          lake_catalog: lake
          output_locations:
            marts: engine
#}
{% macro lake_database_name(custom_database_name, node) -%}
    {%- set location = var('output_locations', {}).get(node.config.schema, 'lake') -%}
    {%- if custom_database_name is not none -%}
        {{ custom_database_name | trim }}
    {%- elif node.resource_type not in ['model', 'snapshot', 'seed'] -%}
        {{ target.database }}
    {%- elif location == 'lake' -%}
        {{ var('lake_catalog', 'lake') }}
    {%- elif location == 'engine' -%}
        {{ target.database }}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "output_locations." ~ node.config.schema ~ " must be 'lake' or 'engine', got '" ~ location ~ "'"
        ) }}
    {%- endif -%}
{%- endmacro %}
