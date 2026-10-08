{#
    A Pipedrive bronze table. The ingest creates a bronze table with its first record,
    so an entity with no records yet (a new account without activities, for example)
    has no table; it is then read as an empty relation with the declared columns.
#}
{% macro pipedrive_source(table_name) %}
    {%- set relation = source('pipedrive', table_name) -%}
    {%- if execute and adapter.get_relation(relation.database, relation.schema, relation.identifier) is none -%}
        {%- do log(relation ~ " does not exist yet; reading it as empty.", info=true) -%}
        {%- set columns = graph.sources['source.pipedrive.pipedrive.' ~ table_name].columns.values() -%}
        (
            select
            {%- for column in columns %}
                cast(null as {{ column.data_type }}) as {{ column.name }}{{ "," if not loop.last }}
            {%- endfor %}
            where false
        ) as {{ table_name }}
    {%- else -%}
        {{ relation }}
    {%- endif -%}
{% endmacro %}
