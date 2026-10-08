{#
    Pipedrive custom fields.

    Bronze keeps every custom field value in the `custom_fields` JSON column, keyed by the
    field's 40-character code. These macros turn them into named, typed columns using the
    bronze `<entity>_fields` definitions, which are queried at compile time. Option fields
    (enum, set) are decoded to their labels.

    By default every custom field is decoded, as `custom_<field name in snake_case>`. When
    the `pipedrive_custom_fields` var lists fields for an entity, only those are decoded,
    under the given column names:

        vars:
          pipedrive_custom_fields:
            deals:
              dcf558aac1ae4e8c4f849ba5e668430d8df9be12: lead_source
#}

{# Decoded columns as `,\n<expression> as <column>` items, to follow the last fixed column. #}
{% macro pipedrive_decode_custom_fields(fields_table, entity) -%}
    {%- for column in pipedrive_custom_field_columns(fields_table, entity) %},
        {{ column.expression }} as {{ column.name }}
    {%- endfor -%}
{%- endmacro %}

{# Names of the decoded columns, in the order they are produced. #}
{% macro pipedrive_custom_field_names(fields_table, entity) -%}
    {{ return(pipedrive_custom_field_columns(fields_table, entity) | map(attribute='name') | list) }}
{%- endmacro %}

{% macro pipedrive_custom_field_columns(fields_table, entity) -%}
    {#- Resolved before the execute check so that parsing records the dependency. -#}
    {%- set fields_relation = source('pipedrive', fields_table) -%}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}

    {%- if adapter.get_relation(
            database=fields_relation.database,
            schema=fields_relation.schema,
            identifier=fields_relation.identifier) is none -%}
        {{ log(fields_relation ~ " does not exist; no custom fields decoded.") }}
        {{ return([]) }}
    {%- endif -%}

    {#- Contact sync fields are flagged as custom but stored as top-level record fields. -#}
    {%- set definitions = run_query(
        "select field_code, field_name, field_type, options from " ~ fields_relation
        ~ " where is_custom_field and regexp_full_match(field_code, '[0-9a-f]{40}')"
        ~ " order by field_name, field_code"
    ) -%}

    {%- set allowed = var('pipedrive_custom_fields', {}).get(entity, {}) -%}
    {%- set columns = [] -%}
    {%- set used = ['custom_fields'] -%}
    {%- for field in definitions.rows if not allowed or field['field_code'] in allowed -%}
        {%- set code = field['field_code'] -%}
        {%- if allowed -%}
            {%- set name = allowed[code] -%}
            {%- if not modules.re.fullmatch('[a-z_][a-z0-9_]*', name) -%}
                {{ exceptions.raise_compiler_error(
                    "pipedrive_custom_fields." ~ entity ~ "." ~ code ~ ": '" ~ name
                    ~ "' is not a lowercase snake_case identifier") }}
            {%- endif -%}
        {%- else -%}
            {%- set name = 'custom_' ~ (_pipedrive_snake_case(field['field_name']) or code[:8]) -%}
            {%- if field['field_type'] in ['user', 'org', 'people'] -%}
                {%- set name = name ~ '_id' -%}
            {%- endif -%}
            {%- if name in used -%}
                {%- set name = name ~ '_' ~ code[:8] -%}
            {%- endif -%}
        {%- endif -%}

        {%- for column in _pipedrive_custom_field_expressions(code, field['field_type'], name, field['options']) -%}
            {%- if column.name in used -%}
                {{ exceptions.raise_compiler_error(
                    "Custom field column '" ~ column.name ~ "' on " ~ entity ~ " is not unique") }}
            {%- endif -%}
            {%- do used.append(column.name) -%}
            {%- do columns.append(column) -%}
        {%- endfor -%}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}

{# Value formats per field type: https://pipedrive.readme.io/docs/pipedrive-api-v2-migration-guide #}
{% macro _pipedrive_custom_field_expressions(code, field_type, name, options_json) -%}
    {%- set value = _pipedrive_json_text(code) -%}
    {%- if field_type in ['varchar', 'varchar_auto', 'text', 'phone'] -%}
        {%- set columns = {name: value} -%}
    {%- elif field_type == 'double' -%}
        {%- set columns = {name: 'try_cast(' ~ value ~ ' as double)'} -%}
    {%- elif field_type in ['user', 'org', 'people'] -%}
        {%- set columns = {name: 'try_cast(' ~ value ~ ' as bigint)'} -%}
    {%- elif field_type == 'date' -%}
        {%- set columns = {name: 'try_cast(' ~ value ~ ' as date)'} -%}
    {%- elif field_type == 'monetary' -%}
        {%- set columns = {
            name: 'try_cast(' ~ _pipedrive_json_text(code, 'value') ~ ' as double)',
            name ~ '_currency': _pipedrive_json_text(code, 'currency'),
        } -%}
    {%- elif field_type == 'daterange' -%}
        {%- set columns = {
            name: 'try_cast(' ~ _pipedrive_json_text(code, 'value') ~ ' as date)',
            name ~ '_until': 'try_cast(' ~ _pipedrive_json_text(code, 'until') ~ ' as date)',
        } -%}
    {%- elif field_type == 'time' -%}
        {%- set columns = {
            name: 'try_cast(' ~ _pipedrive_json_text(code, 'value') ~ ' as time)',
            name ~ '_timezone': _pipedrive_json_text(code, 'timezone_name'),
        } -%}
    {%- elif field_type == 'timerange' -%}
        {%- set columns = {
            name: 'try_cast(' ~ _pipedrive_json_text(code, 'value') ~ ' as time)',
            name ~ '_until': 'try_cast(' ~ _pipedrive_json_text(code, 'until') ~ ' as time)',
            name ~ '_timezone': _pipedrive_json_text(code, 'timezone_name'),
        } -%}
    {%- elif field_type == 'address' -%}
        {%- set columns = {name: _pipedrive_json_text(code, 'value')} -%}
    {%- elif field_type == 'enum' -%}
        {%- set columns = {name: _pipedrive_option_label('try_cast(' ~ value ~ ' as bigint)', options_json)} -%}
    {%- elif field_type == 'set' -%}
        {%- set option_ids = "from_json(json_extract(custom_fields, '$.\"" ~ code ~ "\"'), '[\"BIGINT\"]')" -%}
        {%- set columns = {
            name: 'list_transform(' ~ option_ids ~ ', option_id -> '
                ~ _pipedrive_option_label('option_id', options_json) ~ ')'
        } -%}
    {%- else -%}
        {#- Unknown type: keep the JSON value as text. -#}
        {%- set columns = {name: value} -%}
    {%- endif -%}
    {%- set result = [] -%}
    {%- for column_name, expression in columns.items() -%}
        {%- do result.append({'name': column_name, 'expression': expression}) -%}
    {%- endfor -%}
    {{ return(result) }}
{%- endmacro %}

{# Text value of a custom field, or of one of its keys for object-valued types. #}
{% macro _pipedrive_json_text(code, key=none) -%}
    {%- set path = '$."' ~ code ~ '"' ~ ('.' ~ key if key else '') -%}
    {{ return("json_extract_string(custom_fields, '" ~ path ~ "')") }}
{%- endmacro %}

{# Maps an option id expression to its label using the field's options. #}
{% macro _pipedrive_option_label(option_id, options_json) -%}
    {%- set options = fromjson(options_json) if options_json else [] -%}
    {%- if not options -%}
        {{ return('cast(null as varchar)') }}
    {%- endif -%}
    {%- set branches = [] -%}
    {%- for option in options -%}
        {%- do branches.append("when " ~ option['id'] ~ " then '" ~ (option['label'] | replace("'", "''")) ~ "'") -%}
    {%- endfor -%}
    {{ return('case ' ~ option_id ~ ' ' ~ branches | join(' ') ~ ' end') }}
{%- endmacro %}

{% macro _pipedrive_snake_case(text) -%}
    {{ return(modules.re.sub('[^0-9a-z]+', '_', (text or '') | lower).strip('_')) }}
{%- endmacro %}
