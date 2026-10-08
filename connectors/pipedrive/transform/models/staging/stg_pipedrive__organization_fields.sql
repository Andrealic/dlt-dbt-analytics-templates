select
    field_code,
    field_name,
    field_type,
    is_custom_field,
    is_optional_response_field,
    options,
    subfields,
    _raw_load_id,
    _loaded_at

from {{ pipedrive_source('organization_fields') }}
