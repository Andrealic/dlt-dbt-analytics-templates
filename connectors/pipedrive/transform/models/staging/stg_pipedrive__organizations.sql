with source as (

    select * from {{ pipedrive_source('organizations') }}
    where not _is_deleted

)

select
    id as organization_id,
    name as organization_name,
    owner_id as owner_user_id,
    visible_to,
    json_extract_string(address, '$.value') as address,
    json_extract_string(address, '$.locality') as address_locality,
    json_extract_string(address, '$.postal_code') as address_postal_code,
    json_extract_string(address, '$.country') as address_country,
    website,
    linkedin,
    industry as industry_option_id,
    annual_revenue,
    employee_count,
    from_json(label_ids, '["BIGINT"]') as label_ids,
    add_time as added_at,
    update_time as updated_at,
    custom_fields
    {{- pipedrive_decode_custom_fields('organization_fields', 'organizations') }},
    _raw_load_id,
    _loaded_at

from source
