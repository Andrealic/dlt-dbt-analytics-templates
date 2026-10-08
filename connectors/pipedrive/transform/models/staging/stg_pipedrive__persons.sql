with source as (

    select * from {{ pipedrive_source('persons') }}
    where not _is_deleted

)

select
    id as person_id,
    name as person_name,
    first_name,
    last_name,
    owner_id as owner_user_id,
    org_id as organization_id,
    list_filter(
        from_json(emails, '[{"value": "VARCHAR", "primary": "BOOLEAN"}]'),
        email -> email['primary']
    )[1]['value'] as primary_email,
    list_filter(
        from_json(phones, '[{"value": "VARCHAR", "primary": "BOOLEAN"}]'),
        phone -> phone['primary']
    )[1]['value'] as primary_phone,
    emails,
    phones,
    visible_to,
    from_json(label_ids, '["BIGINT"]') as label_ids,
    picture_id,
    json_extract_string(postal_address, '$.value') as postal_address,
    notes,
    im,
    try_cast(birthday as date) as birthday,
    job_title,
    add_time as added_at,
    update_time as updated_at,
    custom_fields
    {{- pipedrive_decode_custom_fields('person_fields', 'persons') }},
    _raw_load_id,
    _loaded_at

from source
