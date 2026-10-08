with source as (

    select * from {{ pipedrive_source('deals') }}
    where not _is_deleted

)

select
    id as deal_id,
    title,
    status,
    value as deal_value,
    currency,
    probability,
    lost_reason,
    pipeline_id,
    stage_id,
    owner_id as owner_user_id,
    person_id,
    org_id as organization_id,
    visible_to,
    is_archived,
    origin,
    origin_id,
    channel,
    channel_id,
    arr,
    mrr,
    acv,
    from_json(label_ids, '["BIGINT"]') as label_ids,
    expected_close_date,
    add_time as added_at,
    update_time as updated_at,
    stage_change_time as stage_changed_at,
    close_time as closed_at,
    won_time as won_at,
    lost_time as lost_at,
    custom_fields
    {{- pipedrive_decode_custom_fields('deal_fields', 'deals') }},
    _raw_load_id,
    _loaded_at

from source
