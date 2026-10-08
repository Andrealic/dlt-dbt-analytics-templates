with source as (

    select * from {{ pipedrive_source('stages') }}
    where not _is_deleted

)

select
    id as stage_id,
    pipeline_id,
    name as stage_name,
    order_nr as stage_order,
    deal_probability,
    is_deal_rot_enabled,
    days_to_rotten,
    add_time as added_at,
    update_time as updated_at,
    _raw_load_id,
    _loaded_at

from source
