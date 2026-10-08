with source as (

    select * from {{ pipedrive_source('pipelines') }}
    where not _is_deleted

)

select
    id as pipeline_id,
    name as pipeline_name,
    order_nr as pipeline_order,
    is_deal_probability_enabled,
    add_time as added_at,
    update_time as updated_at,
    _raw_load_id,
    _loaded_at

from source
