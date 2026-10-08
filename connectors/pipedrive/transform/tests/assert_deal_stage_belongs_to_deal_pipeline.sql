-- A deal's stage must belong to the deal's pipeline.
select
    deals.deal_id,
    deals.pipeline_id,
    deals.stage_id,
    stages.pipeline_id as stage_pipeline_id

from {{ ref('fct_deals') }} as deals
inner join {{ ref('int_pipedrive__pipeline_stages') }} as stages
    on deals.stage_id = stages.stage_id

where deals.pipeline_id != stages.pipeline_id
