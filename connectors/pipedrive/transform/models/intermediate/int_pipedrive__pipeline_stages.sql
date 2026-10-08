with stages as (

    select * from {{ ref('stg_pipedrive__stages') }}

),

pipelines as (

    select * from {{ ref('stg_pipedrive__pipelines') }}

)

select
    stages.stage_id,
    stages.stage_name,
    stages.stage_order,
    stages.deal_probability,
    stages.pipeline_id,
    pipelines.pipeline_name,
    pipelines.pipeline_order

from stages
inner join pipelines
    on stages.pipeline_id = pipelines.pipeline_id
