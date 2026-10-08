with deals as (

    select * from {{ ref('stg_pipedrive__deals') }}

),

stages as (

    select * from {{ ref('int_pipedrive__pipeline_stages') }}

),

users as (

    select * from {{ ref('stg_pipedrive__users') }}

),

organizations as (

    select * from {{ ref('stg_pipedrive__organizations') }}

),

persons as (

    select * from {{ ref('stg_pipedrive__persons') }}

)

select
    deals.deal_id,
    deals.title,
    deals.status,
    deals.deal_value,
    deals.currency,
    deals.probability,
    deals.lost_reason,
    deals.pipeline_id,
    stages.pipeline_name,
    deals.stage_id,
    stages.stage_name,
    stages.stage_order,
    deals.owner_user_id,
    users.user_name as owner_name,
    deals.organization_id,
    organizations.organization_name,
    deals.person_id,
    persons.person_name,
    deals.is_archived,
    deals.expected_close_date,
    deals.added_at,
    deals.updated_at,
    deals.stage_changed_at,
    deals.closed_at,
    deals.won_at,
    deals.lost_at
    {%- for column in pipedrive_custom_field_names('deal_fields', 'deals') %},
    deals.{{ column }}
    {%- endfor %}

from deals
left join stages
    on deals.stage_id = stages.stage_id
left join users
    on deals.owner_user_id = users.user_id
left join organizations
    on deals.organization_id = organizations.organization_id
left join persons
    on deals.person_id = persons.person_id
