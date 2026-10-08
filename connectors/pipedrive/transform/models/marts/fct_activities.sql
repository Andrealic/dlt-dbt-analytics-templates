with activities as (

    select * from {{ ref('stg_pipedrive__activities') }}

)

select
    activity_id,
    activity_type,
    subject,
    owner_user_id,
    creator_user_id,
    deal_id,
    lead_id,
    person_id,
    organization_id,
    project_id,
    due_date,
    due_time,
    duration,
    is_done,
    done_at,
    is_busy,
    priority,
    outcome_id,
    added_at,
    updated_at

from activities
