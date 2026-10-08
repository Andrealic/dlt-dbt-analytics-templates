with source as (

    select * from {{ pipedrive_source('activities') }}
    where not _is_deleted

)

select
    id as activity_id,
    subject,
    type as activity_type,
    owner_id as owner_user_id,
    creator_user_id,
    deal_id,
    lead_id,
    person_id,
    org_id as organization_id,
    project_id,
    due_date,
    due_time,
    duration,
    busy as is_busy,
    done as is_done,
    marked_as_done_time as done_at,
    priority,
    outcome as outcome_id,
    json_extract_string(location, '$.value') as location,
    participants,
    attendees,
    conference_meeting_client,
    conference_meeting_url,
    conference_meeting_id,
    public_description,
    note,
    add_time as added_at,
    update_time as updated_at,
    _raw_load_id,
    _loaded_at

from source
