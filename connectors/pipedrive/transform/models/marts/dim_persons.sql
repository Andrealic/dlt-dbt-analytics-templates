with persons as (

    select * from {{ ref('stg_pipedrive__persons') }}

),

organizations as (

    select * from {{ ref('stg_pipedrive__organizations') }}

)

select
    persons.person_id,
    persons.person_name,
    persons.first_name,
    persons.last_name,
    persons.primary_email,
    persons.primary_phone,
    persons.job_title,
    persons.owner_user_id,
    persons.organization_id,
    organizations.organization_name,
    persons.label_ids,
    persons.added_at,
    persons.updated_at
    {%- for column in pipedrive_custom_field_names('person_fields', 'persons') %},
    persons.{{ column }}
    {%- endfor %}

from persons
left join organizations
    on persons.organization_id = organizations.organization_id
