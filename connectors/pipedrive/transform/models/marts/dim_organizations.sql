with organizations as (

    select * from {{ ref('stg_pipedrive__organizations') }}

)

select
    organization_id,
    organization_name,
    owner_user_id,
    address,
    address_locality,
    address_postal_code,
    address_country,
    website,
    linkedin,
    industry_option_id,
    annual_revenue,
    employee_count,
    label_ids,
    added_at,
    updated_at
    {%- for column in pipedrive_custom_field_names('organization_fields', 'organizations') %},
    {{ column }}
    {%- endfor %}

from organizations
