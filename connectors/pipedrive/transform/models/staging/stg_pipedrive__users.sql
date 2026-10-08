with source as (

    select * from {{ pipedrive_source('users') }}
    where not _is_deleted

)

select
    id as user_id,
    name as user_name,
    email,
    phone,
    default_currency,
    locale,
    timezone_name,
    timezone_offset,
    role_id,
    icon_url,
    activated as is_activated,
    active_flag as is_active,
    has_created_company,
    last_login as last_login_at,
    created as created_at,
    modified as updated_at,
    _raw_load_id,
    _loaded_at

from source
