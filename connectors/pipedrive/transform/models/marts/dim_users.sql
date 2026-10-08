with users as (

    select * from {{ ref('stg_pipedrive__users') }}

)

select
    user_id,
    user_name,
    email,
    role_id,
    timezone_name,
    default_currency,
    is_active,
    last_login_at,
    created_at,
    updated_at

from users
