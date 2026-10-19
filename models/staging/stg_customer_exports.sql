-- Every daily CRM export, one row per (customer, export date).
select
    customer_id,
    name,
    city,
    tier,
    signup_date,
    cast(regexp_extract(filename, 'dt=(\d{4}-\d{2}-\d{2})', 1) as date) as export_date
from {{ ref('base_customer_exports') }}
where cast(regexp_extract(filename, 'dt=(\d{4}-\d{2}-\d{2})', 1) as date) <= cast('{{ var("as_of") }}' as date)
