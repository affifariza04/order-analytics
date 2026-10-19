-- Type 2 slowly changing dimension: one row per version of a customer.
--
-- Built as a pure function of the immutable daily exports rather than with
-- `dbt snapshot`: snapshots record history only while they run, so a missed run
-- loses it forever and a rebuild can't reproduce it. Because raw exports are
-- kept, history here can be rebuilt exactly, any time.
with exports as (
    select *, city || '|' || tier as state
    from {{ ref('stg_customer_exports') }}
),

changes as (
    select
        *,
        lag(state) over (partition by customer_id order by export_date) as previous_state
    from exports
),

versions as (
    select * from changes
    where previous_state is null or previous_state <> state
)

select
    customer_id || '@' || cast(export_date as varchar) as customer_version_id,
    customer_id,
    name,
    city,
    tier,
    signup_date,
    export_date as valid_from,
    lead(export_date) over (partition by customer_id order by export_date) as valid_to,  -- exclusive
    lead(export_date) over (partition by customer_id order by export_date) is null as is_current
from versions
