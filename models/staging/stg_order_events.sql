-- One clean row per event: typed, UTC, de-duplicated, schema drift absorbed.
with source as (
    select
        *,
        cast(regexp_extract(filename, 'dt=(\d{4}-\d{2}-\d{2})', 1) as date) as load_date
    from {{ ref('base_order_events') }}
),

visible as (
    select * from source
    where load_date <= cast('{{ var("as_of") }}' as date)
),

typed as (
    select
        event_id,
        event_type,
        order_id,
        customer_id,
        -- Mixed +07:00 / Z offsets normalised to one UTC timeline.
        cast(occurred_at as timestamptz) at time zone 'UTC' as occurred_at_utc,
        cast(amount_idr as bigint) as amount_idr,
        -- `channel` only exists from day 20; earlier events are honestly 'unknown'.
        coalesce(channel, 'unknown') as channel,
        load_date
    from visible
),

deduplicated as (
    -- At-least-once delivery: keep the first copy of each event id.
    select *
    from typed
    qualify row_number() over (partition by event_id order by load_date, occurred_at_utc) = 1
)

select * from deduplicated
