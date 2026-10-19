{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- One row per order, folded from its event stream.
--
-- Incremental strategy for late data: each run finds orders that received ANY
-- event in files newer than the watermark (max last_loaded_date already in the
-- table) and recomputes those orders from their complete event history. An event
-- arriving 3 days late therefore corrects its order instead of being lost, and
-- re-running with no new files changes nothing (idempotent).

with events as (
    select * from {{ ref('stg_order_events') }}
    {% if is_incremental() %}
    where order_id in (
        select order_id from {{ ref('stg_order_events') }}
        where load_date > (select coalesce(max(last_loaded_date), date '1900-01-01') from {{ this }})
    )
    {% endif %}
),

orders as (
    select
        order_id,
        any_value(customer_id) as customer_id,
        min(occurred_at_utc) filter (where event_type = 'order_placed') as placed_at_utc,
        min(occurred_at_utc) filter (where event_type = 'order_paid') as paid_at_utc,
        min(occurred_at_utc) filter (where event_type = 'order_refunded') as refunded_at_utc,
        max(amount_idr) filter (where event_type = 'order_placed') as amount_idr,
        coalesce(max(channel) filter (where channel <> 'unknown'), 'unknown') as channel,
        max(load_date) as last_loaded_date
    from events
    group by order_id
    -- A payment can arrive before its order_placed event; wait for the order itself.
    having placed_at_utc is not null
),

with_dates as (
    select
        *,
        -- Business day is Jakarta's (UTC+7, no DST), not UTC's.
        cast(placed_at_utc + interval 7 hour as date) as order_date_wib
    from orders
)

select
    o.order_id,
    o.customer_id,
    o.order_date_wib,
    o.placed_at_utc,
    o.paid_at_utc,
    o.refunded_at_utc,
    case
        when o.refunded_at_utc is not null then 'refunded'
        when o.paid_at_utc is not null then 'paid'
        else 'pending'
    end as status,
    o.amount_idr,
    o.channel,
    -- Point-in-time join: the customer's tier/city *when they ordered*, not today.
    c.tier as customer_tier_at_order,
    c.city as customer_city_at_order,
    o.last_loaded_date
from with_dates as o
left join {{ ref('dim_customers_scd2') }} as c
    on c.customer_id = o.customer_id
    and o.order_date_wib >= c.valid_from
    and (c.valid_to is null or o.order_date_wib < c.valid_to)
