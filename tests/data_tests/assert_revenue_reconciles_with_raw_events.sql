-- Independent recomputation: net revenue straight from de-duplicated events must
-- equal what the marts report. Catches double counting from duplicates or late data.
with from_events as (
    select
        sum(amount_idr) filter (where event_type = 'order_paid')
        - coalesce(sum(amount_idr) filter (where event_type = 'order_refunded'), 0) as net
    from {{ ref('stg_order_events') }}
    where order_id in (select order_id from {{ ref('fct_orders') }})
),
from_mart as (
    select sum(net_revenue_idr) as net from {{ ref('mart_daily_revenue') }}
)
select * from from_events, from_mart
where from_events.net <> from_mart.net
