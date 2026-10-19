-- Daily revenue by channel, in Jakarta business days.
select
    order_date_wib,
    channel,
    count(*) as orders_placed,
    count(*) filter (where status in ('paid', 'refunded')) as orders_paid,
    coalesce(sum(amount_idr) filter (where status in ('paid', 'refunded')), 0) as gross_revenue_idr,
    coalesce(sum(amount_idr) filter (where status = 'refunded'), 0) as refunds_idr,
    coalesce(sum(amount_idr) filter (where status = 'paid'), 0) as net_revenue_idr,
    round(
        coalesce(sum(amount_idr) filter (where status in ('paid', 'refunded')), 0)
        / nullif(count(*) filter (where status in ('paid', 'refunded')), 0)
    ) as avg_order_value_idr,
    round(count(*) filter (where status in ('paid', 'refunded')) / count(*), 4) as payment_conversion
from {{ ref('fct_orders') }}
group by all
