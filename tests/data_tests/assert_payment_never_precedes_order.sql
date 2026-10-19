select order_id from {{ ref('fct_orders') }}
where paid_at_utc < placed_at_utc or refunded_at_utc < paid_at_utc
