-- Monthly cohort retention: of customers whose first paid order was in month M,
-- what share ordered again in month M+k?
with paid as (
    select customer_id, cast(date_trunc('month', order_date_wib) as date) as order_month
    from {{ ref('fct_orders') }}
    where status in ('paid', 'refunded')
),

firsts as (
    select customer_id, min(order_month) as cohort_month
    from paid
    group by customer_id
),

activity as (
    select distinct
        f.cohort_month,
        datediff('month', f.cohort_month, p.order_month) as months_since_first,
        p.customer_id
    from paid as p
    join firsts as f using (customer_id)
),

cohort_sizes as (
    select cohort_month, count(*) as cohort_size from firsts group by cohort_month
)

select
    a.cohort_month,
    a.months_since_first,
    s.cohort_size,
    count(*) as active_customers,
    round(count(*) / s.cohort_size, 4) as retention_rate
from activity as a
join cohort_sizes as s using (cohort_month)
group by a.cohort_month, a.months_since_first, s.cohort_size
