select order_date_wib, channel, count(*) as n
from {{ ref('mart_daily_revenue') }}
group by all
having count(*) > 1
