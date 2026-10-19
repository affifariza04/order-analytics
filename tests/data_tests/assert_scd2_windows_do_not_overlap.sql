-- Each customer has exactly one valid version on any date.
select a.customer_id, a.valid_from, b.valid_from as overlapping_from
from {{ ref('dim_customers_scd2') }} as a
join {{ ref('dim_customers_scd2') }} as b
  on a.customer_id = b.customer_id
 and a.customer_version_id <> b.customer_version_id
 and a.valid_from < coalesce(b.valid_to, date '9999-12-31')
 and b.valid_from < coalesce(a.valid_to, date '9999-12-31')
