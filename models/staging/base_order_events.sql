-- Raw drops as-is: immutable files, one folder per load date (dt=YYYY-MM-DD).
-- The explicit column list is the ingestion contract: types are read leniently
-- (amount arrives as int or string) and fixed in staging; unknown keys are ignored.
select *
from read_json(
    '{{ env_var("RAW_PATH", "data/raw") }}/order_events/*/*.jsonl',
    format = 'newline_delimited',
    filename = true,
    columns = {
        event_id: 'VARCHAR', event_type: 'VARCHAR', order_id: 'VARCHAR', customer_id: 'VARCHAR',
        occurred_at: 'VARCHAR', amount_idr: 'VARCHAR', channel: 'VARCHAR'
    }
)
