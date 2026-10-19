-- Daily full exports of the CRM, kept forever (that is what makes SCD2 rebuildable).
select *
from read_json(
    '{{ env_var("RAW_PATH", "data/raw") }}/customers/*/*.jsonl',
    format = 'newline_delimited',
    filename = true,
    columns = {
        customer_id: 'VARCHAR', name: 'VARCHAR', city: 'VARCHAR', tier: 'VARCHAR',
        signup_date: 'DATE', updated_at: 'DATE'
    }
)
