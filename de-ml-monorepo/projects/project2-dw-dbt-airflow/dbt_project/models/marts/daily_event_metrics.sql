-- Mart: daily event counts broken down by event type
select
    event_date,
    event_type,
    count(*) as event_count
from {{ ref('stg_events') }}
group by event_date, event_type
order by event_date desc, event_count desc
