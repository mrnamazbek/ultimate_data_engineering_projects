-- Staging model: clean and type-cast raw_events
select
    id,
    ts::timestamp                as event_ts,
    value                        as event_type,
    user_id,
    ts::date                     as event_date
from {{ source('raw', 'raw_events') }}
where ts is not null
