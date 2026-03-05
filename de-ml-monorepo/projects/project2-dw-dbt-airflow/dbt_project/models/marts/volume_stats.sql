-- Row-count snapshot for all pipeline tables; refreshed on every dbt run.
-- Use in Superset SQL Lab to monitor pipeline health at a glance.
select 'raw_events'           as table_name, count(*) as row_count from {{ source('raw', 'raw_events') }}
union all
select 'event_agg',                          count(*) from {{ source('raw', 'event_agg') }}
union all
select 'stg_events',                         count(*) from {{ ref('stg_events') }}
union all
select 'daily_event_metrics',               count(*) from {{ ref('daily_event_metrics') }}
order by table_name
