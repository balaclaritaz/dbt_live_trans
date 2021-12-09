{{ config(materialized='incremental',unique_key='_airbyte_ab_id',incremental_strategy='insert_overwrite') }}

with dbt_ts_stats as
(    
   
        select
        -- cast(_airbyte_ab_id as uuid) as _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, 
           -- _airbyte_ga_traffic_source_stats_hashid,
            to_date(ga_date,'YYYYMMDD'), ga_dateHourMinute, ga_users, 
            ga_medium, ga_source, ga_newusers, ga_sessions, ga_pageviews, ga_dimension2, 
            cast(ga_dimension6 as uuid) as ga_dimension6, ga_transactions, ga_channelgrouping, ga_sessionduration,
            ga_transactionrevenue, 
            case when ga_dimension6 is null then ga_dimension2 when ga_dimension2 is null then ga_dimension6 
            else concat(ga_dimension6,'_',ga_dimension2,'_',ga_datehourminute) end as dimension6_dimension2_dhm
            from google_analytics.ga_traffic_source_stats
          --  where _airbyte_normalized_at > (select max(_airbyte_normalized_at) from dbt_live_trans.traffic_source_stats)
   
)

select * from dbt_ts_stats 