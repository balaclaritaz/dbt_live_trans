{{ config(materialized='incremental',unique_key='_airbyte_ab_id',incremental_strategy='insert_overwrite') }}

with dbt_s_stats as
(    
   
        select 
      --  cast(_airbyte_ab_id as uuid) as _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, 
       --     _airbyte_ga_session_stats_hashid,
             to_date(ga_date,'YYYYMMDD'), ga_hits, ga_dateHourMinute, ga_users, ga_bounces,
            ga_newusers, ga_sessions, ga_pageviews, ga_dimension2, cast(ga_dimension6 as uuid) as ga_dimension6,
            ga_sessionduration, 
            case when ga_dimension6 is null then ga_dimension2 when ga_dimension2 is null then ga_dimension6 
            else concat(ga_dimension6,'_',ga_dimension2,'_',ga_datehourminute) end as dimension6_dimension2_dhm
            from google_analytics.ga_session_stats
         --   where _airbyte_normalized_at > (select max(_airbyte_normalized_at) from dbt_live_trans.session_stats)
   
)

select * from dbt_s_stats 