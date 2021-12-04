{{ config(materialized='incremental',unique_key='_airbyte_ab_id',incremental_strategy='insert_overwrite') }}

with dbt_demo_stats as
(    
   
        select cast(_airbyte_ab_id as uuid) as _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, 
            _airbyte_ga_demographic_stats_hashid, to_date(ga_date,'YYYYMMDD'), ga_hour, ga_users, ga_bounces,
            ga_newusers, ga_sessions, ga_pageviews, ga_bouncerate, ga_dimension2, 
            cast(ga_dimension6 as uuid) as ga_dimension6, ga_usergender, ga_transactions, ga_useragebracket,
            ga_sessionduration, ga_transactionrevenue
            from dbt_live_trans.ga_demographic_stats
            where _airbyte_normalized_at > (select max(_airbyte_normalized_at) from dbt_live_trans.demographic_stats)
   
)

select * from dbt_demo_stats 