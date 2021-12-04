{{ config(materialized='incremental',unique_key='_airbyte_ab_id',incremental_strategy='insert_overwrite') }}

with dbt_c_stats as
(    
   
        select cast(_airbyte_ab_id as uuid) as _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, 
            _airbyte_ga_campaign_stats_hashid, to_date(ga_date,'YYYYMMDD'), ga_hour, ga_users, ga_medium,
            ga_source, ga_campaign, ga_newusers, ga_sessions, ga_pageviews, ga_dimension2, 
            cast(ga_dimension6 as uuid) as ga_dimension6, ga_transactions, ga_sessionduration, ga_transactionrevenue
            from dbt_live_trans.ga_campaign_stats
            where _airbyte_normalized_at > (select max(_airbyte_normalized_at) from dbt_live_trans.campaign_stats)
   
)

select * from dbt_c_stats 