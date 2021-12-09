{{ config(materialized='incremental',unique_key='_airbyte_ab_id',incremental_strategy='insert_overwrite') }}

with dbt_adw_stats as
(    
   
        select
      --   cast(_airbyte_ab_id as uuid) as _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, 
        --    _airbyte_ga_adwords_stats_hashid, 
            ga_cpc, ga_cpm, ga_ctr, to_date(ga_date,'YYYYMMDD'), ga_dateHourMinute, 
            ga_adcost, ga_adclicks, ga_campaign, ga_impressions, ga_adwordscampaignid, 
            ga_adwordscustomerid, ga_addistributionnetwork
            from google_analytics.ga_adwords_stats
         --   where _airbyte_normalized_at > (select max(_airbyte_normalized_at) from dbt_live_trans.adwords_stats)
   
)

select * from dbt_adw_stats 