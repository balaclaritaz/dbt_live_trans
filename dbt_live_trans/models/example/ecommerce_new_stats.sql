{{ config(materialized='incremental',unique_key='_airbyte_ab_id',incremental_strategy='insert_overwrite') }}

with dbt_ecomnew_stats as
(    
   
        select cast(_airbyte_ab_id as uuid) as _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, 
            _airbyte_ga_ecommerce_new_stats_hashid, to_date(ga_date,'YYYYMMDD'), ga_hour, ga_exits, ga_entrances,
            ga_pagetitle, cast(ga_dimension6 as uuid) as ga_dimension6, ga_exitpagepath, ga_avgtimeonpage, 
            ga_secondpagepath, ga_landingpagepath
            from dbt_live_trans.ga_ecommerce_new_stats
            where _airbyte_normalized_at > (select max(_airbyte_normalized_at) from dbt_live_trans.ecommerce_new_stats)
   
)

select * from dbt_ecomnew_stats 