{{ config(materialized='incremental',unique_key='_airbyte_ab_id',incremental_strategy='insert_overwrite') }}

with dbt_em_stats as
(    
   
        select cast(_airbyte_ab_id as uuid) as _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, 
            _airbyte_ga_ecommerce__metrics_stats_hashid, to_date(ga_date,'YYYYMMDD'), ga_hour, ga_users, 
            ga_newusers, ga_sessions, ga_dimension2, cast(ga_dimension6 as uuid) as ga_dimension6, ga_affiliation,
            ga_itemrevenue, ga_productname, ga_itemquantity, ga_transactionid, ga_sessionduration,
            ga_uniquepurchases
            from dbt_live_trans.ga_ecommerce__metrics_stats
            where _airbyte_normalized_at > (select max(_airbyte_normalized_at) from dbt_live_trans.ecommerce__metrics_stats)
   
)

select * from dbt_em_stats 