{{ config(materialized='ephemeral') }}

with stg_btc as (
    select *
    from {{ ref('stg_btc__btc') }}
),

filtered as (
    select *
    from stg_btc
    where is_coinbase = false
)

select *
from filtered