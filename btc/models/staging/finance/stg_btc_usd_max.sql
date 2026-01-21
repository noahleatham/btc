{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('btc_usd_max') }}
),

casted as (
    select
        try_to_timestamp(snapped_at, 'YYYY-MM-DD HH24:MI:SS UTC') as snapped_at,
        price,
        market_cap,
        total_volume
    from source
)

select *
from casted