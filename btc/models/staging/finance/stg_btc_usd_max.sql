{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('btc_usd_max') }}
),

casted as (
    select
        price,
        market_cap,
        total_volume,
        try_to_timestamp(snapped_at, 'YYYY-MM-DD HH24:MI:SS UTC') as snapped_at
    from source
)

select *
from casted
