{{ config(materialized='table') }}

with transactions as (
    select *
    from {{ ref('stg_btc__transactions') }}
),

filtered as (
    select *
    from transactions
    where output_individual_value > 10
),

aggregated as (
    select
        output_address,
        sum(output_individual_value) as total_output_value,
        count(*) as transaction_count
    from filtered
    group by output_address
),

latest_price as (
    select
        price
    from {{ ref('stg_btc_usd_max') }}
    where snapped_at = (select max(snapped_at) from {{ ref('stg_btc_usd_max') }})
    limit 1
),

enriched as (
    select
        a.output_address,
        a.total_output_value,
        a.transaction_count,
        p.price,
        a.total_output_value * p.price as total_value_usd
    from aggregated a
    cross join latest_price p
)

select *
from enriched