-- Test composite key uniqueness for stg_btc__btc (hash_key + output_index)
-- Each combination of transaction hash and output index should be unique

select
    hash_key,
    output_index,
    count(*) as occurrences
from {{ ref('stg_btc__btc') }}
group by hash_key, output_index
having count(*) > 1
