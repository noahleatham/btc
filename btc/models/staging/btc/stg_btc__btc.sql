with renamed as (
select
    hash_key,
    block_hash,
    block_number,
    block_timestamp,
    fee,
    input_value,
    output_value,
    fee_per_byte,
    is_coinbase,
    outputs
from {{ source('raw', 'btc') }}
)

select
    r.hash_key,
    r.block_hash,
    r.block_number,
    r.block_timestamp,
    r.fee,
    r.input_value,
    r.output_value,
    r.fee_per_byte,
    r.is_coinbase,
    r.outputs,
    f.value:address::string as output_address,
    f.value:index::int as output_index,
    f.value:required_signatures::int as output_required_signatures,
    f.value:script_asm::string as output_script_asm,
    f.value:script_hex::string as output_script_hex,
    f.value:type::string as output_type,
    f.value:value::float as output_individual_value
from renamed r,
lateral flatten(input => r.outputs) f
where f.value:address is not null