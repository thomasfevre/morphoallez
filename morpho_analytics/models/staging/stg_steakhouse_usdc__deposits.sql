-- models/staging/stg_steakhouse_usdc__deposits.sql
-- Staging model for Steakhouse USDC vault deposits

{{ config(materialized='view') }}

SELECT
    rindexer_id AS deposit_id,
    contract_address AS vault_address,
    sender AS depositor_address,
    owner AS beneficiary_address,
    assets AS deposited_assets_raw,
    shares AS deposited_shares_raw,
    -- Convert to normalized amounts (USDC has 6 decimals)
    assets / POW(10, 6) AS deposited_assets_normalized,
    shares / POW(10, 6) AS deposited_shares_normalized,
    tx_hash,
    block_number,
    block_hash,
    tx_index,
    log_index,
    -- Convert Ethereum block number to timestamp
    -- Using reference: Block 21439512 = 2024-12-19 22:20:23 UTC (1734648023)
    -- Ethereum blocks average ~12 seconds since the merge
    toDateTime(1734648023 + (block_number - 21439512) * 12) AS approximate_timestamp,
    'USDC' AS vault_asset,
    'deposit' AS transaction_type
FROM
    {{ source('morpho_raw', 'morpho_blue_etl_meta_morpho__steakhouse__usdc_deposit') }}
WHERE 
    _peerdb_is_deleted = false
