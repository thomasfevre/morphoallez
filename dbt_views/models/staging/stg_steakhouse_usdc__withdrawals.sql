-- models/staging/stg_steakhouse_usdc__withdrawals.sql
-- Staging model for Steakhouse USDC vault withdrawals

{{ config(materialized='view') }}

SELECT
    rindexer_id AS withdrawal_id,
    contract_address AS vault_address,
    sender AS withdrawer_address,
    receiver AS recipient_address,
    owner AS owner_address,
    assets AS withdrawn_assets_raw,
    shares AS withdrawn_shares_raw,
    -- Convert to normalized amounts (USDC has 6 decimals)
    assets / POW(10, 6) AS withdrawn_assets_normalized,
    shares / POW(10, 6) AS withdrawn_shares_normalized,
    tx_hash,
    block_number,
    block_hash,
    tx_index,
    log_index,
    -- Convert Ethereum block number to timestamp
    -- Using reference: Block 21439512 = 2024-12-19 22:20:23 UTC (1734648023)
    -- Ethereum blocks average ~12 seconds since the merge
    toDateTime(1734648023 + (block_number - 21439512) * 12) AS approximate_timestamp,
    toLowCardinality('USDC') AS vault_asset,
    toLowCardinality('withdrawal') AS transaction_type
FROM
    {{ source('morpho_raw', 'morpho_blue_etl_meta_morpho__steakhouse__usdc_withdraw') }}
WHERE 
    _peerdb_is_deleted = false
