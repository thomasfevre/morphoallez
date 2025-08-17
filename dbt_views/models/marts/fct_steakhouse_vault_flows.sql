-- models/marts/fct_steakhouse_vault_flows.sql
-- Fact table combining all Steakhouse vault deposit and withdrawal flows

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(transaction_date, vault_asset, block_number)',
    partition_by='toYYYYMM(transaction_date)'
) }}

WITH usdc_deposits AS (
    SELECT
        deposit_id AS event_id,
        vault_address,
        depositor_address AS actor_address,
        beneficiary_address,
        NULL AS recipient_address,
        deposited_assets_raw AS assets_raw,
        deposited_shares_raw AS shares_raw,
        deposited_assets_normalized AS assets_normalized,
        deposited_shares_normalized AS shares_normalized,
        tx_hash,
        block_number,
        block_hash,
        tx_index,
        log_index,
        approximate_timestamp,
        vault_asset,
        transaction_type,
        'inflow' AS flow_direction,
        1 AS flow_multiplier  -- Positive for deposits
    FROM {{ ref('stg_steakhouse_usdc__deposits') }}
),

usdc_withdrawals AS (
    SELECT
        withdrawal_id AS event_id,
        vault_address,
        withdrawer_address AS actor_address,
        owner_address AS beneficiary_address,
        recipient_address,
        withdrawn_assets_raw AS assets_raw,
        withdrawn_shares_raw AS shares_raw,
        withdrawn_assets_normalized AS assets_normalized,
        withdrawn_shares_normalized AS shares_normalized,
        tx_hash,
        block_number,
        block_hash,
        tx_index,
        log_index,
        approximate_timestamp,
        vault_asset,
        transaction_type,
        'outflow' AS flow_direction,
        -1 AS flow_multiplier  -- Negative for withdrawals
    FROM {{ ref('stg_steakhouse_usdc__withdrawals') }}
),

weth_deposits AS (
    SELECT
        deposit_id AS event_id,
        vault_address,
        depositor_address AS actor_address,
        beneficiary_address,
        NULL AS recipient_address,
        deposited_assets_raw AS assets_raw,
        deposited_shares_raw AS shares_raw,
        deposited_assets_normalized AS assets_normalized,
        deposited_shares_normalized AS shares_normalized,
        tx_hash,
        block_number,
        block_hash,
        tx_index,
        log_index,
        approximate_timestamp,
        vault_asset,
        transaction_type,
        'inflow' AS flow_direction,
        1 AS flow_multiplier  -- Positive for deposits
    FROM {{ ref('stg_steakhouse_weth__deposits') }}
),

weth_withdrawals AS (
    SELECT
        withdrawal_id AS event_id,
        vault_address,
        withdrawer_address AS actor_address,
        owner_address AS beneficiary_address,
        recipient_address,
        withdrawn_assets_raw AS assets_raw,
        withdrawn_shares_raw AS shares_raw,
        withdrawn_assets_normalized AS assets_normalized,
        withdrawn_shares_normalized AS shares_normalized,
        tx_hash,
        block_number,
        block_hash,
        tx_index,
        log_index,
        approximate_timestamp,
        vault_asset,
        transaction_type,
        'outflow' AS flow_direction,
        -1 AS flow_multiplier  -- Negative for withdrawals
    FROM {{ ref('stg_steakhouse_weth__withdrawals') }}
)

SELECT
    event_id,
    vault_address,
    actor_address,
    beneficiary_address,
    recipient_address,
    assets_raw,
    shares_raw,
    assets_normalized,
    shares_normalized,
    -- Net flow calculations for aggregations
    assets_normalized * flow_multiplier AS net_assets_flow,
    shares_normalized * flow_multiplier AS net_shares_flow,
    tx_hash,
    block_number,
    block_hash,
    tx_index,
    log_index,
    approximate_timestamp,
    toDate(approximate_timestamp) AS transaction_date,
    vault_asset,
    transaction_type,
    flow_direction,
    flow_multiplier
FROM (
    SELECT * FROM usdc_deposits
    UNION ALL
    SELECT * FROM usdc_withdrawals
    UNION ALL
    SELECT * FROM weth_deposits
    UNION ALL
    SELECT * FROM weth_withdrawals
)
ORDER BY block_number DESC, tx_index, log_index
