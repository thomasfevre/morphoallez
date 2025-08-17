-- models/marts/agg_steakhouse_daily_vault_summary.sql
-- Daily aggregated summary of Steakhouse vault activity

{{ config(
    materialized='incremental',
    unique_key=['transaction_date', 'vault_asset'],
    engine='ReplacingMergeTree()',
    order_by='(transaction_date, vault_asset)',
    partition_by='toYYYYMM(transaction_date)'
) }}

SELECT
    transaction_date,
    toLowCardinality(vault_asset) AS vault_asset,
    vault_address,
    
    -- Transaction counts
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN transaction_type = 'deposit' THEN 1 ELSE 0 END) AS deposit_count,
    SUM(CASE WHEN transaction_type = 'withdrawal' THEN 1 ELSE 0 END) AS withdrawal_count,
    
    -- Unique user counts
    COUNT(DISTINCT actor_address) AS unique_users,
    COUNT(DISTINCT CASE WHEN transaction_type = 'deposit' THEN actor_address END) AS unique_depositors,
    COUNT(DISTINCT CASE WHEN transaction_type = 'withdrawal' THEN actor_address END) AS unique_withdrawers,
    
    -- Asset flows (gross amounts)
    SUM(CASE WHEN transaction_type = 'deposit' THEN assets_normalized ELSE 0 END) AS total_deposits_assets,
    SUM(CASE WHEN transaction_type = 'withdrawal' THEN assets_normalized ELSE 0 END) AS total_withdrawals_assets,
    
    -- Share flows (gross amounts)
    SUM(CASE WHEN transaction_type = 'deposit' THEN shares_normalized ELSE 0 END) AS total_deposits_shares,
    SUM(CASE WHEN transaction_type = 'withdrawal' THEN shares_normalized ELSE 0 END) AS total_withdrawals_shares,
    
    -- Net flows (deposits minus withdrawals)
    SUM(net_assets_flow) AS net_assets_flow,
    SUM(net_shares_flow) AS net_shares_flow,
    
    -- Average transaction sizes
    AVG(CASE WHEN transaction_type = 'deposit' THEN assets_normalized END) AS avg_deposit_size_assets,
    AVG(CASE WHEN transaction_type = 'withdrawal' THEN assets_normalized END) AS avg_withdrawal_size_assets,
    
    -- Block range for the day
    MIN(block_number) AS min_block_number,
    MAX(block_number) AS max_block_number

FROM {{ ref('fct_steakhouse_vault_flows') }}
{% if is_incremental() %}
WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
{% endif %}
GROUP BY 
    transaction_date,
    vault_asset,
    vault_address
ORDER BY 
    transaction_date DESC,
    vault_asset
