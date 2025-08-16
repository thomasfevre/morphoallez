-- models/marts/agg_steakhouse_user_activity.sql
-- User-level aggregated activity across Steakhouse vaults

{{ config(materialized='table') }}

SELECT
    actor_address AS user_address,
    vault_asset,
    vault_address,
    
    -- Activity summary
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN transaction_type = 'deposit' THEN 1 ELSE 0 END) AS total_deposits,
    SUM(CASE WHEN transaction_type = 'withdrawal' THEN 1 ELSE 0 END) AS total_withdrawals,
    
    -- First and last activity
    MIN(approximate_timestamp) AS first_transaction_time,
    MAX(approximate_timestamp) AS last_transaction_time,
    MIN(transaction_date) AS first_transaction_date,
    MAX(transaction_date) AS last_transaction_date,
    
    -- Total volumes
    SUM(CASE WHEN transaction_type = 'deposit' THEN assets_normalized ELSE 0 END) AS total_deposited_assets,
    SUM(CASE WHEN transaction_type = 'withdrawal' THEN assets_normalized ELSE 0 END) AS total_withdrawn_assets,
    SUM(net_assets_flow) AS net_assets_position,
    
    -- Share volumes
    SUM(CASE WHEN transaction_type = 'deposit' THEN shares_normalized ELSE 0 END) AS total_deposited_shares,
    SUM(CASE WHEN transaction_type = 'withdrawal' THEN shares_normalized ELSE 0 END) AS total_withdrawn_shares,
    SUM(net_shares_flow) AS net_shares_position,
    
    -- Average transaction sizes
    AVG(CASE WHEN transaction_type = 'deposit' THEN assets_normalized END) AS avg_deposit_size,
    AVG(CASE WHEN transaction_type = 'withdrawal' THEN assets_normalized END) AS avg_withdrawal_size,
    
    -- User classification
    CASE 
        WHEN SUM(net_assets_flow) > 0 THEN 'Net Depositor'
        WHEN SUM(net_assets_flow) < 0 THEN 'Net Withdrawer'
        ELSE 'Balanced'
    END AS user_type,
    
    -- Activity level
    CASE 
        WHEN COUNT(*) >= 10 THEN 'High Activity'
        WHEN COUNT(*) >= 3 THEN 'Medium Activity'
        ELSE 'Low Activity'
    END AS activity_level

FROM {{ ref('fct_steakhouse_vault_flows') }}
GROUP BY 
    actor_address,
    vault_asset,
    vault_address
ORDER BY 
    total_deposited_assets DESC
