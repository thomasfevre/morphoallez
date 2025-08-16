# Steakhouse Vault Analytics Dashboard Queries

This document contains useful queries for analyzing Steakhouse USDC and WETH vault activity.

## 1. Daily Volume Trends

```sql
-- Daily deposit and withdrawal volumes by vault
SELECT 
    transaction_date,
    vault_asset,
    ROUND(total_deposits_assets) AS daily_deposits,
    ROUND(total_withdrawals_assets) AS daily_withdrawals,
    ROUND(net_assets_flow) AS daily_net_flow,
    deposit_count,
    withdrawal_count,
    unique_users
FROM analytics.agg_steakhouse_daily_vault_summary
ORDER BY transaction_date DESC, vault_asset;
```

## 2. Vault Growth Comparison

```sql
-- Cumulative net flows over time
WITH daily_flows AS (
    SELECT 
        transaction_date,
        vault_asset,
        net_assets_flow
    FROM analytics.agg_steakhouse_daily_vault_summary
)
SELECT 
    transaction_date,
    vault_asset,
    ROUND(net_assets_flow) AS daily_net_flow,
    ROUND(SUM(net_assets_flow) OVER (
        PARTITION BY vault_asset 
        ORDER BY transaction_date 
        ROWS UNBOUNDED PRECEDING
    )) AS cumulative_net_flow
FROM daily_flows
ORDER BY transaction_date DESC, vault_asset;
```

## 3. Top Users Analysis

```sql
-- Top 10 users by total deposited volume across both vaults
SELECT 
    user_address,
    vault_asset,
    ROUND(total_deposited_assets) AS total_deposited_assets,
    ROUND(total_withdrawn_assets) AS total_withdrawn_assets,
    ROUND(net_assets_position) AS net_assets_position,
    total_transactions,
    user_type,
    activity_level,
    first_transaction_date,
    last_transaction_date
FROM analytics.agg_steakhouse_user_activity
ORDER BY total_deposited_assets DESC
LIMIT 10;
```

## 4. Vault Utilization Metrics

```sql
-- Recent activity summary (last 30 days)
SELECT 
    vault_asset,
    COUNT(*) AS active_days,
    SUM(total_transactions) AS total_transactions,
    SUM(unique_users) AS total_unique_users,
    ROUND(SUM(total_deposits_assets)) AS total_volume_deposited,
    ROUND(SUM(total_withdrawals_assets)) AS total_volume_withdrawn,
    ROUND(SUM(net_assets_flow)) AS net_flow_30d,
    ROUND(AVG(total_deposits_assets)) AS avg_daily_deposits,
    ROUND(AVG(total_withdrawals_assets)) AS avg_daily_withdrawals
FROM analytics.agg_steakhouse_daily_vault_summary
WHERE transaction_date >= today() - INTERVAL 30 DAY
GROUP BY vault_asset;
```


## 6. Transaction Size Analysis

```sql
-- Transaction size distribution
SELECT 
    vault_asset,
    transaction_type,
    COUNT(*) AS transaction_count,
    ROUND(MIN(assets_normalized)) AS min_size,
    ROUND(quantileExact(0.25)(assets_normalized)) AS p25_size,
    ROUND(quantileExact(0.5)(assets_normalized)) AS median_size,
    ROUND(quantileExact(0.75)(assets_normalized)) AS p75_size,
    ROUND(MAX(assets_normalized)) AS max_size,
    ROUND(AVG(assets_normalized)) AS avg_size
FROM analytics.fct_steakhouse_vault_flows
GROUP BY vault_asset, transaction_type
ORDER BY vault_asset, transaction_type;
```

## 7. Recent Large Transactions

```sql
-- Large transactions in the last 7 days (>$10k equivalent)
SELECT 
    approximate_timestamp,
    vault_asset,
    transaction_type,
    actor_address,
    ROUND(assets_normalized) AS assets_normalized,
    tx_hash,
    ROUND(CASE vault_asset
        WHEN 'USDC' THEN assets_normalized  -- Already in USD for USDC
        WHEN 'WETH' THEN assets_normalized * 3000  -- Approximate ETH price
    END) AS estimated_usd_value
FROM analytics.fct_steakhouse_vault_flows
WHERE 
    transaction_date >= today() - INTERVAL 7 DAY
    AND (
        (vault_asset = 'USDC' AND assets_normalized > 10000)
        OR (vault_asset = 'WETH' AND assets_normalized > 3.33)  -- ~$10k worth
    )
ORDER BY approximate_timestamp DESC;
```

