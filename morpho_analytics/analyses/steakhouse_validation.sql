-- validation_queries.sql
-- Quick validation queries for the Steakhouse vault analytics

-- 1. Check staging models have data
SELECT 
    'USDC Deposits' as model_name,
    COUNT(*) as record_count,
    MIN(approximate_timestamp) as earliest_timestamp,
    MAX(approximate_timestamp) as latest_timestamp
FROM {{ ref('stg_steakhouse_usdc__deposits') }}

UNION ALL

SELECT 
    'USDC Withdrawals' as model_name,
    COUNT(*) as record_count,
    MIN(approximate_timestamp) as earliest_timestamp,
    MAX(approximate_timestamp) as latest_timestamp
FROM {{ ref('stg_steakhouse_usdc__withdrawals') }}

UNION ALL

SELECT 
    'WETH Deposits' as model_name,
    COUNT(*) as record_count,
    MIN(approximate_timestamp) as earliest_timestamp,
    MAX(approximate_timestamp) as latest_timestamp
FROM {{ ref('stg_steakhouse_weth__deposits') }}

UNION ALL

SELECT 
    'WETH Withdrawals' as model_name,
    COUNT(*) as record_count,
    MIN(approximate_timestamp) as earliest_timestamp,
    MAX(approximate_timestamp) as latest_timestamp
FROM {{ ref('stg_steakhouse_weth__withdrawals') }}

ORDER BY model_name;
