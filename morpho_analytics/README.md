# Steakhouse Vault Analytics

This dbt project provides analytics for the Steakhouse USDC and WETH MetaMorpho vaults, focusing on deposit and withdrawal activity.

## Project Structure

### Staging Models (`models/staging/`)
- `stg_steakhouse_usdc__deposits.sql` - Cleaned USDC vault deposit events
- `stg_steakhouse_usdc__withdrawals.sql` - Cleaned USDC vault withdrawal events  
- `stg_steakhouse_weth__deposits.sql` - Cleaned WETH vault deposit events
- `stg_steakhouse_weth__withdrawals.sql` - Cleaned WETH vault withdrawal events

### Mart Models (`models/marts/`)
- `fct_steakhouse_vault_flows.sql` - Unified fact table of all vault flows
- `agg_steakhouse_daily_vault_summary.sql` - Daily aggregated vault activity
- `agg_steakhouse_user_activity.sql` - User-level activity aggregations

### Key Features
- ✅ **Decimal Normalization**: Proper handling of USDC (6 decimals) and WETH (18 decimals)
- ✅ **Clean Naming**: Clear staging (`stg_`) and fact/aggregate (`fct_`, `agg_`) prefixes
- ✅ **Comprehensive Documentation**: Full schema documentation with tests
- ✅ **Analytics Ready**: Pre-built queries for common dashboard use cases

### Getting Started

1. Ensure your ClickHouse connection is configured in `profiles.yml`
2. Run the staging models first:
   ```bash
   dbt run --select staging
   ```
3. Then run the mart models:
   ```bash
   dbt run --select marts
   ```

### Sample Analytics

See `STEAKHOUSE_ANALYTICS_QUERIES.md` for ready-to-use dashboard queries including:
- Daily volume trends
- User behavior analysis  
- Transaction size distributions
- Cross-vault user activity

### Data Quality

All models include comprehensive tests for:
- Unique constraints on key identifiers
- Not null checks on critical fields
- Accepted values validation for categoricals
- Data freshness monitoring

Run tests with:
```bash
dbt test
```
