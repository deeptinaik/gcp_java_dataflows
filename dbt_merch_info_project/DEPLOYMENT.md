# DBT Deployment Guide

## Quick Start

### 1. Environment Setup
```bash
# Install DBT with Snowflake adapter
pip install dbt-snowflake

# Set environment variables
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=your_role
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_WAREHOUSE=your_warehouse
```

### 2. Project Initialization
```bash
cd dbt_merch_info_project
dbt debug  # Test connection
```

### 3. Execution Order

#### Option 1: Full Run
```bash
# Run with variables
dbt run --vars '{"source_etlbatchid": 12345, "etlbatchid": 67890}'

# Run tests
dbt test
```

#### Option 2: Staged Execution
```bash
# 1. Run staging models
dbt run --select staging --vars '{"source_etlbatchid": 12345, "etlbatchid": 67890}'

# 2. Run intermediate models
dbt run --select intermediate --vars '{"source_etlbatchid": 12345, "etlbatchid": 67890}'

# 3. Run marts models
dbt run --select marts --vars '{"source_etlbatchid": 12345, "etlbatchid": 67890}'

# 4. Run specific models for updates
dbt run --select purge_flag_updates current_ind_updates max_etlbatchid_updates --vars '{"source_etlbatchid": 12345, "etlbatchid": 67890}'
```

### 4. Model Dependencies
```
stg_dimension_merch_info_temp
    ↓
int_youcm_gppn_temp
    ↓
master_merch_info ← purge_flag_updates
    ↓
current_ind_updates
    ↓
max_etlbatchid_updates
```

### 5. Target Environments

#### Development
```bash
dbt run --target dev
```

#### Staging
```bash
dbt run --target staging
```

#### Production
```bash
dbt run --target prod
```

## Equivalent BigQuery Script Execution Mapping

| Original BigQuery Operation | DBT Command |
|----------------------------|-------------|
| `bq_run "${load_temp_table}"` | `dbt run --select stg_dimension_merch_info_temp` |
| `bq_run "${gpn_temp_table_load}"` | `dbt run --select int_youcm_gppn_temp` |
| `bq_run "${insert_table_data}"` | `dbt run --select master_merch_info` |
| `bq_run "${query_2}"` | `dbt run --select purge_flag_updates` (post-hook) |
| `bq_run "${query_3}"` | `dbt run --select purge_flag_updates` (post-hook) |
| `upsert_run "${update_current_ind}"` | `dbt run --select current_ind_updates` |
| `upsert_run "${get_max_etlbatchid}"` | `dbt run --select max_etlbatchid_updates` |

## Monitoring & Troubleshooting

### Check Model Status
```bash
# See what would run
dbt run --dry-run

# Run with verbose logging
dbt run --debug

# Run specific model with full refresh
dbt run --select master_merch_info --full-refresh
```

### Data Quality Checks
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select master_merch_info

# Run custom tests
dbt test --select test_type:custom
```

### Documentation
```bash
# Generate documentation
dbt docs generate

# Serve documentation locally
dbt docs serve --port 8080
```

## Performance Optimization

### For Large Data Volumes
```bash
# Use multiple threads
dbt run --threads 8

# Run incrementally
dbt run --select master_merch_info --vars '{"source_etlbatchid": 12345, "etlbatchid": 67890}'
```

### Cluster Key Recommendations (for Snowflake)
- `master_merch_info`: Cluster on `merchant_number`, `etlbatchid`
- `int_youcm_gppn_temp`: Cluster on `merchant_number`

## Error Handling

### Common Issues and Solutions

1. **Connection Issues**
   ```bash
   dbt debug  # Check connection
   ```

2. **Variable Issues**
   ```bash
   # Check if variables are set
   dbt run --vars '{"source_etlbatchid": 0, "etlbatchid": 0}' --dry-run
   ```

3. **Schema Issues**
   ```bash
   # Drop and recreate schemas
   dbt run --full-refresh
   ```

4. **Dependency Issues**
   ```bash
   # Check model dependencies
   dbt deps
   dbt list --resource-type model
   ```

## Continuous Integration Example

### GitHub Actions
```yaml
name: DBT CI/CD
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.8
      - name: Install dependencies
        run: |
          pip install dbt-snowflake
      - name: Run DBT tests
        run: |
          cd dbt_merch_info_project
          dbt debug
          dbt run --target staging
          dbt test
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
          SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
```