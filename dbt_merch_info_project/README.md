# Merchant Info DBT Project

This DBT project converts the BigQuery shell script `DEMGPPNT_merch_info_load.sh` into a modular, maintainable DBT project with Snowflake compatibility.

## Project Structure

```
dbt_merch_info_project/
├── dbt_project.yml           # Main project configuration
├── profiles.yml              # Database connection profiles
├── models/
│   ├── staging/              # Staging models (views)
│   │   ├── stg_dimension_merch_info_temp.sql
│   │   └── source_dimension_merch_info.sql
│   ├── intermediate/         # Intermediate transformation models (views)
│   │   └── int_youcm_gppn_temp.sql
│   ├── marts/               # Final output models (tables)
│   │   ├── master_merch_info.sql
│   │   ├── purge_flag_updates.sql
│   │   ├── current_ind_updates.sql
│   │   └── max_etlbatchid_updates.sql
│   ├── sources.yml          # Source table definitions
│   └── schema.yml           # Model tests and documentation
├── macros/                  # Reusable SQL functions
│   ├── generate_uuid.sql
│   ├── hash_md5_base64.sql
│   ├── parse_date_from_string.sql
│   ├── current_datetime.sql
│   └── data_formatting.sql
└── README.md               # This file
```

## DBT Components Mapping

### Original BigQuery Script Operations → DBT Models

1. **load_temp_table** → `stg_dimension_merch_info_temp.sql`
   - Materialization: Table
   - Purpose: Filters source data based on etlbatchid

2. **gpn_temp_table_load** → `int_youcm_gppn_temp.sql`
   - Materialization: Table  
   - Purpose: Complex business logic transformation with change data capture hash

3. **insert_table_data** → `master_merch_info.sql`
   - Materialization: Incremental
   - Purpose: Main target table with MERGE logic using incremental strategy

4. **query_2 & query_3** → `purge_flag_updates.sql`
   - Materialization: Table with post-hook
   - Purpose: Updates purge_flag values

5. **update_current_ind** → `current_ind_updates.sql`
   - Materialization: Incremental with post-hook
   - Purpose: Updates current_ind flags

6. **get_max_etlbatchid** → `max_etlbatchid_updates.sql`
   - Materialization: Table with post-hook
   - Purpose: Updates maximum ETL batch ID

### Macros for Cross-Platform Compatibility

- **generate_uuid()**: Handles UUID generation across BigQuery and Snowflake
- **hash_md5_base64()**: MD5 hashing with Base64 encoding
- **parse_date_from_string()**: Date parsing functions
- **current_datetime()**: Current timestamp functions
- **Data formatting macros**: TRIM, masking, and casting utilities

## Key Conversion Features

### BigQuery → Snowflake Function Mapping
- `GENERATE_UUID()` → `UUID_STRING()`
- `TO_BASE64(MD5())` → `BASE64_ENCODE(MD5())`
- `PARSE_DATE()` → `TO_DATE()`
- `CURRENT_DATETIME()` → `CURRENT_TIMESTAMP()`

### DML → SELECT Query Conversion
- All INSERT, UPDATE, MERGE operations converted to SELECT queries
- Incremental materialization handles MERGE logic
- Post-hooks handle UPDATE operations where necessary

### Complex Array Operations
- BigQuery ARRAY_AGG with STRUCT → Snowflake ARRAY_AGG with OBJECT_CONSTRUCT
- UNPIVOT operations simplified for Snowflake compatibility
- Complex JOIN operations preserved but optimized

## Running the Project

### Prerequisites
1. Snowflake account with appropriate permissions
2. DBT installed with Snowflake adapter
3. Environment variables configured for Snowflake connection

### Environment Variables
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=your_role
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_WAREHOUSE=your_warehouse
```

### Execution Commands
```bash
# Install dependencies
dbt deps

# Test source connections
dbt debug

# Run staging models
dbt run --select staging

# Run intermediate models
dbt run --select intermediate

# Run marts models
dbt run --select marts

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Variables
The project uses variables for ETL batch IDs:
```bash
# Run with specific batch IDs
dbt run --vars '{"source_etlbatchid": 12345, "etlbatchid": 67890}'
```

## Data Quality & Testing

- **Uniqueness tests**: Ensure merchant_number uniqueness
- **Not null tests**: Critical fields must not be null
- **Accepted values tests**: Validate status codes and flags
- **Referential integrity**: Check relationships between models

## Performance Considerations

- **Materialization Strategy**: 
  - Staging: Views (fast, no storage)
  - Intermediate: Views (reusable, no duplication)
  - Marts: Tables/Incremental (optimized for queries)

- **Incremental Models**: Use `unique_key` for optimal merge performance
- **Indexing**: Leverage Snowflake's clustering keys for large tables

## Monitoring & Maintenance

- Use `dbt run --fail-fast` for quick error detection
- Monitor model execution times in DBT logs
- Set up alerts for test failures
- Regular documentation updates with `dbt docs generate`

## Best Practices Implemented

1. **Modularity**: Separated concerns into staging, intermediate, and marts
2. **Reusability**: Created macros for common operations
3. **Testability**: Comprehensive test coverage
4. **Documentation**: Clear model and column descriptions
5. **Performance**: Appropriate materialization strategies
6. **Maintainability**: Clear naming conventions and structure