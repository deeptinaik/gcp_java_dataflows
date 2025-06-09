# GCP Dataflow to DBT Migration

This DBT project is a complete migration of the GCP Dataflow Java pipelines to SQL-based transformations running on Snowflake.

## Original GCP Dataflow Pipelines

The original codebase contained several Java-based Apache Beam pipelines:

1. **EcommercePipeline** - Real-time purchase event processing with customer tier assignment
2. **FraudDetectionPipeline** - Transaction fraud detection with customer profile enrichment  
3. **SalesDataPipeline** - Batch retail sales data processing
4. **Complex Fraud Detection** - Advanced fraud rules and validation

## DBT Project Structure

```
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Snowflake connection settings
├── models/
│   ├── staging/             # Raw data staging (views)
│   │   ├── stg_purchase_events.sql
│   │   ├── stg_transactions.sql
│   │   ├── stg_customer_profiles.sql
│   │   ├── stg_sales_data.sql
│   │   └── sources.yml      # Source table definitions
│   ├── intermediate/        # Business logic transformations (tables)
│   │   ├── int_purchase_events_enriched.sql
│   │   ├── int_transactions_enriched.sql
│   │   ├── int_sales_enriched.sql
│   │   └── schema.yml       # Model documentation
│   └── marts/              # Final analytical outputs (tables)
│       ├── mart_sales_by_tier.sql
│       ├── mart_suspicious_transactions.sql
│       ├── mart_normal_transactions.sql
│       ├── mart_retail_sales.sql
│       └── schema.yml      # Model documentation
├── macros/                 # Reusable SQL functions
│   ├── assign_customer_tier.sql
│   ├── detect_fraud.sql
│   ├── deduplicate_by_key.sql
│   └── create_time_windows.sql
└── tests/                  # Data quality tests
    ├── test_fraud_detection_logic.sql
    ├── test_customer_tier_logic.sql
    └── test_data_completeness.sql
```

## Migration Mapping

### Java DoFn → SQL Macros

| Java Transform | DBT Macro | Description |
|----------------|-----------|-------------|
| `AddCustomerTierFn` | `assign_customer_tier()` | GOLD/SILVER/BRONZE tier assignment |
| `DeduplicateFn` | `deduplicate_by_key()` | Stateful deduplication logic |
| Fraud detection logic | `detect_fraud()` | Transaction risk assessment |
| `FixedWindows` | `create_time_windows()` | Time-based windowing |

### Pipeline → Model Mapping

| Original Pipeline | DBT Models | Description |
|------------------|------------|-------------|
| `EcommercePipeline` | `stg_purchase_events` → `int_purchase_events_enriched` → `mart_sales_by_tier` | Purchase processing with tier aggregation |
| `FraudDetectionPipeline` | `stg_transactions` + `stg_customer_profiles` → `int_transactions_enriched` → `mart_suspicious_transactions` + `mart_normal_transactions` | Fraud detection and classification |
| `SalesDataPipeline` | `stg_sales_data` → `int_sales_enriched` → `mart_retail_sales` | Retail sales processing |

### Data Flow

1. **Raw Data** (Snowflake tables) → **Staging Models** (light cleaning, typing)
2. **Staging Models** → **Intermediate Models** (business logic, enrichment) 
3. **Intermediate Models** → **Mart Models** (aggregation, final outputs)

## Key Features

### ✅ Complete Business Logic Migration
- Customer tier assignment (GOLD/SILVER/BRONZE)
- Fraud detection using customer profiles
- Data deduplication and enrichment
- Time-based windowing and aggregation

### ✅ Data Quality & Testing
- Source data validation
- Business logic testing  
- Data completeness checks
- Schema enforcement

### ✅ Snowflake Optimization
- Materialization strategies (views for staging, tables for marts)
- Column-level lineage and documentation
- Configurable variables for business rules

### ✅ DBT Best Practices
- Layered architecture (staging → intermediate → marts)
- Reusable macros for common logic
- Comprehensive documentation
- Version control friendly

## Environment Setup

### 1. Install DBT
```bash
pip install dbt-snowflake
```

### 2. Configure Snowflake Connection
Set environment variables:
```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-username" 
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_ROLE="TRANSFORMER"
export SNOWFLAKE_DATABASE="ANALYTICS"
export SNOWFLAKE_WAREHOUSE="TRANSFORMING"
export SNOWFLAKE_SCHEMA="dbt_dev"
```

### 3. Run DBT
```bash
# Install dependencies (if any)
dbt deps

# Run staging models
dbt run --models staging

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```

## Data Sources

The DBT models expect the following raw tables in Snowflake:

| Table | Description | Original Source |
|-------|-------------|-----------------|
| `raw_data.purchase_events` | Purchase events from PubSub | `projects/your-project/topics/purchase-events` |
| `raw_data.transactions` | Transaction events from PubSub | `projects/your-project/topics/transactions` |
| `raw_data.customer_profiles` | Customer lookup data | `your-project:dataset.customer_profiles` |
| `raw_data.sales_data` | Retail sales files | File ingestion |

## Configuration Variables

Customize business logic via `dbt_project.yml` variables:

```yaml
vars:
  fraud_threshold_multiplier: 3          # Fraud detection sensitivity
  customer_tier_gold_threshold: 500      # GOLD tier minimum amount
  customer_tier_silver_threshold: 100    # SILVER tier minimum amount
  window_minutes: 1                      # Aggregation window size
  deduplication_hours: 10                # Deduplication time window
```

## Testing

The project includes comprehensive tests:

- **Source tests**: Data validation on raw tables
- **Model tests**: Business logic validation
- **Custom tests**: End-to-end data quality checks

Run tests:
```bash
dbt test                    # All tests
dbt test --models staging  # Just staging tests
dbt test --models marts    # Just mart tests
```

## Performance Considerations

### Materialization Strategy
- **Staging**: Views (fast, no storage cost)
- **Intermediate**: Tables (optimized for downstream consumption)
- **Marts**: Tables (optimized for analytics queries)

### Incremental Processing
For large datasets, consider converting models to incremental:

```sql
{{ config(materialized='incremental') }}

SELECT * FROM {{ ref('stg_transactions') }}
{% if is_incremental() %}
  WHERE transaction_timestamp > (SELECT MAX(transaction_timestamp) FROM {{ this }})
{% endif %}
```

## Migration Benefits

### ✅ Simplified Architecture
- No complex Java infrastructure
- SQL-based transformations (easier to maintain)
- Version controlled data pipelines

### ✅ Improved Observability  
- Built-in lineage tracking
- Automated documentation
- Data quality monitoring

### ✅ Team Collaboration
- SQL accessible to analysts
- GitOps workflow
- Shared business logic in macros

### ✅ Cost Optimization
- Snowflake's compute scaling
- Pay-per-use model
- Optimized materialization strategies

## Next Steps

1. **Set up CI/CD** - Automated testing and deployment
2. **Add incremental models** - For high-volume tables
3. **Create dashboards** - Connect BI tools to mart tables  
4. **Monitor performance** - Optimize slow-running models
5. **Expand testing** - Add more business logic validation