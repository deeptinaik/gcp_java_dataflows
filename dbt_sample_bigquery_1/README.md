# Sample BigQuery to DBT with Snowflake Migration

## Project Overview
This project demonstrates the complete migration of `sample_bigquery_1.sql` from BigQuery to a production-ready DBT with Snowflake project.

## Source Analysis
The original BigQuery SQL performs customer analysis with:
- Complex array aggregations (ARRAY_AGG with STRUCT)
- UNNEST operations for array flattening
- Window functions for ranking
- Customer tier classification based on spending

## Migration Architecture

### Model Structure
```
├── staging/
│   ├── stg_sales.sql                 # Sales data with item aggregation
│   ├── stg_customer_totals.sql       # Customer spending totals
│   └── stg_ranked_orders.sql         # Ranked order analysis
└── marts/
    └── mart_customer_analysis.sql    # Final customer analysis output
```

### Key Conversions
| BigQuery Function | Snowflake Equivalent | Implementation |
|------------------|---------------------|----------------|
| `ARRAY_AGG(STRUCT(...))` | `ARRAY_AGG(OBJECT_CONSTRUCT(...))` | Direct conversion |
| `UNNEST(array)` | `LATERAL FLATTEN(INPUT => array)` | Lateral join |
| Window functions | Window functions | Direct mapping |

### Materialization Strategy
- **Staging Models**: `view` for minimal storage overhead
- **Mart Models**: `table` for optimal query performance

## Configuration
- Multi-environment support (dev/staging/prod)
- Environment variable-based credentials
- Configurable customer tier thresholds

## Testing Framework
- Generic tests for data quality
- Custom business logic tests
- Edge case validation
- Customer tier classification validation

## Usage
1. Set environment variables for Snowflake connection
2. Run `dbt deps` to install dependencies
3. Run `dbt seed` if using seed data
4. Run `dbt run` to build models
5. Run `dbt test` to validate data quality

## Business Logic Preserved
- Customer spending aggregation
- Order ranking by date
- Customer tier classification (VIP > $10k, Preferred > $5k)
- Last 3 orders tracking per customer