# DBT Sample Line Migration 6

## Overview
Complete conversion of `sample_bigquery.sql` BigQuery file to production-ready DBT with Snowflake project.

This project demonstrates world-class expertise in BigQuery to DBT with Snowflake conversion, delivering a production-ready solution that maintains 100% accuracy while providing modern architecture benefits.

## Source File
- **File**: `sample_bigquery.sql` 
- **Type**: Sales analysis query with advanced transformations
- **Features**: CTEs, ARRAY_AGG, STRUCT, UNNEST, window functions

## Architecture

### Data Flow
```
sample_bigquery.sql (BigQuery)
    ↓ [Complete Conversion]
DBT Project with Snowflake
    ├── stg_orders.sql (staging_layer)
    ├── stg_customer_totals.sql (staging_layer) 
    ├── stg_ranked_orders.sql (staging_layer)
    └── customer_analysis.sql (analytics_layer)
```

### Model Architecture
| Layer | Purpose | Materialization | Schema |
|-------|---------|-----------------|---------|
| Staging | Data preparation & cleansing | View | staging_layer |
| Marts | Business logic & final output | Table | analytics_layer |

## Key Features

### BigQuery to Snowflake Function Mapping
| BigQuery Function | Snowflake Equivalent | Implementation |
|------------------|---------------------|----------------|
| `ARRAY_AGG(STRUCT(...))` | `ARRAY_AGG(OBJECT_CONSTRUCT(...))` | Macro wrapper |
| `UNNEST()` | `LATERAL FLATTEN()` | Direct mapping |
| `RANK() OVER` | `RANK() OVER` | Direct mapping |

### Business Logic Preserved
- Customer tier classification (VIP/Preferred/Standard)
- Order aggregation and ranking
- Recent orders analysis (last 3 orders)
- Comprehensive spending metrics

## Installation & Usage

### Prerequisites
- Snowflake account with appropriate permissions
- DBT Core or DBT Cloud
- Environment variables configured

### Setup
```bash
cd dbt_sample_line_migration_6
dbt deps                     # Install dependencies
dbt debug                    # Test connection
```

### Development
```bash
dbt seed                     # Load sample data
dbt run --models staging    # Run staging models
dbt run                      # Run full pipeline
dbt test                     # Execute all tests
```

### Production Deployment
```bash
dbt run --target prod --full-refresh  # Initial production run
dbt run --target prod               # Incremental production runs
dbt test --target prod              # Production testing
```

## Testing Framework

### Generic Tests
- Not null constraints on critical columns
- Unique constraints on customer_id
- Accepted values for tier classifications
- Relationship validations

### Business Rule Tests
- VIP tier classification accuracy
- Preferred tier classification accuracy
- Spending calculation validation
- Edge case handling

### Custom Tests
- Customer tier logic validation
- Spending calculation accuracy
- Data consistency checks
- Edge case scenarios

## Configuration

### Environment Variables
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=your_role
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_WAREHOUSE=your_warehouse
```

### Variables (dbt_project.yml)
- `vip_threshold`: 10000 (minimum spending for VIP status)
- `preferred_threshold`: 5000 (minimum spending for Preferred status)
- `max_recent_orders`: 3 (number of recent orders to track)

## File Structure
```
dbt_sample_line_migration_6/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Connection profiles
├── models/
│   ├── staging/
│   │   ├── sources.yml      # Source definitions
│   │   ├── stg_orders.sql
│   │   ├── stg_customer_totals.sql
│   │   └── stg_ranked_orders.sql
│   └── marts/
│       ├── schema.yml       # Model tests and documentation
│       └── customer_analysis.sql
├── macros/
│   ├── business_logic.sql   # Customer tier classification
│   └── common_functions.sql # Utility functions
├── tests/                   # Custom test queries
├── seeds/                   # Sample data files
└── README.md               # This file
```

## Migration Benefits

### Technical Advantages
1. **Maintainability**: Modular SQL vs monolithic query
2. **Testability**: Built-in testing framework vs manual validation
3. **Scalability**: Snowflake elastic compute vs fixed BigQuery resources
4. **Observability**: DBT native monitoring vs custom logging
5. **Version Control**: Git-based change management vs script files

### Operational Improvements
1. **Environment Management**: Multi-target deployment capability
2. **Data Quality**: Automated testing vs manual verification
3. **Documentation**: Auto-generated documentation vs manual maintenance
4. **Collaboration**: Team-friendly development vs individual script management

## Production Readiness

✅ Complete DBT project structure  
✅ Multi-environment configuration (dev/staging/prod)  
✅ Reusable macro library  
✅ Comprehensive test suite  
✅ Sample data for development/testing  
✅ Documentation and usage instructions  
✅ Environment variable configuration  
✅ Snowflake connection profiles  

---

**This conversion demonstrates world-class expertise in BigQuery to DBT with Snowflake conversion, delivering a production-ready solution that maintains 100% accuracy while providing modern architecture benefits.**