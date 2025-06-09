# Customer Analytics DBT Project

This DBT project converts the original BigQuery customer analytics query into a well-structured, modular DBT project that follows best practices for maintainability, performance, and scalability.

## Project Structure

```
├── dbt_project.yml          # Main project configuration
├── profiles.yml             # Connection profiles for different environments
├── models/
│   ├── staging/
│   │   └── stg_orders.sql   # Clean and standardized raw orders data
│   ├── intermediate/
│   │   ├── int_sales_aggregated.sql     # Sales data with item arrays
│   │   ├── int_customer_totals.sql      # Customer spending totals
│   │   └── int_ranked_orders.sql        # Orders ranked by date
│   ├── marts/
│   │   └── customer_analytics.sql       # Final customer analytics
│   └── schema.yml           # Model documentation and tests
├── macros/
│   └── get_customer_tier.sql           # Reusable customer tier logic
├── seeds/
│   └── customer_tier_definitions.csv   # Reference data for customer tiers
└── tests/
    └── test_customer_tier_logic.sql     # Custom business logic validation
```

## Models Overview

### Staging Layer
- **stg_orders**: Clean, standardized access to raw orders data with basic quality filters

### Intermediate Layer
- **int_sales_aggregated**: Replicates the 'sales' CTE, groups orders and creates structured item arrays
- **int_customer_totals**: Replicates the 'customer_totals' CTE, calculates total spent and order count per customer
- **int_ranked_orders**: Replicates the 'ranked_orders' CTE, ranks orders by date for each customer

### Marts Layer
- **customer_analytics**: Final model combining all intermediate models to provide comprehensive customer insights

## Key Features

1. **Exact Business Logic Replication**: All transformations, aggregations, and conditions from the original BigQuery are precisely maintained
2. **Modular Architecture**: Complex query broken into logical, maintainable components
3. **Reusable Macros**: Customer tier classification logic abstracted into a reusable macro
4. **Data Quality Tests**: Comprehensive tests ensure data integrity and business rule validation
5. **Multi-Environment Support**: Configured for dev, staging, and production environments
6. **Performance Optimization**: Appropriate materialization strategies for each layer

## Usage

### Prerequisites
- DBT CLI installed
- BigQuery credentials configured
- Access to source BigQuery dataset

### Running the Project

1. **Install dependencies**:
   ```bash
   dbt deps
   ```

2. **Run staging models**:
   ```bash
   dbt run --models staging
   ```

3. **Run all models**:
   ```bash
   dbt run
   ```

4. **Run tests**:
   ```bash
   dbt test
   ```

5. **Generate documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

### Environment Configuration

Update variables in `dbt_project.yml` or use command-line overrides:
```bash
dbt run --vars '{"source_project": "your-project", "source_dataset": "your-dataset"}'
```

## Business Logic

The project maintains the exact same business logic as the original BigQuery:

1. **Order Aggregation**: Groups orders and creates arrays of item details
2. **Customer Totals**: Calculates total spending and order count per customer
3. **Order Ranking**: Ranks orders by date to identify recent purchases
4. **Customer Segmentation**: Classifies customers into VIP (>$10K), Preferred ($5K-$10K), or Standard (≤$5K) tiers
5. **Last 3 Orders**: Aggregates the most recent 3 orders for each customer

## Testing Strategy

- **Schema Tests**: Validate data types, null values, and uniqueness
- **Business Logic Tests**: Custom tests ensure customer tier logic accuracy
- **Data Quality Tests**: Range checks and accepted values validation
- **Referential Integrity**: Ensure proper joins between models

## Performance Considerations

- **Staging**: Views for flexibility and minimal storage
- **Intermediate**: Ephemeral models for optimal query performance
- **Marts**: Tables for fast end-user access
- **Incremental Loading**: Can be configured for large datasets (unique_key already set)

This DBT project provides a production-ready, scalable solution that maintains 100% accuracy with the original BigQuery logic while adding the benefits of modularity, testing, and documentation.