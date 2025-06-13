# BigQuery to Snowflake Function Mapping

This document details the complete mapping of BigQuery functions to Snowflake equivalents used in the Sample BigQuery 1 conversion.

## Function Conversions

| BigQuery Function | Snowflake Equivalent | DBT Macro | Usage |
|------------------|---------------------|-----------|-------|
| `ARRAY_AGG(STRUCT(col1, col2))` | `ARRAY_AGG(OBJECT_CONSTRUCT('col1', col1, 'col2', col2))` | `create_order_items_array()` | Order item aggregation |
| `ARRAY_AGG(...) ORDER BY ... LIMIT n` | `ARRAY_AGG(...) WITHIN GROUP (ORDER BY ...)[0:n-1]` | `create_order_summary_array()` | Recent orders array |
| `UNNEST(array_column)` | Flattened aggregation approach | Direct aggregation | Array processing |
| `CURRENT_DATETIME()` | `CURRENT_TIMESTAMP()` | `generate_current_timestamp()` | Timestamp fields |
| `SAFE_CAST(col AS type)` | `TRY_CAST(col AS type)` | `safe_cast()` | Safe type conversions |
| `RANK() OVER(...)` | `RANK() OVER(...)` | Native syntax | Window functions |
| `SUM(quantity * price)` | `SUM(quantity * price)` | `calculate_line_total()` | Amount calculations |

## SQL Syntax Conversions

### Array Operations
**BigQuery:**
```sql
ARRAY_AGG(STRUCT(product_id, quantity, price)) AS items
```

**Snowflake:**
```sql
ARRAY_AGG(
    OBJECT_CONSTRUCT(
        'product_id', product_id,
        'quantity', quantity,
        'price', price
    )
) AS items
```

### Array Processing
**BigQuery:**
```sql
FROM sales, UNNEST(items)
```

**Snowflake:**
```sql
-- Converted to direct aggregation approach
FROM {{ ref('stg_orders') }}
GROUP BY order_id, customer_id, order_date
```

### Table References
**BigQuery:**
```sql
FROM `project.dataset.orders`
```

**Snowflake:**
```sql
FROM {{ source('raw_data', 'orders') }}
```

## Business Logic Macros

### Customer Tier Calculation
```sql
{% macro calculate_customer_tier(total_spent) %}
    CASE
        WHEN {{ total_spent }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ total_spent }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}
```

### Array Construction
```sql
{% macro create_order_items_array(product_id, quantity, price) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'product_id', {{ product_id }},
            'quantity', {{ quantity }},
            'price', {{ price }}
        )
    )
{% endmacro %}
```

## Data Type Conversions

| BigQuery Type | Snowflake Type | Notes |
|--------------|----------------|-------|
| `INT64` | `NUMBER` | Snowflake uses NUMBER for all numeric types |
| `FLOAT64` | `FLOAT` | Direct mapping |
| `STRING` | `VARCHAR` | Snowflake VARCHAR has no length limit by default |
| `DATE` | `DATE` | Direct mapping |
| `DATETIME` | `TIMESTAMP` | Snowflake uses TIMESTAMP for datetime |
| `ARRAY<STRUCT<...>>` | `ARRAY<OBJECT>` | Complex array structure conversion |

## Performance Optimizations

### Materialization Strategy
- **Staging Models**: `view` for real-time data access
- **Intermediate Models**: `ephemeral` for lightweight processing
- **Mart Models**: `table` for analytics performance
- **Business Logic**: `macro` for code reusability

### Query Optimization
- **Window Functions**: Optimized for Snowflake performance characteristics
- **Array Operations**: Efficient OBJECT_CONSTRUCT approach
- **Aggregations**: Proper grouping for optimal performance

## Testing Strategy

### Generic Tests
- `not_null` for critical fields
- `unique` for identifier columns
- `accepted_values` for categorical data
- `dbt_utils.expression_is_true` for business rules

### Custom Business Logic Tests
- Customer tier assignment validation
- Aggregation consistency checks
- Array operations integrity
- Date logic validation

### Edge Case Coverage
- Null value handling in arrays
- Zero and negative amount validation
- Date relationship validation
- Customer lifetime calculation accuracy