# BigQuery to Snowflake Function Mapping

This document details the complete mapping of BigQuery functions to Snowflake equivalents used in the Sample BigQuery 1 conversion.

## Function Conversions

| BigQuery Function | Snowflake Equivalent | DBT Macro | Usage |
|------------------|---------------------|-----------|--------|
| `ARRAY_AGG(STRUCT(col1, col2, col3))` | `ARRAY_AGG(OBJECT_CONSTRUCT('col1', col1, 'col2', col2, 'col3', col3))` | `array_agg_struct()` | Order item aggregation |
| `UNNEST(array_column)` | `LATERAL FLATTEN(input => array_column)` | `unnest_array()` | Array expansion for joins |
| `RANK() OVER(PARTITION BY col ORDER BY col)` | `RANK() OVER(PARTITION BY col ORDER BY col)` | `rank_over()` | Order ranking |
| `ARRAY_AGG(...ORDER BY...LIMIT n)` | `ARRAY_AGG(...ORDER BY...)[0:n-1]` | `array_agg_struct()` with limit | Limited aggregation |
| `COUNT(DISTINCT col)` | `COUNT(DISTINCT col)` | Direct mapping | Distinct counting |
| `SUM(col1 * col2)` | `SUM(col1 * col2)` | Direct mapping | Mathematical operations |
| `CASE WHEN...END` | `CASE WHEN...END` | `customer_tier_classification()` | Conditional logic |

## Array Operations

### BigQuery ARRAY_AGG with STRUCT
**BigQuery:**
```sql
ARRAY_AGG(STRUCT(product_id, quantity, price)) AS items
```

**Snowflake:**
```sql
ARRAY_AGG(OBJECT_CONSTRUCT(
    'product_id', product_id,
    'quantity', quantity, 
    'price', price
)) AS items
```

### BigQuery UNNEST Operations
**BigQuery:**
```sql
FROM sales, UNNEST(items)
WHERE quantity * price > 100
```

**Snowflake:**
```sql
FROM sales,
     LATERAL FLATTEN(input => sales.items) item_data
WHERE item_data.value:quantity::NUMBER * item_data.value:price::NUMBER > 100
```

### BigQuery Array Aggregation with ORDER BY and LIMIT
**BigQuery:**
```sql
ARRAY_AGG(STRUCT(order_id, order_total, order_date) ORDER BY order_date DESC LIMIT 3)
```

**Snowflake:**
```sql
ARRAY_AGG(OBJECT_CONSTRUCT(
    'order_id', order_id,
    'order_total', order_total,
    'order_date', order_date
) ORDER BY order_date DESC)[0:2]
```

## SQL Syntax Conversions

### JSON/Object Access
**BigQuery:**
```sql
-- Not applicable - uses STRUCT directly
```

**Snowflake:**
```sql
item_data.value:quantity::NUMBER
item_data.value:price::NUMBER
```

### Window Functions
**BigQuery:**
```sql
RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
```

**Snowflake:**
```sql
RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
```

### Table References
**BigQuery:**
```sql
FROM `project.dataset.orders`
```

**Snowflake:**
```sql
FROM {{ source('sales_data', 'orders') }}
-- or for development with seeds:
FROM {{ ref('sample_orders_data') }}
```

## Business Logic Macros

### Customer Tier Classification
```sql
{% macro customer_tier_classification(amount_column) %}
    CASE
        WHEN {{ amount_column }} > {{ var('vip_threshold') }} THEN 'VIP'
        WHEN {{ amount_column }} > {{ var('preferred_threshold') }} THEN 'Preferred'
        ELSE 'Standard'
    END
{% endmacro %}
```

### Array Aggregation with Struct
```sql
{% macro array_agg_struct(columns, order_by_clause=none, limit_clause=none) %}
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            {% for column in columns %}
            '{{ column.name }}', {{ column.expr }}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        )
        {% if order_by_clause %}
        ORDER BY {{ order_by_clause }}
        {% endif %}
    )
    {% if limit_clause %}
    [0:{{ limit_clause - 1 }}]
    {% endif %}
{% endmacro %}
```

### Array Unnesting
```sql
{% macro unnest_array(table_alias, array_column) %}
    LATERAL FLATTEN(input => {{ table_alias }}.{{ array_column }})
{% endmacro %}
```

## Data Type Conversions

| BigQuery Type | Snowflake Type | Notes |
|--------------|----------------|--------|
| `STRING` | `VARCHAR` | Snowflake VARCHAR has no length limit by default |
| `INT64` | `NUMBER` | Snowflake uses NUMBER for all numeric types |
| `FLOAT64` | `FLOAT` | Direct mapping |
| `DATE` | `DATE` | Direct mapping |
| `STRUCT` | `OBJECT` | BigQuery STRUCT → Snowflake OBJECT/VARIANT |
| `ARRAY<STRUCT>` | `ARRAY` | Array of objects in Snowflake |

## Performance Optimizations

### Materialization Strategy
- **Staging Models**: `view` for real-time data access
- **Intermediate Models**: `ephemeral` for memory-based processing  
- **Mart Models**: `table` for optimal query performance
- **Array Operations**: Minimize unnecessary array manipulations

### Snowflake-Specific Optimizations
- Use `LATERAL FLATTEN` efficiently with appropriate JOIN conditions
- Leverage array slicing `[0:n]` instead of LIMIT for better performance
- Utilize `OBJECT_CONSTRUCT` for structured data creation
- Implement proper casting with `::` operator for JSON path access

## Testing Strategy

### Generic Tests
- `not_null` for critical fields
- `unique` for identifier columns  
- `accepted_values` for categorical data
- `dbt_utils.expression_is_true` for numerical validations

### Custom Business Logic Tests
- Customer tier classification validation
- Array size and structure validation
- Cross-model calculation consistency
- Edge case scenario testing

### Performance Tests
- Query execution time validation
- Resource usage monitoring
- Concurrent execution testing
- Large dataset processing validation