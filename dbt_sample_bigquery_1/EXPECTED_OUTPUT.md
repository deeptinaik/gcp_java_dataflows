# Expected Output Example

Based on the sample seed data, the converted DBT project should produce output similar to:

## customer_sales_analysis table

| customer_id | total_spent | total_orders | customer_tier | last_3_orders |
|-------------|-------------|--------------|---------------|---------------|
| CUST004     | 18750.00    | 3            | VIP           | [{"order_id":"ORD011","order_total":5000.00,"order_date":"2024-03-10"}, {"order_id":"ORD010","order_total":8750.00,"order_date":"2024-02-25"}, {"order_id":"ORD009","order_total":10000.00,"order_date":"2024-01-30"}] |
| CUST002     | 4150.00     | 3            | Standard      | [{"order_id":"ORD006","order_total":3000.00,"order_date":"2024-03-01"}, {"order_id":"ORD005","order_total":750.00,"order_date":"2024-02-15"}, {"order_id":"ORD004","order_total":400.00,"order_date":"2024-01-20"}] |
| CUST001     | 575.00      | 3            | Standard      | [{"order_id":"ORD003","order_total":200.00,"order_date":"2024-03-05"}, {"order_id":"ORD002","order_total":250.00,"order_date":"2024-02-10"}, {"order_id":"ORD001","order_total":200.00,"order_date":"2024-01-15"}] |
| CUST003     | 175.00      | 2            | Standard      | [{"order_id":"ORD008","order_total":50.00,"order_date":"2024-02-20"}, {"order_id":"ORD007","order_total":125.00,"order_date":"2024-01-25"}] |

## Key Features Demonstrated

1. **Customer Tier Classification**: 
   - CUST004 → VIP (spent $18,750 > $10,000 threshold)
   - Others → Standard (spent < $5,000 threshold)

2. **Array of Last 3 Orders**: 
   - Properly formatted JSON objects with order details
   - Ordered by date descending (most recent first)
   - Limited to maximum 3 orders per customer

3. **Accurate Aggregations**:
   - Total spent matches sum of all order line items
   - Total orders matches distinct order count
   - Customer tiers applied according to business rules

This output format demonstrates the successful conversion of BigQuery's complex array aggregation and struct operations to Snowflake's native OBJECT_CONSTRUCT and ARRAY_AGG functions.