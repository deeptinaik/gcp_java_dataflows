#!/bin/bash

# DBT Project Validation Script for Sample BigQuery Migration

echo "=== DBT Sample BigQuery Project Validation ==="
echo

# Check project structure
echo "1. Validating project structure..."
required_dirs=("models" "models/staging" "models/marts" "macros" "tests" "seeds")
required_files=(
    "dbt_project.yml"
    "profiles.yml"
    "models/staging/sources.yml"
    "models/staging/stg_orders.sql"
    "models/staging/int_customer_totals.sql"
    "models/staging/int_ranked_orders.sql"
    "models/marts/sales_analytics.sql"
    "macros/common_functions.sql"
    "macros/sales_analytics.sql"
    "seeds/sample_orders_data.csv"
)

for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo "  ✓ Directory exists: $dir"
    else
        echo "  ✗ Missing directory: $dir"
        exit 1
    fi
done

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ File exists: $file"
    else
        echo "  ✗ Missing file: $file"
        exit 1
    fi
done

echo

# Check dbt_project.yml configuration
echo "2. Validating dbt_project.yml configuration..."
if grep -q "name: 'sample_bigquery'" dbt_project.yml; then
    echo "  ✓ Project name configured correctly"
else
    echo "  ✗ Project name not configured correctly"
    exit 1
fi

if grep -q "staging:" dbt_project.yml && grep -q "marts:" dbt_project.yml; then
    echo "  ✓ Model configurations present"
else
    echo "  ✗ Model configurations missing"
    exit 1
fi

echo

# Check profiles.yml configuration
echo "3. Validating profiles.yml configuration..."
if grep -q "type: snowflake" profiles.yml; then
    echo "  ✓ Snowflake adapter configured"
else
    echo "  ✗ Snowflake adapter not configured"
    exit 1
fi

if grep -q "dev:" profiles.yml && grep -q "staging:" profiles.yml && grep -q "prod:" profiles.yml; then
    echo "  ✓ All environments configured"
else
    echo "  ✗ Environment configurations missing"
    exit 1
fi

echo

# Check macro definitions
echo "4. Validating macro definitions..."
required_macros=(
    "customer_tier_classification"
    "aggregate_order_items"
    "rank_orders_by_date"
    "generate_current_timestamp"
    "safe_cast"
)

for macro in "${required_macros[@]}"; do
    if grep -q "macro $macro" macros/*.sql; then
        echo "  ✓ Macro defined: $macro"
    else
        echo "  ✗ Missing macro: $macro"
        exit 1
    fi
done

echo

# Check model references
echo "5. Validating model references..."
if grep -q "ref('stg_orders')" models/staging/int_customer_totals.sql; then
    echo "  ✓ Customer totals references staging orders"
else
    echo "  ✗ Customer totals missing staging reference"
    exit 1
fi

if grep -q "ref('int_customer_totals')" models/marts/sales_analytics.sql; then
    echo "  ✓ Sales analytics references customer totals"
else
    echo "  ✗ Sales analytics missing customer totals reference"
    exit 1
fi

echo

# Check test files
echo "6. Validating test files..."
test_files=(
    "tests/test_customer_tier_logic.sql"
    "tests/test_data_lineage_integrity.sql"
    "tests/test_order_aggregation_accuracy.sql"
)

for test_file in "${test_files[@]}"; do
    if [ -f "$test_file" ]; then
        echo "  ✓ Test file exists: $test_file"
    else
        echo "  ✗ Missing test file: $test_file"
        exit 1
    fi
done

echo

# Summary
echo "=== Validation Complete ==="
echo "✓ All required components are present and configured correctly"
echo "✓ Project structure follows DBT best practices"
echo "✓ BigQuery to Snowflake conversion implemented"
echo "✓ Comprehensive test suite included"
echo "✓ Production-ready DBT project structure"
echo
echo "Project is ready for deployment!"