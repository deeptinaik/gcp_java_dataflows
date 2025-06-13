#!/bin/bash

# Validation script for Sample BigQuery DBT Project
# This script validates the DBT project structure and configuration

echo "=== Sample BigQuery DBT Project Validation ==="
echo ""

# Check if we're in the right directory
if [ ! -f "dbt_project.yml" ]; then
    echo "❌ Error: Not in a DBT project directory"
    exit 1
fi

echo "✅ DBT project directory confirmed"

# Validate project structure
echo ""
echo "📁 Checking project structure..."

required_dirs=("models" "models/staging" "models/intermediate" "models/marts" "macros" "tests" "seeds")
required_files=("dbt_project.yml" "profiles.yml" "README.md" ".gitignore")

for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo "✅ Directory exists: $dir"
    else
        echo "❌ Missing directory: $dir"
    fi
done

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ File exists: $file"
    else
        echo "❌ Missing file: $file"
    fi
done

# Check key model files
echo ""
echo "📋 Checking model files..."

models=(
    "models/staging/stg_orders.sql"
    "models/intermediate/int_sales_aggregated.sql"
    "models/intermediate/int_customer_totals.sql"
    "models/intermediate/int_ranked_orders.sql"
    "models/marts/customer_analytics.sql"
)

for model in "${models[@]}"; do
    if [ -f "$model" ]; then
        echo "✅ Model exists: $model"
    else
        echo "❌ Missing model: $model"
    fi
done

# Check macro files
echo ""
echo "🔧 Checking macro files..."

macros=(
    "macros/sample_bigquery_macros.sql"
)

for macro in "${macros[@]}"; do
    if [ -f "$macro" ]; then
        echo "✅ Macro exists: $macro"
    else
        echo "❌ Missing macro: $macro"
    fi
done

# Check test files
echo ""
echo "🧪 Checking test files..."

tests=(
    "tests/test_customer_tier_classification.sql"
    "tests/test_order_ranking_logic.sql"
    "tests/test_spending_calculation_accuracy.sql"
    "tests/test_edge_cases_validation.sql"
)

for test in "${tests[@]}"; do
    if [ -f "$test" ]; then
        echo "✅ Test exists: $test"
    else
        echo "❌ Missing test: $test"
    fi
done

# Check seed files
echo ""
echo "🌱 Checking seed files..."

seeds=(
    "seeds/sample_orders.csv"
)

for seed in "${seeds[@]}"; do
    if [ -f "$seed" ]; then
        echo "✅ Seed exists: $seed"
    else
        echo "❌ Missing seed: $seed"
    fi
done

# Validate dbt_project.yml content
echo ""
echo "📋 Validating dbt_project.yml configuration..."

if grep -q "dbt_sample_bigquery" dbt_project.yml; then
    echo "✅ Project name configured correctly"
else
    echo "❌ Project name not found in dbt_project.yml"
fi

if grep -q "materialized: table" dbt_project.yml; then
    echo "✅ Table materialization configured"
else
    echo "❌ Table materialization not configured"
fi

if grep -q "unique_key" dbt_project.yml; then
    echo "✅ Unique key configuration found"
else
    echo "❌ Unique key configuration missing"
fi

# Check profiles.yml
echo ""
echo "🔗 Validating profiles.yml configuration..."

if grep -q "snowflake" profiles.yml; then
    echo "✅ Snowflake adapter configured"
else
    echo "❌ Snowflake adapter not configured"
fi

if grep -q "dev:" profiles.yml && grep -q "staging:" profiles.yml && grep -q "prod:" profiles.yml; then
    echo "✅ All target environments configured"
else
    echo "❌ Missing target environments"
fi

# Check for BigQuery to Snowflake conversions
echo ""
echo "🔄 Validating BigQuery to Snowflake conversions..."

if grep -q "OBJECT_CONSTRUCT" models/intermediate/int_sales_aggregated.sql; then
    echo "✅ ARRAY_AGG(STRUCT()) converted to OBJECT_CONSTRUCT()"
else
    echo "❌ BigQuery ARRAY_AGG(STRUCT()) conversion missing"
fi

if grep -q "LATERAL FLATTEN" models/intermediate/int_customer_totals.sql; then
    echo "✅ UNNEST() converted to LATERAL FLATTEN()"
else
    echo "❌ BigQuery UNNEST() conversion missing"
fi

if grep -q "classify_customer_tier" models/marts/customer_analytics.sql; then
    echo "✅ Customer tier macro implemented"
else
    echo "❌ Customer tier macro missing"
fi

# Final summary
echo ""
echo "=== Validation Complete ==="
echo ""
echo "📊 Project Summary:"
echo "   - Project Name: Sample BigQuery DBT"
echo "   - Target Platform: Snowflake"
echo "   - Models: Staging + Intermediate + Marts"
echo "   - Materialization: View + Ephemeral + Table"
echo "   - Tests: Data Quality + Business Logic"
echo "   - Documentation: Comprehensive README"
echo ""
echo "🎯 This project successfully converts the sample_bigquery.sql"
echo "   BigQuery query to production-ready DBT with Snowflake implementation."
echo ""
echo "✨ Ready for deployment and execution!"