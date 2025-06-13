#!/bin/bash

# DBT Project Validation Script for sample_bigquery_1
# Validates project structure, configuration, and key files

echo "🔍 Validating DBT project structure for sample_bigquery_1..."

PROJECT_ROOT="dbt_sample_bigquery_1"
EXIT_CODE=0

# Function to check if file exists
check_file() {
    if [ -f "$1" ]; then
        echo "✅ $1"
    else
        echo "❌ $1 - MISSING"
        EXIT_CODE=1
    fi
}

# Function to check if directory exists
check_dir() {
    if [ -d "$1" ]; then
        echo "✅ $1/"
    else
        echo "❌ $1/ - MISSING"
        EXIT_CODE=1
    fi
}

echo ""
echo "📁 Core Project Structure:"
check_file "$PROJECT_ROOT/dbt_project.yml"
check_file "$PROJECT_ROOT/profiles.yml"
check_file "$PROJECT_ROOT/packages.yml"
check_file "$PROJECT_ROOT/.gitignore"
check_file "$PROJECT_ROOT/README.md"
check_file "$PROJECT_ROOT/CONVERSION_SUMMARY.md"

echo ""
echo "📁 Directory Structure:"
check_dir "$PROJECT_ROOT/models"
check_dir "$PROJECT_ROOT/models/staging"
check_dir "$PROJECT_ROOT/models/intermediate"
check_dir "$PROJECT_ROOT/models/marts"
check_dir "$PROJECT_ROOT/macros"
check_dir "$PROJECT_ROOT/tests"
check_dir "$PROJECT_ROOT/seeds"

echo ""
echo "📁 Model Files:"
check_file "$PROJECT_ROOT/models/staging/sources.yml"
check_file "$PROJECT_ROOT/models/staging/stg_orders.sql"
check_file "$PROJECT_ROOT/models/staging/schema.yml"
check_file "$PROJECT_ROOT/models/intermediate/int_sales_items.sql"
check_file "$PROJECT_ROOT/models/intermediate/int_customer_totals.sql"
check_file "$PROJECT_ROOT/models/intermediate/int_ranked_orders.sql"
check_file "$PROJECT_ROOT/models/marts/customer_analysis.sql"
check_file "$PROJECT_ROOT/models/marts/schema.yml"

echo ""
echo "📁 Macro Files:"
check_file "$PROJECT_ROOT/macros/bigquery_to_snowflake_functions.sql"
check_file "$PROJECT_ROOT/macros/utility_functions.sql"

echo ""
echo "📁 Test Files:"
check_file "$PROJECT_ROOT/tests/test_customer_tier_logic.sql"
check_file "$PROJECT_ROOT/tests/test_data_lineage_integrity.sql"
check_file "$PROJECT_ROOT/tests/test_transformation_accuracy.sql"

echo ""
echo "📁 Seed Files:"
check_file "$PROJECT_ROOT/seeds/sample_orders_data.csv"
check_file "$PROJECT_ROOT/seeds/schema.yml"

echo ""
echo "🔍 Configuration Validation:"

# Check if dbt_project.yml contains required configurations
if grep -q "name: 'sample_bigquery_1'" "$PROJECT_ROOT/dbt_project.yml"; then
    echo "✅ Project name configured correctly"
else
    echo "❌ Project name not configured correctly"
    EXIT_CODE=1
fi

if grep -q "materialized: ephemeral" "$PROJECT_ROOT/dbt_project.yml"; then
    echo "✅ Intermediate model materialization configured"
else
    echo "❌ Intermediate model materialization not configured"
    EXIT_CODE=1
fi

# Check if profiles.yml contains Snowflake configuration
if grep -q "type: snowflake" "$PROJECT_ROOT/profiles.yml"; then
    echo "✅ Snowflake adapter configured"
else
    echo "❌ Snowflake adapter not configured"
    EXIT_CODE=1
fi

echo ""
echo "📊 Project Statistics:"
MODEL_COUNT=$(find "$PROJECT_ROOT/models" -name "*.sql" | wc -l)
MACRO_COUNT=$(find "$PROJECT_ROOT/macros" -name "*.sql" | wc -l)
TEST_COUNT=$(find "$PROJECT_ROOT/tests" -name "*.sql" | wc -l)
SEED_COUNT=$(find "$PROJECT_ROOT/seeds" -name "*.csv" | wc -l)

echo "📄 Models: $MODEL_COUNT"
echo "🔧 Macros: $MACRO_COUNT"
echo "🧪 Tests: $TEST_COUNT"
echo "🌱 Seeds: $SEED_COUNT"

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "🎉 Project validation PASSED! All required files and configurations are present."
else
    echo "❌ Project validation FAILED! Some files or configurations are missing."
fi

exit $EXIT_CODE