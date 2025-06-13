#!/bin/bash

# Sample BigQuery 1 DBT Project Validation Script
# Validates the converted DBT project structure and configuration

echo "=== DBT Project Validation for Sample BigQuery 1 ==="
echo

# Check if we're in the right directory
if [ ! -f "dbt_project.yml" ]; then
    echo "❌ Error: dbt_project.yml not found. Please run this script from the dbt_sample_bigquery_1 directory."
    exit 1
fi

# Validation functions
check_file() {
    if [ -f "$1" ]; then
        echo "✅ $1"
        return 0
    else
        echo "❌ $1 (missing)"
        return 1
    fi
}

check_directory() {
    if [ -d "$1" ]; then
        echo "✅ $1/"
        return 0
    else
        echo "❌ $1/ (missing)"
        return 1
    fi
}

validation_errors=0

echo "📁 Directory Structure Validation:"
check_directory "models" || ((validation_errors++))
check_directory "models/staging" || ((validation_errors++))
check_directory "models/intermediate" || ((validation_errors++))
check_directory "models/marts" || ((validation_errors++))
check_directory "macros" || ((validation_errors++))
check_directory "tests" || ((validation_errors++))
check_directory "seeds" || ((validation_errors++))
echo

echo "📄 Core Configuration Files:"
check_file "dbt_project.yml" || ((validation_errors++))
check_file "profiles.yml" || ((validation_errors++))
check_file "README.md" || ((validation_errors++))
check_file "CONVERSION_SUMMARY.md" || ((validation_errors++))
check_file ".gitignore" || ((validation_errors++))
echo

echo "🏗️ Model Files:"
check_file "models/staging/sources.yml" || ((validation_errors++))
check_file "models/staging/stg_orders.sql" || ((validation_errors++))
check_file "models/intermediate/int_sales_aggregated.sql" || ((validation_errors++))
check_file "models/intermediate/int_customer_totals.sql" || ((validation_errors++))
check_file "models/intermediate/int_ranked_orders.sql" || ((validation_errors++))
check_file "models/marts/customer_analysis.sql" || ((validation_errors++))
check_file "models/marts/schema.yml" || ((validation_errors++))
echo

echo "🛠️ Macro Files:"
check_file "macros/common_functions.sql" || ((validation_errors++))
echo

echo "🧪 Test Files:"
check_file "tests/test_customer_tier_logic.sql" || ((validation_errors++))
check_file "tests/test_data_consistency.sql" || ((validation_errors++))
check_file "tests/test_last_orders_array.sql" || ((validation_errors++))
echo

echo "🌱 Seed Files:"
check_file "seeds/sample_orders.csv" || ((validation_errors++))
check_file "seeds/schema.yml" || ((validation_errors++))
echo

echo "🔍 Configuration Validation:"

# Check dbt_project.yml content
if grep -q "sample_bigquery_1" dbt_project.yml; then
    echo "✅ Project name configured correctly"
else
    echo "❌ Project name not configured correctly"
    ((validation_errors++))
fi

if grep -q "staging:" dbt_project.yml; then
    echo "✅ Staging model configuration found"
else
    echo "❌ Staging model configuration missing"
    ((validation_errors++))
fi

if grep -q "intermediate:" dbt_project.yml; then
    echo "✅ Intermediate model configuration found"
else
    echo "❌ Intermediate model configuration missing"
    ((validation_errors++))
fi

if grep -q "marts:" dbt_project.yml; then
    echo "✅ Marts model configuration found"
else
    echo "❌ Marts model configuration missing"
    ((validation_errors++))
fi

# Check if profiles.yml has Snowflake configuration
if grep -q "type: snowflake" profiles.yml; then
    echo "✅ Snowflake adapter configured"
else
    echo "❌ Snowflake adapter not configured"
    ((validation_errors++))
fi

# Check for multi-environment setup
if grep -q -E "(dev|staging|prod):" profiles.yml; then
    echo "✅ Multi-environment configuration found"
else
    echo "❌ Multi-environment configuration missing"
    ((validation_errors++))
fi

echo

echo "📊 Summary:"
if [ $validation_errors -eq 0 ]; then
    echo "🎉 All validations passed! The DBT project is properly structured."
    echo "   Ready for development and deployment."
else
    echo "⚠️  $validation_errors validation error(s) found."
    echo "   Please review and fix the issues above."
fi

echo
echo "🚀 Next Steps:"
echo "   1. Set up Snowflake environment variables"
echo "   2. Run 'dbt debug' to test connection"
echo "   3. Run 'dbt seed' to load sample data"
echo "   4. Run 'dbt run' to execute the pipeline"
echo "   5. Run 'dbt test' to validate data quality"
echo "   6. Run 'dbt docs generate && dbt docs serve' for documentation"

exit $validation_errors