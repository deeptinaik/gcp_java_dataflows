#!/bin/bash

# DBT Project Validation Script
# Validates the structure and completeness of the DBT project

echo "=== DBT Sample Line Migration 6 Project Validation ==="
echo ""

# Check if we're in the right directory
if [ ! -f "dbt_project.yml" ]; then
    echo "❌ Error: dbt_project.yml not found. Please run this script from the project root."
    exit 1
fi

echo "✅ Project root directory confirmed"

# Validate required files
echo ""
echo "📁 Checking required files..."

required_files=(
    "dbt_project.yml"
    "profiles.yml"
    "README.md"
    ".gitignore"
    "models/staging/sources.yml"
    "models/staging/stg_orders.sql"
    "models/staging/stg_customer_totals.sql"
    "models/staging/stg_ranked_orders.sql"
    "models/marts/customer_analysis.sql"
    "models/marts/schema.yml"
    "macros/business_logic.sql"
    "macros/common_functions.sql"
    "seeds/sample_orders.csv"
    "tests/test_vip_tier_classification.sql"
    "tests/test_preferred_tier_classification.sql"
    "tests/test_spending_calculation_accuracy.sql"
    "tests/test_edge_cases_validation.sql"
)

missing_files=0
for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file"
    else
        echo "❌ $file (missing)"
        missing_files=$((missing_files + 1))
    fi
done

echo ""
echo "📊 Validation Summary:"
echo "   Total files checked: ${#required_files[@]}"
echo "   Missing files: $missing_files"

if [ $missing_files -eq 0 ]; then
    echo "✅ All required files are present!"
    echo ""
    echo "🎉 Project validation completed successfully!"
    echo "   Ready for DBT operations (seed, run, test)"
    exit 0
else
    echo "❌ Project validation failed!"
    echo "   Please ensure all required files are present before proceeding."
    exit 1
fi