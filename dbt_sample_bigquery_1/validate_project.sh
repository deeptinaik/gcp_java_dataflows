#!/bin/bash

# DBT Project Structure Validation Script
# Validates the complete project structure and key files

echo "=== DBT Project Validation for sample_bigquery_1 Migration ==="
echo

PROJECT_DIR="/home/runner/work/gcp_java_dataflows/gcp_java_dataflows/dbt_sample_bigquery_1"
cd "$PROJECT_DIR" || exit 1

echo "✓ Project Directory: $PROJECT_DIR"
echo

# Check core configuration files
echo "📁 Core Configuration Files:"
files=("dbt_project.yml" "profiles.yml" "README.md")
for file in "${files[@]}"; do
    if [[ -f "$file" ]]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (missing)"
    fi
done
echo

# Check directory structure
echo "📁 Directory Structure:"
dirs=("models" "models/staging" "models/marts" "macros" "tests" "seeds")
for dir in "${dirs[@]}"; do
    if [[ -d "$dir" ]]; then
        echo "  ✓ $dir/"
    else
        echo "  ✗ $dir/ (missing)"
    fi
done
echo

# Check staging models
echo "📁 Staging Models:"
staging_files=("models/staging/sources.yml" "models/staging/stg_sales.sql" "models/staging/stg_customer_totals.sql" "models/staging/stg_ranked_orders.sql")
for file in "${staging_files[@]}"; do
    if [[ -f "$file" ]]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (missing)"
    fi
done
echo

# Check mart models
echo "📁 Mart Models:"
mart_files=("models/marts/mart_customer_analysis.sql" "models/marts/schema.yml")
for file in "${mart_files[@]}"; do
    if [[ -f "$file" ]]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (missing)"
    fi
done
echo

# Check macros
echo "📁 Macros:"
macro_files=("macros/common_functions.sql" "macros/business_logic.sql")
for file in "${macro_files[@]}"; do
    if [[ -f "$file" ]]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (missing)"
    fi
done
echo

# Check test files
echo "📁 Test Files:"
test_files=("tests/test_customer_tier_logic_vip.sql" "tests/test_customer_tier_logic_preferred.sql" "tests/test_customer_tier_logic_standard.sql" "tests/test_last_3_orders_limit.sql" "tests/test_no_negative_amounts.sql")
for file in "${test_files[@]}"; do
    if [[ -f "$file" ]]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (missing)"
    fi
done
echo

# Count files and provide summary
total_files=$(find . -type f -name "*.sql" -o -name "*.yml" -o -name "*.md" | wc -l)
echo "📊 Summary:"
echo "  Total project files: $total_files"
echo "  Staging models: $(find models/staging -name "*.sql" 2>/dev/null | wc -l)"
echo "  Mart models: $(find models/marts -name "*.sql" 2>/dev/null | wc -l)"
echo "  Macro files: $(find macros -name "*.sql" 2>/dev/null | wc -l)"
echo "  Test files: $(find tests -name "*.sql" 2>/dev/null | wc -l)"
echo

echo "=== Validation Complete ==="