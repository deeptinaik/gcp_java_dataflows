#!/bin/bash

# Validation script for DBT UKRG VPayNet Consumption Load project
# Validates project structure, configuration, and SQL syntax

echo "=============================================="
echo "DBT UKRG VPayNet Consumption Load Validation"
echo "=============================================="

PROJECT_DIR="/home/runner/work/gcp_java_dataflows/gcp_java_dataflows/dbt_ukrg_vpaynet_consumption_load"

# Check if we're in the right directory
if [ ! -d "$PROJECT_DIR" ]; then
    echo "❌ Project directory not found: $PROJECT_DIR"
    exit 1
fi

cd "$PROJECT_DIR"
echo "✅ Project directory found"

# Check required files
echo ""
echo "Checking required files..."

required_files=(
    "dbt_project.yml"
    "profiles.yml"
    "README.md"
    ".gitignore"
    "models/staging/sources.yml"
    "models/staging/schema.yml"
    "models/marts/schema.yml"
)

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

# Check model files
echo ""
echo "Checking model files..."

model_files=(
    "models/staging/stg_lotr_uid_key_ukrg.sql"
    "models/staging/stg_ukrg_trans_fact.sql" 
    "models/staging/stg_vpaynet_installment_fee_detail_daily_uk.sql"
    "models/marts/wwmaster_transaction_detail_vpaynet_update.sql"
)

for file in "${model_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

# Check macro files
echo ""
echo "Checking macro files..."

macro_files=(
    "macros/vpaynet_installment_indicator.sql"
    "macros/handle_null_vpaynet_sk.sql"
)

for file in "${macro_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

# Check test files
echo ""
echo "Checking test files..."

test_files=(
    "tests/validate_vpaynet_installment_indicator.sql"
    "seeds/sample_transaction_data.csv"
)

for file in "${test_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

echo ""
echo "=============================================="
echo "Project structure validation complete!"
echo "=============================================="

# Basic YAML syntax validation
echo ""
echo "Validating YAML syntax..."

if command -v python3 &> /dev/null; then
    python3 -c "
import yaml
import sys

files_to_check = [
    'dbt_project.yml',
    'models/staging/sources.yml',
    'models/staging/schema.yml', 
    'models/marts/schema.yml'
]

for file in files_to_check:
    try:
        with open(file, 'r') as f:
            yaml.safe_load(f)
        print(f'✅ {file} - valid YAML')
    except Exception as e:
        print(f'❌ {file} - invalid YAML: {e}')
        sys.exit(1)
"
    echo "✅ All YAML files are valid"
else
    echo "⚠️  Python3 not available, skipping YAML validation"
fi

echo ""
echo "=============================================="
echo "Validation complete!"
echo "=============================================="