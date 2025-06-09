#!/bin/bash

# DBT Customer Analytics Project - Run Script
# This script demonstrates how to run the DBT project in different scenarios

echo "🚀 Customer Analytics DBT Project Runner"
echo "======================================="

# Function to display usage
usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  deps     Install DBT dependencies"
    echo "  run      Run all models"
    echo "  test     Run all tests"
    echo "  docs     Generate and serve documentation"
    echo "  full     Run complete pipeline (deps + run + test)"
    echo "  staging  Run models for staging layer only"
    echo "  marts    Run models for marts layer only"
    echo ""
    echo "Examples:"
    echo "  $0 full                    # Complete pipeline"
    echo "  $0 run --target production # Run in production"
    echo "  $0 staging                 # Run staging models only"
    exit 1
}

# Install dependencies
run_deps() {
    echo "📦 Installing DBT dependencies..."
    dbt deps
}

# Run all models
run_models() {
    echo "🔧 Running DBT models..."
    dbt run "$@"
}

# Run tests
run_tests() {
    echo "🧪 Running DBT tests..."
    dbt test "$@"
}

# Generate documentation
run_docs() {
    echo "📚 Generating DBT documentation..."
    dbt docs generate "$@"
    echo "🌐 Serving documentation (Ctrl+C to stop)..."
    dbt docs serve
}

# Run staging models only
run_staging() {
    echo "🔧 Running staging models..."
    dbt run --models staging "$@"
}

# Run marts models only
run_marts() {
    echo "🔧 Running marts models..."
    dbt run --models marts "$@"
}

# Full pipeline
run_full() {
    echo "🚀 Running full DBT pipeline..."
    run_deps
    run_models "$@"
    run_tests "$@"
    echo "✅ Full pipeline completed successfully!"
}

# Main execution
case "$1" in
    deps)
        shift
        run_deps "$@"
        ;;
    run)
        shift
        run_models "$@"
        ;;
    test)
        shift
        run_tests "$@"
        ;;
    docs)
        shift
        run_docs "$@"
        ;;
    staging)
        shift
        run_staging "$@"
        ;;
    marts)
        shift
        run_marts "$@"
        ;;
    full)
        shift
        run_full "$@"
        ;;
    *)
        usage
        ;;
esac