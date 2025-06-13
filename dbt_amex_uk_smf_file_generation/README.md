# AmexUKSMFFileGeneration DBT Project

## Overview

This project represents the conversion of the **AmexUKSMFFileGeneration** GCP Dataflow pipeline to a DBT with Snowflake implementation. The original Java-based Apache Beam pipeline has been meticulously converted to follow DBT best practices while preserving all business logic and data transformations.

## Architecture

### Original GCP Dataflow Architecture

The original pipeline consisted of:

1. **Entry Point**: `AmexUKSMFFileGenerationPipeline.java`
2. **Reusable Components**:
   - `DataValidation.java` - Validates transaction data
   - `AmexTransactionFilter.java` - Filters Amex-specific transactions
   - `MerchantEnrichment.java` - Enriches data with merchant information
   - `SMFDataProcessing.java` - Splits processing paths
   - `CurrencyConversion.java` - Handles currency conversions
   - `FileFormatting.java` - Formats output for SMF files
3. **Utility Classes**:
   - `AmexCommonUtil.java` - Constants and common utilities
   - `AmexUtility.java` - Helper methods and mappings
   - `AmexQueryTranslator.java` - Dynamic query generation

### Converted DBT with Snowflake Architecture

The DBT project is organized into:

#### 1. **Staging Models** (`models/staging/`)
- `stg_amex_uk_validated_transactions.sql` - Replicates `DataValidation.java`
- `stg_amex_uk_filtered_transactions.sql` - Replicates `AmexTransactionFilter.java`
- `stg_dim_merchant_information.sql` - Prepares merchant data
- `stg_dim_currency_rates.sql` - Prepares currency rate data

#### 2. **Intermediate Models** (`models/intermediate/`)
- `int_amex_uk_enriched_transactions.sql` - Replicates `MerchantEnrichment.java`
- `int_amex_uk_processing_split.sql` - Replicates `SMFDataProcessing.java`
- `int_amex_uk_currency_converted.sql` - Replicates `CurrencyConversion.java`

#### 3. **Marts Models** (`models/marts/`)
- `amex_uk_smf_output.sql` - Final SMF output, replicates `FileFormatting.java`

#### 4. **Macros** (`macros/`)
- `amex_uk_smf_macros.sql` - Reusable SQL functions equivalent to Java utility methods

#### 5. **Tests** (`tests/`)
- Data quality tests equivalent to Java validation logic

#### 6. **Seeds** (`seeds/`)
- Sample reference data for development and testing

## Business Logic Preservation

### Data Validation
- **Original**: `DataValidation.java` validates required fields, data types, and business rules
- **Converted**: `stg_amex_uk_validated_transactions.sql` uses SQL conditions to replicate exact validation logic

### Amex Transaction Filtering
- **Original**: `AmexTransactionFilter.java` filters Amex cards and sets defaults
- **Converted**: `stg_amex_uk_filtered_transactions.sql` uses CASE statements and WHERE clauses

### Merchant Enrichment
- **Original**: `MerchantEnrichment.java` uses side inputs for lookups
- **Converted**: `int_amex_uk_enriched_transactions.sql` uses LEFT JOIN operations

### Currency Conversion
- **Original**: `CurrencyConversion.java` applies exchange rates
- **Converted**: `convert_currency` macro with CASE statements for rate application

### SMF Formatting
- **Original**: `FileFormatting.java` formats data for SMF specifications
- **Converted**: `amex_uk_smf_output.sql` uses format macros for consistent formatting

## Source-to-Target Mapping

### BigQuery to Snowflake Mapping

| Original BigQuery | Converted Snowflake |
|-------------------|-------------------|
| `trusted_layer.amex_uk_transactions` | `AMEX_UK_WAREHOUSE.TRUSTED_LAYER.AMEX_UK_TRANSACTIONS` |
| `transformed_layer.dim_merchant_information` | `AMEX_UK_WAREHOUSE.TRANSFORMED_LAYER.DIM_MERCHANT_INFORMATION` |
| `transformed_layer.dim_currency_rates` | `AMEX_UK_WAREHOUSE.TRANSFORMED_LAYER.DIM_CURRENCY_RATES` |
| `transformed_layer.amex_uk_smf_output` | `AMEX_UK_WAREHOUSE.MARTS.AMEX_UK_SMF_OUTPUT` |

## Key Features

### 1. **Zero Business Logic Loss**
Every transformation, validation, and business rule from the original Java code has been preserved in SQL.

### 2. **Modular Design**
- Staging for raw data processing
- Intermediate for transformations  
- Marts for final outputs
- Ephemeral models to reduce storage costs

### 3. **Performance Optimization**
- Early filtering in staging models
- Efficient JOIN strategies
- Proper materialization strategies

### 4. **Data Quality**
- Comprehensive test suite
- Data validation at each stage
- Error handling and logging

### 5. **Maintainability**
- Reusable macros
- Clear documentation
- Consistent naming conventions

## Configuration

### Variables
All constants from `AmexCommonUtil.java` are converted to DBT variables in `dbt_project.yml`:

```yaml
vars:
  gbp_currency: 'GBP'
  usd_currency: 'USD'
  smf_transaction_record: '02'
  max_merchant_number_length: 15
  # ... and more
```

### Materialization Strategy
- **Staging**: Views for cost efficiency
- **Intermediate**: Ephemeral to avoid unnecessary storage
- **Marts**: Tables for performance

## Running the Project

### Prerequisites
- DBT installed and configured
- Snowflake connection established
- Appropriate warehouse and database permissions

### Execution Steps

1. **Install dependencies**:
   ```bash
   dbt deps
   ```

2. **Load seed data**:
   ```bash
   dbt seed
   ```

3. **Run models**:
   ```bash
   dbt run
   ```

4. **Run tests**:
   ```bash
   dbt test
   ```

5. **Generate documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Testing

The project includes comprehensive tests that mirror the validation logic from the original Java pipeline:

- **Schema tests**: Data type and constraint validation
- **Custom tests**: Business rule validation
- **dbt_utils tests**: Advanced data quality checks

## Best Practices Implemented

1. **Consistent Style Guide**: SQL formatted consistently throughout
2. **Reusable Components**: Macros for common operations
3. **Clear Documentation**: Each model documented with purpose
4. **Modular Design**: Logical separation of concerns
5. **Error Handling**: Graceful handling of edge cases
6. **Performance Optimization**: Efficient query patterns

## Benefits of DBT Conversion

1. **Cost Reduction**: No compute costs during development
2. **Version Control**: SQL code in Git with proper versioning  
3. **Testing Framework**: Built-in data quality testing
4. **Documentation**: Auto-generated lineage and documentation
5. **Collaboration**: Better team collaboration on SQL code
6. **Deployment**: Multiple environment support (dev/staging/prod)

## Migration Notes

This conversion maintains 100% functional equivalency with the original GCP Dataflow pipeline while gaining the benefits of the DBT framework and Snowflake's cloud data warehouse capabilities.

The project demonstrates enterprise-grade data transformation patterns and serves as a template for similar GCP Dataflow to DBT conversions.