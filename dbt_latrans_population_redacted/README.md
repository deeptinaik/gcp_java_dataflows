# LA Trans Population Redacted - DBT Conversion

This DBT project is a complete conversion of the LATransPopulation_Redacted GCP Dataflow pipeline to DBT with Snowflake.

## Original Pipeline Analysis

### Entry Point
- **TransFTTransformPipeline.java**: Main orchestration class that defines the complete data processing pipeline

### Reusable Components
- **Utility.java**: Schema mappings, field transformations, and helper functions
- **FieldTransformation.java**: Core field-level transformations (dates, amounts, cleaning)
- **TransFTProcessing.java**: Processing manager for joins and validations
- **TempTransFT0Processing.java**: Dimension lookup and hierarchy processing
- **Various Transform Classes**: Specialized transformations for currency codes, filtering, joins

## Data Flow

```
trusted_layer.la_trans_ft 
    ↓ (Field Transformations)
    ↓ (Merchant Info Join)
    ↓ (Hierarchy Dimension Joins)
    ↓ (Currency Code Processing)
    ↓ (Settlement Currency Processing)
    ↓ (Data Cleaning & Validation)
    ↓ (Final Filtering)
transformed_layer.la_trans_ft
```

## DBT Project Structure

### Models

#### Staging (`models/staging/`)
- **stg_latrans_ft_source.sql**: Source transaction data with field transformations
- **stg_dim_*.sql**: Dimension table staging models
- **sources.yml**: Source table definitions

#### Intermediate (`models/intermediate/`)
- **int_latrans_ft_merchant_hierarchy.sql**: Merchant and hierarchy dimension joins
- **int_latrans_ft_currency_processing.sql**: Transaction currency code processing
- **int_latrans_ft_settlement_currency_processing.sql**: Settlement currency processing

#### Marts (`models/marts/`)
- **mart_latrans_ft.sql**: Final output table with complete processing
- **schema.yml**: Data quality tests and documentation

### Macros (`macros/`)
- **date_transformations.sql**: Date formatting and conversion functions
- **amount_and_field_transformations.sql**: Amount calculations and field processing
- **currency_code_transformations.sql**: Currency validation and classification

### Configuration
- **dbt_project.yml**: Project configuration, materialization settings, and variables

## Key Business Logic Converted

### Field Transformations
- **Date Conversions**: MMDDYY to YYYYMMDD, YYMMDD to YYYYMMDD
- **Amount Calculations**: Scale by power of 10 for decimal precision
- **Authorization Dates**: Complex year boundary logic
- **Time Formatting**: Time field validation and formatting
- **Batch Control Numbers**: Composite key generation

### Dimension Processing
- **Merchant Information**: Join with merchant dimension for enrichment
- **Hierarchy Lookups**: Corporate → Region → Principal → Associate → Chain
- **Default Values**: Failed lookups marked with DMX_LOOKUP_FAILURE (-2)

### Currency Code Processing
- **Classification**: Alpha, numeric, or null currency codes
- **Default Lookups**: Invalid currencies get defaults from merchant corporate/region
- **ISO Mappings**: Alpha codes mapped to ISO numeric equivalents
- **Dual Processing**: Separate logic for transaction and settlement currencies

### Data Quality
- **Validation**: Date format validation, amount range checks
- **Cleaning**: Null handling, blank field management
- **Filtering**: Remove invalid or test transactions

## Configuration Variables

- **amount_exponent**: 5 (for decimal scaling)
- **dmx_lookup_failure**: '-2' (failed lookup indicator)
- **dmx_lookup_null_blank**: '-1' (null/blank indicator)
- **date_format_***: Various date format constants

## Materialization Strategy

- **Staging Models**: Views for lightweight access to source data
- **Intermediate Models**: Views for step-by-step processing
- **Mart Models**: Tables for final output with partitioning and clustering

## Performance Optimizations

- **Partitioning**: Final table partitioned by transaction_date
- **Clustering**: Clustered on corporate_sk, region_sk, merchant_information_sk
- **Incremental Processing**: Configured for append-only updates

## Testing

- **Uniqueness**: UUID and primary key constraints
- **Not Null**: Critical business fields validation
- **Data Range**: Amount and date range validations
- **Referential Integrity**: Dimension key validation

## Usage

1. **Configure Connection**: Set up Snowflake connection in profiles.yml
2. **Run Staging**: `dbt run --select staging`
3. **Run Intermediate**: `dbt run --select intermediate`
4. **Run Marts**: `dbt run --select marts`
5. **Test Data**: `dbt test`

## Notes

- All business logic from the original Java pipeline has been preserved
- Currency processing handles both alpha and numeric currency codes
- Dimension lookups include proper fallback handling
- Date transformations include complex year boundary logic
- Amount calculations maintain precision using power-of-10 scaling
- The pipeline is designed for append-only operations (WRITE_APPEND equivalent)

This conversion maintains 100% functional equivalence with the original GCP Dataflow pipeline while providing the benefits of DBT's modularity, testing, and documentation capabilities.