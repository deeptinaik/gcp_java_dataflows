# BigQuery to Snowflake Function Mapping

This document details the complete mapping of BigQuery functions to Snowflake equivalents used in the UKRG LCOT UID Auth Dif conversion.

## Function Conversions

| BigQuery Function | Snowflake Equivalent | DBT Macro | Usage |
|------------------|---------------------|-----------|--------|
| `GENERATE_UUID()` | `UUID_STRING()` | `generate_uuid()` | LCOT GUID generation |
| `SAFE_CAST(x AS type)` | `TRY_CAST(x AS type)` | `safe_cast(column, type)` | Error-resistant casting |
| `CURRENT_DATETIME()` | `CURRENT_TIMESTAMP()` | `current_datetime()` | Timestamp functions |
| `CURRENT_DATE()` | `CURRENT_DATE()` | `current_date()` | Date functions |
| `FORMAT_DATE(fmt, date)` | `TO_CHAR(date, fmt)` | `format_date(format, date)` | Date formatting |
| `PARSE_DATE(fmt, str)` | `TO_DATE(str, fmt)` | `parse_date(format, string)` | Date parsing |
| `DATE_SUB(date, INTERVAL n unit)` | `DATEADD(unit, -n, date)` | `date_sub(date, n, unit)` | Date arithmetic |
| `DATE_ADD(date, INTERVAL n unit)` | `DATEADD(unit, n, date)` | `date_add(date, n, unit)` | Date arithmetic |
| `SPLIT(str, delim)[SAFE_OFFSET(n)]` | `SPLIT_PART(str, delim, n+1)` | `split_safe_offset(str, delim, n)` | String splitting |
| `TO_BASE64(SHA512(str))` | `BASE64_ENCODE(SHA2_BINARY(str, 512))` | `sha512_base64(str)` | Hash generation |
| `RPAD(str, len, pad)` | `RPAD(str, len, pad)` | `rpad(str, len, pad)` | String padding |
| `LTRIM(str, chars)` | `LTRIM(str, chars)` | `ltrim_zeros(str)` | String trimming |
| `REGEXP_REPLACE(str, pattern, replacement)` | `REGEXP_REPLACE(str, pattern, replacement)` | `regexp_replace(str, pattern, replacement)` | Regex operations |

## SQL Syntax Conversions

### QUALIFY Clause
**BigQuery:**
```sql
SELECT col1, ROW_NUMBER() OVER(PARTITION BY col2 ORDER BY col3) as rn
FROM table1
QUALIFY rn = 1
```

**Snowflake:**
```sql
SELECT col1 
FROM (
    SELECT col1, ROW_NUMBER() OVER(PARTITION BY col2 ORDER BY col3) as rn
    FROM table1
) qualified_data
WHERE rn = 1
```

### Table References
**BigQuery:**
```sql
FROM project_id.dataset_id.table_name
```

**Snowflake:**
```sql
FROM {{ source('schema_name', 'table_name') }}
```

## Business Logic Macros

### Authorization Response Code Validation
```sql
{% macro auth_response_code_valid(resp_code, record_type, rec_type, excep_reason_code) %}
  (
    (
      COALESCE({{ resp_code }}, 0) < 50 
      AND COALESCE({{ record_type }}, '') <> 'mpg_s'
      AND (COALESCE({{ rec_type }}, '') <> '21' OR COALESCE({{ excep_reason_code }}, '') < '500' OR COALESCE({{ excep_reason_code }}, '') > '511')
    )
    OR
    (
      COALESCE({{ record_type }}, '') = 'mpg_s'
      AND COALESCE({{ resp_code }}, 0) <> 5
    )
  )
{% endmacro %}
```

### Terminal ID Matching Logic
```sql
{% macro terminal_id_match(terminal_id_t, terminal_id_a) %}
  ({{ terminal_id_t }} = {{ terminal_id_a }} 
   OR (TRIM({{ terminal_id_t }}) = '' AND TRIM({{ terminal_id_a }}) <> '') 
   OR (TRIM({{ terminal_id_t }}) = '' AND TRIM({{ terminal_id_a }}) = ''))
{% endmacro %}
```

### LCOT GUID Key Selection
```sql
{% macro select_lcot_guid_key_sk(trans_sk, auth_sk, lcot_guid_key_sk_trans, lcot_guid_key_sk_auth) %}
  CASE 
    WHEN {{ trans_sk }} <> '-1' AND {{ auth_sk }} <> '-1' THEN COALESCE({{ lcot_guid_key_sk_trans }}, {{ lcot_guid_key_sk_auth }})
    WHEN {{ trans_sk }} <> '-1' THEN {{ lcot_guid_key_sk_trans }}
    WHEN {{ auth_sk }} <> '-1' THEN {{ lcot_guid_key_sk_auth }}
    ELSE {{ generate_uuid() }}
  END
{% endmacro %}
```

## Data Type Conversions

| BigQuery Type | Snowflake Type | Notes |
|--------------|----------------|-------|
| `INT64` | `NUMBER` | Snowflake uses NUMBER for all numeric types |
| `FLOAT64` | `FLOAT` | Direct mapping |
| `STRING` | `VARCHAR` | Snowflake VARCHAR has no length limit by default |
| `DATE` | `DATE` | Direct mapping |
| `DATETIME` | `TIMESTAMP` | Snowflake uses TIMESTAMP for datetime |
| `TIMESTAMP` | `TIMESTAMP_LTZ` | With timezone awareness |

## Performance Optimizations

### Incremental Models
- Use `unique_key` for proper merge operations
- Specify `merge_update_columns` for controlled field updates
- Implement date-based filtering for incremental processing

### Materialization Strategy
- **Staging Models**: `view` for real-time data access
- **Mart Models**: `incremental` with merge strategy for large datasets
- **Reference Models**: `table` for small, frequently accessed data

## Testing Strategy

### Generic Tests
- `not_null` for critical fields
- `unique` for identifier columns
- `relationships` for foreign key constraints
- `accepted_values` for categorical data

### Custom Business Logic Tests
- Complex authorization-transaction matching rules
- Data quality validations
- Edge case scenario testing
- Incremental processing integrity