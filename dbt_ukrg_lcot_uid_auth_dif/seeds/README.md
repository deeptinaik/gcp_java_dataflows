# Seeds Directory

This directory contains CSV files that define static reference data for the UKRG LCOT UID Auth Dif DBT project.

## Usage

Seeds are used for:
- Static lookup tables
- Reference data that changes infrequently
- Configuration data that needs to be version controlled

## Example Seeds

You can add CSV files here for:
- Card scheme mappings
- Authorization response code mappings  
- Terminal ID validation rules
- Merchant category lookups

## Loading Seeds

To load seeds into your data warehouse:

```bash
dbt seed
```

To load specific seeds:

```bash
dbt seed --select seed_name
```