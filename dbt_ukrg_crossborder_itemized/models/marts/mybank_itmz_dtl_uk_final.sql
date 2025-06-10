{{
    config(
        materialized='table',
        post_hook="UPDATE {{ this }} AS main SET main.transaction_id = temp.transaction_id FROM (SELECT transaction_id, trans_sk_guid, type FROM {{ this }} WHERE file_date >= DATEADD(day, -10, CURRENT_DATE()) AND type IN ('SALE', 'REFUND', 'CASH_ADVANCE', 'UNKNOWN') AND status IN ('PROCESSED', 'FE_REJECTED') AND exception_action_report_sk = '-1') temp WHERE main.file_date >= DATEADD(day, -10, CURRENT_DATE()) AND (main.transaction_id IS NULL OR TRIM(main.transaction_id) = '') AND main.trans_sk_guid = temp.trans_sk_guid AND main.sub_type = 'TRANSACTION_FEE' AND main.fee_type IN ('CROSSBORDER_FEE') AND main.status = 'PROCESSED' AND main.exception_action_report_sk = '-1'"
    )
}}

-- Post-processing model to update transaction_id references
-- This replicates the UPDATE query from the original BigQuery script
SELECT * FROM {{ ref('mybank_itmz_dtl_uk') }}