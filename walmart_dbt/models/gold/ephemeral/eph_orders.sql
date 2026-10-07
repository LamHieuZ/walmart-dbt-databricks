{{ config(materialized='ephemeral') }}

SELECT DISTINCT
    order_id,
    customer_id,
    store_id,
    order_timestamp,
    payment_method,
    order_status,
    total_amount,
    order_created_timestamp,
    order_updated_timestamp,
    order_is_active,
    order_processed_at,
    CURRENT_TIMESTAMP AS order_gold_processed_at
FROM {{ ref('obt_b') }}
WHERE order_id IS NOT NULL
