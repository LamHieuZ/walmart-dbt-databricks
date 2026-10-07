{{ config(materialized='ephemeral') }}

SELECT DISTINCT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_amount,
    order_item_created_timestamp,
    order_item_updated_timestamp,
    order_item_is_active,
    order_item_processed_at,
    CURRENT_TIMESTAMP AS order_item_gold_processed_at
FROM {{ ref('obt_b') }}
WHERE order_item_id IS NOT NULL
