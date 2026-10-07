-- One row per order item (grain: order_item_id)
SELECT
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.store_id,
    o.order_timestamp,
    CAST(o.order_timestamp AS DATE) AS order_date,
    o.payment_method,
    o.order_status,
    oi.quantity,
    oi.unit_price,
    oi.line_amount,
    o.total_amount AS order_total_amount,   -- order-level: don't SUM across items
    CURRENT_TIMESTAMP AS fact_gold_processed_at
FROM {{ ref('eph_order_items') }} oi
JOIN {{ ref('eph_orders') }} o
    ON oi.order_id = o.order_id
