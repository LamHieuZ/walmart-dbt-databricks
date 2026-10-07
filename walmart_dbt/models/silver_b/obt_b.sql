{{
config(
    materialized='table'
)
}}

SELECT
    *,
    current_timestamp() AS obt_processed_at
FROM {{ ref('refer') }}
