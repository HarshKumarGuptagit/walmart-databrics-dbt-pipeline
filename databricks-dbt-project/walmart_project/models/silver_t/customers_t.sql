{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    )
}}

SELECT
    *,
    current_timestamp() AS processed_at
FROM 
    {{ source('walmart_databricks', 'customers') }}

{% if is_incremental() %}
  WHERE updated_timestamp >= coalesce((select max(updated_timestamp) from {{ this }}), '2026-11-30')
{% endif %}
