SELECT 
    o.order_id,
    o.order_item_id,
    o.customer_id,
    o.employee_id,
    o.store_id,
    o.total_amount,
    o.quantity,
    o.unit_price,
    o.line_amount
FROM 
    {{ ref('obt_b') }} AS o