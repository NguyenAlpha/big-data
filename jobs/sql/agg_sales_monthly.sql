-- KPI bán hàng theo tháng: 1 dòng = 1 tháng.
-- revenue = tổng giá sản phẩm; gmv = revenue + phí vận chuyển (tổng tiền khách trả cho hàng).
-- Chỉ tính đơn hợp lệ (không huỷ, không hết hàng).
SELECT
    purchase_year,
    purchase_month,
    COUNT(*)                                   AS orders,
    COUNT(DISTINCT customer_unique_id)         AS customers,
    SUM(items)                                 AS items,
    SUM(items_value)                           AS revenue,
    SUM(freight_value)                         AS freight,
    SUM(order_value)                           AS gmv,
    ROUND(AVG(order_value), 2)                 AS avg_order_value,
    ROUND(AVG(review_score), 2)                AS avg_review_score,
    ROUND(AVG(delivery_days), 1)               AS avg_delivery_days,
    -- % đơn giao trễ, chỉ tính trên đơn đã giao (is_late NULL bị AVG bỏ qua)
    ROUND(100 * AVG(CAST(is_late AS INT)), 2)  AS late_delivery_pct
FROM fact_orders
WHERE is_valid_sale
GROUP BY purchase_year, purchase_month
ORDER BY purchase_year, purchase_month
