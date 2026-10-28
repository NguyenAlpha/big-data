-- KPI theo bang (state) của khách hàng: 1 dòng = 1 bang (27 bang của Brazil).
-- Dùng để vẽ bản đồ doanh thu, so sánh chi phí vận chuyển và tốc độ giao giữa các vùng.
SELECT
    customer_state,
    COUNT(*)                                          AS orders,
    COUNT(DISTINCT customer_unique_id)                AS customers,
    SUM(items_value)                                  AS revenue,
    SUM(order_value)                                  AS gmv,
    ROUND(AVG(order_value), 2)                        AS avg_order_value,
    ROUND(AVG(freight_value), 2)                      AS avg_freight,
    -- Phí ship chiếm bao nhiêu % giá trị hàng
    ROUND(100 * SUM(freight_value) / SUM(items_value), 2) AS freight_pct_of_revenue,
    ROUND(AVG(delivery_days), 1)                      AS avg_delivery_days,
    ROUND(100 * AVG(CAST(is_late AS INT)), 2)         AS late_delivery_pct,
    ROUND(AVG(review_score), 2)                       AS avg_review_score
FROM fact_orders
WHERE is_valid_sale
GROUP BY customer_state
ORDER BY revenue DESC
