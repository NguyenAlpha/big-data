-- Hiệu suất người bán: 1 dòng = 1 người bán.
-- Dùng để xếp hạng seller, tìm seller hay giao trễ / bị đánh giá thấp.
SELECT
    seller_id,
    MAX(seller_state)                                 AS seller_state,
    MAX(seller_city)                                  AS seller_city,
    COUNT(DISTINCT order_id)                          AS orders,
    COUNT(*)                                          AS items,
    COUNT(DISTINCT customer_unique_id)                AS customers,
    SUM(price)                                        AS revenue,
    ROUND(AVG(price), 2)                              AS avg_price,
    ROUND(AVG(review_score), 2)                       AS avg_review_score,
    ROUND(AVG(delivery_days), 1)                      AS avg_delivery_days,
    ROUND(100 * AVG(CAST(is_late AS INT)), 2)         AS late_delivery_pct,
    ROUND(AVG(distance_km), 1)                        AS avg_distance_km,
    MIN(purchase_date)                                AS first_sale_date,
    MAX(purchase_date)                                AS last_sale_date,
    -- Số tháng có phát sinh đơn
    COUNT(DISTINCT purchase_year * 100 + purchase_month) AS active_months
FROM fact_order_items
WHERE is_valid_sale
GROUP BY seller_id
ORDER BY revenue DESC
