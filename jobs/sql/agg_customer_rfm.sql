-- Hồ sơ khách hàng theo mô hình RFM: 1 dòng = 1 khách thật (customer_unique_id).
--   Recency   - bao nhiêu ngày kể từ lần mua cuối (so với ngày mua mới nhất trong dữ liệu)
--   Frequency - số đơn đã mua
--   Monetary  - tổng tiền đã chi
-- Đây là đầu vào chuẩn cho bài toán phân cụm khách hàng (clustering) ở giai đoạn Analytics.
WITH ref AS (
    SELECT MAX(purchase_date) AS ref_date FROM fact_orders
)
SELECT
    f.customer_unique_id,
    -- Bang của lần mua gần nhất (khách có thể đã chuyển nhà)
    MAX_BY(f.customer_state, f.order_purchase_timestamp) AS customer_state,
    MIN(f.purchase_date)                      AS first_purchase_date,
    MAX(f.purchase_date)                      AS last_purchase_date,
    DATEDIFF(MAX(ref.ref_date), MAX(f.purchase_date)) AS recency_days,
    COUNT(*)                                  AS frequency,
    SUM(f.order_value)                        AS monetary,
    ROUND(AVG(f.order_value), 2)              AS avg_order_value,
    ROUND(AVG(f.review_score), 2)             AS avg_review_score
FROM fact_orders f
CROSS JOIN ref
WHERE f.is_valid_sale
GROUP BY f.customer_unique_id
