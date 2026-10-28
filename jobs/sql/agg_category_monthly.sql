-- Doanh thu theo danh mục sản phẩm và tháng: 1 dòng = 1 danh mục x 1 tháng.
-- Tính từ fact_order_items (cấp sản phẩm) vì 1 đơn có thể chứa nhiều danh mục.
SELECT
    purchase_year,
    purchase_month,
    category,
    COUNT(DISTINCT order_id)                   AS orders,
    COUNT(*)                                   AS items,
    SUM(price)                                 AS revenue,
    SUM(freight_value)                         AS freight,
    ROUND(AVG(price), 2)                       AS avg_price,
    -- Điểm review của đơn chứa sản phẩm thuộc danh mục (tính theo dòng sản phẩm)
    ROUND(AVG(review_score), 2)                AS avg_review_score
FROM fact_order_items
WHERE is_valid_sale
GROUP BY purchase_year, purchase_month, category
ORDER BY purchase_year, purchase_month, revenue DESC
