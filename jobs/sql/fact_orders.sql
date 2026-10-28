-- BẢNG FACT cấp đơn hàng: 1 dòng = 1 đơn hàng.
-- Dùng cho các câu hỏi theo đơn: giá trị đơn trung bình, thời gian giao, review, khách hàng.
-- Giữ cả 775 đơn không có sản phẩm (items = 0) để số đơn khớp với bảng orders.
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    o.order_status,
    -- Đơn hợp lệ để tính doanh thu: không huỷ / hết hàng VÀ có ít nhất 1 sản phẩm
    o.order_status NOT IN ('canceled', 'unavailable') AND NOT o.dq_no_items AS is_valid_sale,
    o.order_purchase_timestamp,
    TO_DATE(o.order_purchase_timestamp)                AS purchase_date,
    c.state                                            AS customer_state,
    c.city                                             AS customer_city,

    -- Tổng hợp từ order_items
    COALESCE(it.items, 0)                              AS items,
    COALESCE(it.sellers, 0)                            AS sellers,
    COALESCE(it.items_value, 0)                        AS items_value,    -- tổng giá sản phẩm
    COALESCE(it.freight_value, 0)                      AS freight_value,  -- tổng phí vận chuyển
    COALESCE(it.items_value, 0) + COALESCE(it.freight_value, 0) AS order_value,

    -- Thanh toán (có thể lệch order_value do lãi trả góp / voucher)
    op.payment_total,
    op.payment_count,
    op.main_payment_type,
    op.max_installments,

    -- Giao hàng & review
    o.delivery_days,
    o.delivery_delay_days,
    o.is_late,
    r.review_score,
    r.review_has_comment,

    -- Cờ chất lượng từ silver
    o.dq_delivered_without_date,
    o.dq_carrier_before_purchase,
    o.dq_no_items,
    o.dq_no_payment,

    o.purchase_year,
    o.purchase_month
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN (
    SELECT order_id,
           COUNT(*)                  AS items,
           COUNT(DISTINCT seller_id) AS sellers,
           SUM(price)                AS items_value,
           SUM(freight_value)        AS freight_value
    FROM order_items
    GROUP BY order_id
) it ON o.order_id = it.order_id
LEFT JOIN order_payments op ON o.order_id = op.order_id
LEFT JOIN order_reviews  r  ON o.order_id = r.order_id
