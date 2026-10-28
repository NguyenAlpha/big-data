-- BẢNG FACT chi tiết: 1 dòng = 1 sản phẩm trong 1 đơn hàng (cùng "độ hạt" với order_items).
-- Gắn đủ thông tin đơn hàng, khách, sản phẩm, người bán, thanh toán, review vào mỗi dòng
-- để nhóm Analytics truy vấn mà không cần tự join 7 bảng.
--
-- Dùng INNER JOIN với orders/customers/products/sellers vì bước profiling đã chứng minh
-- 0 khoá ngoại mồ côi; job vẫn đối chiếu lại số dòng sau khi ghi (reconciliation).
-- LEFT JOIN với review/thanh toán vì có đơn không có review hoặc thanh toán.
SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    c.customer_unique_id,

    -- Đơn hàng
    o.order_status,
    -- Đơn bị huỷ / hết hàng không được tính vào doanh thu
    o.order_status NOT IN ('canceled', 'unavailable') AS is_valid_sale,
    o.order_purchase_timestamp,
    TO_DATE(o.order_purchase_timestamp)                AS purchase_date,

    -- Sản phẩm & tiền
    p.product_category_name_english                    AS category,
    i.price,
    i.freight_value,
    i.item_total,

    -- Địa lý
    c.state AS customer_state,
    c.city  AS customer_city,
    s.state AS seller_state,
    s.city  AS seller_city,
    -- Khoảng cách đường chim bay người bán -> khách (công thức Haversine, bán kính Trái Đất 6371 km)
    ROUND(6371 * 2 * ASIN(SQRT(
        POW(SIN(RADIANS(c.lat - s.lat) / 2), 2) +
        COS(RADIANS(s.lat)) * COS(RADIANS(c.lat)) * POW(SIN(RADIANS(c.lng - s.lng) / 2), 2)
    )), 1)                                             AS distance_km,

    -- Giao hàng
    o.delivery_days,
    o.delivery_delay_days,
    o.is_late,

    -- Review & thanh toán (cấp đơn hàng, lặp lại cho mỗi sản phẩm trong đơn)
    r.review_score,
    op.main_payment_type,
    op.max_installments,

    -- Cột chia thư mục khi ghi
    o.purchase_year,
    o.purchase_month
FROM order_items i
JOIN orders    o ON i.order_id   = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN products  p ON i.product_id = p.product_id
JOIN sellers   s ON i.seller_id  = s.seller_id
LEFT JOIN order_reviews  r  ON i.order_id = r.order_id
LEFT JOIN order_payments op ON i.order_id = op.order_id
