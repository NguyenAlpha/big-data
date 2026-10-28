-- VIEW TRUNG GIAN: gom bảng payments về 1 dòng / đơn hàng.
-- Một đơn có thể trả bằng nhiều phương thức (vd thẻ + voucher) -> nhiều dòng payments.
-- Nếu join thẳng payments vào order_items, mỗi dòng sản phẩm sẽ bị nhân bản theo
-- số lần thanh toán -> doanh thu bị cộng trùng. Vì vậy phải gom trước khi join.
SELECT
    order_id,
    SUM(payment_value)                  AS payment_total,
    COUNT(*)                            AS payment_count,
    MAX(payment_installments)           AS max_installments,
    -- Phương thức chiếm số tiền lớn nhất trong đơn
    MAX_BY(payment_type, payment_value) AS main_payment_type
FROM payments
GROUP BY order_id
