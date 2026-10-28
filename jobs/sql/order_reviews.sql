-- VIEW TRUNG GIAN: mỗi đơn hàng giữ đúng 1 review (review được trả lời gần nhất).
-- Profiling cho thấy 547 đơn có nhiều hơn 1 review; join thẳng sẽ nhân bản dòng.
-- ROW_NUMBER() OVER (PARTITION BY ...) đánh số các review trong từng đơn,
-- rồi chỉ lấy dòng số 1 - giống cách lấy "bản ghi mới nhất mỗi nhóm" trong SQL thường.
SELECT order_id, review_score, has_comment AS review_has_comment
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY review_answer_timestamp DESC, review_creation_date DESC, review_id
           ) AS rn
    FROM reviews
) ranked
WHERE rn = 1
