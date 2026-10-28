# Bàn giao dữ liệu: Processing → Analytics

**Bộ dữ liệu:** Olist E-commerce (Brazil), 09/2016 – 10/2018
**Nhóm bàn giao:** Processing (Storage → Processing)
**Nhóm nhận:** Analytics (Processing → Analytics), và Presentation nếu dùng trực tiếp
**Mã nguồn:** https://github.com/NguyenAlpha/big-data

Tài liệu này mô tả **những gì bạn nhận được, ý nghĩa từng cột, và những điểm cần lưu ý**
khi phân tích. Bạn không cần đọc code của nhóm Processing để dùng được dữ liệu.

---

## 1. Bạn nhận được gì

7 bảng ở định dạng **Parquet** (thư mục `gold/` trong file zip). Dữ liệu đã được làm sạch,
join sẵn và kiểm tra.

| Bảng | 1 dòng = | Số dòng | Dùng cho |
|---|---|---:|---|
| `fact_order_items` | 1 sản phẩm trong 1 đơn hàng | 112.650 | Phân tích theo sản phẩm, danh mục, người bán, khoảng cách giao hàng |
| `fact_orders` | 1 đơn hàng | 99.441 | Phân tích theo đơn: giá trị đơn, giao hàng, review, khách hàng |
| `agg_sales_monthly` | 1 tháng | 24 | Xu hướng doanh thu, số đơn, giao trễ theo thời gian |
| `agg_category_monthly` | 1 danh mục × 1 tháng | 1.282 | Danh mục bán chạy, mùa vụ |
| `agg_customer_state` | 1 bang (27 bang) | 27 | Bản đồ doanh thu, phí ship, tốc độ giao theo vùng |
| `agg_seller_performance` | 1 người bán | 3.053 | Xếp hạng, đánh giá người bán |
| `agg_customer_rfm` | 1 khách hàng | 94.983 | Phân cụm khách hàng (RFM / clustering) |

- Hai bảng `fact_*` là **dữ liệu chi tiết**: muốn tự tính chỉ số mới thì dùng 2 bảng này.
- Năm bảng `agg_*` là **chỉ số tính sẵn**: dùng ngay cho dashboard.
- Hai bảng fact được chia thư mục theo `purchase_year=YYYY/purchase_month=M/`. Khi đọc,
  trỏ vào **thư mục gốc của bảng**, Spark/Pandas sẽ tự ghép lại và tự thêm 2 cột năm/tháng.

---

## 2. Năm quy tắc bắt buộc khi phân tích

1. **Chỉ tính doanh thu trên đơn hợp lệ:** `WHERE is_valid_sale`.
   Đơn hợp lệ = không bị huỷ / hết hàng **và** có ít nhất 1 sản phẩm (98.199 trên 99.441 đơn).
   Các bảng `agg_*` đã lọc sẵn.
2. **Đếm khách hàng bằng `customer_unique_id`**, **không** dùng `customer_id`. Olist sinh
   `customer_id` mới cho **mỗi đơn**: có 99.441 `customer_id` nhưng chỉ 96.096 khách thật.
3. **Vẽ xu hướng theo thời gian chỉ dùng khoảng 2017-01 → 2018-08.** Các tháng 2016-09
   (2 đơn), 2016-12 (1 đơn), 2018-09 (1 đơn) không đầy đủ. 2018-10 chỉ có đơn bị huỷ.
4. **Không cộng `review_score` theo dòng sản phẩm** rồi coi là điểm của đơn. Review gắn ở
   cấp đơn hàng. Trong `fact_order_items`, đơn có 3 sản phẩm thì điểm review lặp lại 3 lần.
   Phân tích review nên dùng `fact_orders`.
5. **Không tự join lại bảng thanh toán/review** vào fact (dữ liệu gốc có đơn nhiều thanh
   toán, nhiều review). Join thẳng sẽ nhân bản dòng và làm doanh thu sai khoảng +5%. Các cột
   cần thiết đã được gom sẵn vào fact.

---

## 3. Định nghĩa các chỉ số

| Chỉ số | Định nghĩa |
|---|---|
| `revenue` | Tổng **giá sản phẩm** (`price`), chưa gồm phí vận chuyển |
| `freight` / `freight_value` | Tổng phí vận chuyển |
| `gmv` / `order_value` | `revenue` + phí vận chuyển = số tiền khách trả cho hàng |
| `payment_total` | Tổng tiền thanh toán thực tế. Có thể khác `order_value` do lãi trả góp hoặc voucher. |
| `avg_order_value` | Trung bình `order_value` mỗi đơn |
| `delivery_days` | Số ngày từ lúc mua đến lúc khách nhận hàng. NULL nếu chưa giao. |
| `delivery_delay_days` | Ngày nhận − ngày dự kiến. **Âm = giao sớm**, dương = trễ. |
| `is_late` | Ngày nhận > ngày dự kiến. NULL nếu chưa giao. |
| `late_delivery_pct` | % đơn **đã giao** bị trễ (đơn chưa giao không tính) |
| `distance_km` | Khoảng cách đường chim bay từ người bán đến khách, tính theo toạ độ mã zip |
| `recency_days` | Số ngày từ lần mua cuối của khách đến **2018-10-17** (ngày mua mới nhất trong dữ liệu) |
| `frequency` | Số đơn hợp lệ của khách |
| `monetary` | Tổng `order_value` của khách |

- **Đơn vị tiền:** Real Brazil (BRL). Các cột tiền có kiểu `decimal(…, 2)`.
- **Thời gian:** giờ địa phương Brazil, giữ nguyên như dữ liệu gốc, không có múi giờ.
- **Danh mục sản phẩm** (`category`): tên **tiếng Anh**. 610 sản phẩm không có danh mục được
  gán `unknown`.

---

## 4. Từ điển cột

### `fact_orders` (1 dòng = 1 đơn)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `order_id` | string | Khoá chính |
| `customer_id` | string | ID khách **theo đơn** (xem quy tắc 2) |
| `customer_unique_id` | string | ID khách thật |
| `order_status` | string | `delivered`, `shipped`, `canceled`, `unavailable`, `invoiced`, `processing`, `created`, `approved` |
| `is_valid_sale` | boolean | Đơn được tính doanh thu (quy tắc 1) |
| `order_purchase_timestamp` / `purchase_date` | timestamp / date | Thời điểm / ngày mua |
| `customer_state`, `customer_city` | string | Bang (2 chữ cái, vd `SP`) và thành phố (chữ thường, không dấu) |
| `items`, `sellers` | bigint | Số sản phẩm, số người bán trong đơn |
| `items_value`, `freight_value`, `order_value` | decimal | Tiền hàng, phí ship, tổng (mục 3) |
| `payment_total`, `payment_count` | decimal / bigint | Tổng thanh toán, số lần thanh toán |
| `main_payment_type` | string | Phương thức chiếm nhiều tiền nhất: `credit_card`, `boleto`, `voucher`, `debit_card`, `not_defined`. NULL nếu đơn không có thanh toán. |
| `max_installments` | int | Số kỳ trả góp lớn nhất |
| `delivery_days`, `delivery_delay_days`, `is_late` | int / int / boolean | Mục 3 |
| `review_score` | int | Điểm 1-5 (review mới nhất của đơn). NULL nếu không có review. |
| `review_has_comment` | boolean | Khách có viết bình luận hay chỉ chấm sao |
| `dq_*` | boolean | Cờ chất lượng dữ liệu (mục 5) |
| `purchase_year`, `purchase_month` | int | Năm, tháng mua (cột chia thư mục) |

### `fact_order_items` (1 dòng = 1 sản phẩm trong 1 đơn)

Có các cột định danh và đơn hàng giống `fact_orders`, cộng thêm:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `order_item_id` | int | Thứ tự sản phẩm trong đơn. Khoá chính là (`order_id`, `order_item_id`). |
| `product_id`, `seller_id` | string | Sản phẩm, người bán |
| `category` | string | Danh mục (tiếng Anh) |
| `price`, `freight_value`, `item_total` | decimal | Giá, phí ship, tổng của dòng này |
| `seller_state`, `seller_city` | string | Vị trí người bán |
| `distance_km` | double | Mục 3. NULL nếu thiếu toạ độ. |
| `review_score`, `main_payment_type`, `max_installments` | | Thông tin **cấp đơn**, lặp lại cho mỗi sản phẩm (quy tắc 4) |

### Các bảng `agg_*`

Tên cột tự giải thích theo định nghĩa ở mục 3. Schema đầy đủ (tên cột và kiểu) có trong
`SCHEMA.md` đi kèm file zip.

---

## 5. Cờ chất lượng dữ liệu (`dq_*`)

Nhóm Processing **không xoá** các đơn bất thường mà **gắn cờ** (true/false, không bao giờ NULL).
Bạn tự quyết định có lọc hay không.

| Cờ (trong `fact_orders`) | Ý nghĩa | Số đơn |
|---|---|---:|
| `dq_delivered_without_date` | Trạng thái `delivered` nhưng thiếu ngày khách nhận | 8 |
| `dq_carrier_before_purchase` | Ngày giao cho đơn vị vận chuyển sớm hơn ngày mua | 166 |
| `dq_no_items` | Đơn không có sản phẩm nào (đã bị loại khỏi `is_valid_sale`) | 775 |
| `dq_no_payment` | Đơn không có thông tin thanh toán | 1 |

Ví dụ phân tích thời gian giao hàng mà bỏ qua dữ liệu mâu thuẫn:
```sql
WHERE order_status = 'delivered' AND NOT dq_carrier_before_purchase
```

---

## 6. Cách đọc dữ liệu

**PySpark / Spark SQL:**
```python
df = spark.read.parquet("gold/fact_orders")
df.createOrReplaceTempView("fact_orders")
spark.sql("""
    SELECT customer_state, ROUND(AVG(delivery_days), 1) AS avg_days
    FROM fact_orders
    WHERE is_valid_sale AND purchase_year = 2018
    GROUP BY customer_state ORDER BY avg_days DESC
""").show()
```

**Pandas** (cần `pip install pandas pyarrow`):
```python
import pandas as pd
monthly = pd.read_parquet("gold/agg_sales_monthly")
orders  = pd.read_parquet("gold/fact_orders")        # tự ghép các thư mục năm/tháng
```

**Power BI:** *Get Data → Parquet*, chọn file `.parquet` trong thư mục `gold/agg_*`
(mỗi bảng tổng hợp có đúng 1 file). Với bảng fact, dùng *Get Data → Folder* để gộp nhiều file.

---

## 7. Dữ liệu đã được kiểm tra thế nào

Trước khi bàn giao, pipeline tự động kiểm tra **11 cặp số liệu phải khớp nhau**: số dòng và
tổng tiền giữ nguyên sau khi join, và tổng theo tháng = theo danh mục = theo bang = theo người
bán. Kết quả: **11/11 đạt**. Doanh thu đơn hợp lệ là **13.494.400,74 BRL** ở mọi bảng. Chi
tiết trong `gold_report.md` đi kèm.

Bạn có thể dùng con số này để tự kiểm tra sau khi đọc dữ liệu:
```python
spark.sql("SELECT SUM(revenue) FROM agg_sales_monthly").show()   # phải ra 13494400.74
```

---

## 8. Gợi ý hướng phân tích

Đây là những điểm nhóm Processing nhận thấy khi kiểm tra dữ liệu, **chưa được phân tích sâu**:

- Tỷ lệ giao trễ tăng vọt vào 11/2017 (Black Friday, 14,3%) và 2-3/2018 (16-21%), cùng lúc
  điểm review trung bình giảm thấp nhất. **Giao trễ có làm giảm điểm review không?**
- **97% khách chỉ mua 1 lần.** Nhóm khách nào quay lại? (gợi ý: `agg_customer_rfm`)
- Bang SP chiếm khoảng 38% doanh thu, giao nhanh nhất và phí ship thấp nhất. **Khoảng cách
  (`distance_km`) ảnh hưởng thế nào đến phí ship và thời gian giao?**
- Danh mục doanh thu cao nhất: `health_beauty`, `watches_gifts`, `bed_bath_table`.

---

## 9. Tạo lại dữ liệu và liên hệ

Nếu cần dữ liệu ở **quy mô lớn hơn** (x5, x10, x20 để thử hiệu năng), hoặc cần **thêm cột /
thêm bảng tổng hợp**, hãy liên hệ nhóm Processing. Mỗi bảng gold là một file SQL trong
`jobs/sql/`, nên thêm bảng mới khá nhanh.

Tự tạo lại toàn bộ từ dữ liệu gốc: xem `README.md` trong repo (cần Docker và bộ CSV từ
Kaggle, khoảng 3 phút).
