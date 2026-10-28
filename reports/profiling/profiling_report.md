# Báo cáo Data Profiling - dữ liệu Olist (TRƯỚC xử lý)

> File này được sinh tự động bởi `jobs/01_profiling.py`. Cách đọc báo cáo: xem `docs/02-data-profiling.md`.

## 1. Thông tin lần chạy

| Thuộc tính | Giá trị |
|---|---|
| Thời điểm chạy | 2026-10-28 03:04:05 |
| Phiên bản Spark | 3.5.3 |
| Spark master | spark://spark-master:7077 |
| Số executor | 2 |
| Số core xử lý song song (defaultParallelism) | 4 |
| Tổng dung lượng dữ liệu gốc | 120.35 MB |
| Tổng số bản ghi | 1,550,922 |
| Thời gian profiling 9 bảng | 35.23 s |
| Thời gian kiểm tra khoá ngoại | 7.45 s |
| Tổng thời gian chạy job | 49.16 s |

## 2. Tổng quan các bảng

| Bảng | File | Dung lượng (MB) | Số dòng | Số cột | Partition | Dòng trùng hoàn toàn | Trùng khoá chính | Thời gian (s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| orders | `olist_orders_dataset.csv` | 16.84 | 99,441 | 8 | 4 | 0 | 0 | 12.9 |
| order_items | `olist_order_items_dataset.csv` | 14.72 | 112,650 | 7 | 4 | 0 | 0 | 3.49 |
| payments | `olist_order_payments_dataset.csv` | 5.51 | 103,886 | 5 | 2 | 0 | 0 | 2.92 |
| reviews | `olist_order_reviews_dataset.csv` | 13.78 | 99,224 | 7 | 1 | 0 | 814 | 3.05 |
| customers | `olist_customers_dataset.csv` | 8.62 | 99,441 | 5 | 3 | 0 | 0 | 1.89 |
| sellers | `olist_sellers_dataset.csv` | 0.17 | 3,095 | 4 | 1 | 0 | 0 | 0.92 |
| products | `olist_products_dataset.csv` | 2.27 | 32,951 | 9 | 1 | 0 | 0 | 1.64 |
| geolocation | `olist_geolocation_dataset.csv` | 58.44 | 1,000,163 | 5 | 4 | 261,831 | - | 7.71 |
| category_translation | `product_category_name_translation.csv` | 0.0 | 71 | 2 | 1 | 0 | 0 | 0.57 |

## 3. Vấn đề phát hiện khi đọc file

- Không có (Spark đọc đúng header của cả 9 file).

## 4. Kiểm tra khoá ngoại (toàn vẹn tham chiếu)

| Kiểm tra | Quan hệ | Số dòng mồ côi | % | Số giá trị khác nhau |
|---|---|---:|---:|---:|
| Đơn hàng trỏ tới khách hàng không tồn tại | `orders.customer_id -> customers.customer_id` | 0 | 0.0 | 0 |
| Dòng hàng trỏ tới đơn hàng không tồn tại | `order_items.order_id -> orders.order_id` | 0 | 0.0 | 0 |
| Dòng hàng trỏ tới sản phẩm không tồn tại | `order_items.product_id -> products.product_id` | 0 | 0.0 | 0 |
| Dòng hàng trỏ tới người bán không tồn tại | `order_items.seller_id -> sellers.seller_id` | 0 | 0.0 | 0 |
| Thanh toán trỏ tới đơn hàng không tồn tại | `payments.order_id -> orders.order_id` | 0 | 0.0 | 0 |
| Review trỏ tới đơn hàng không tồn tại | `reviews.order_id -> orders.order_id` | 0 | 0.0 | 0 |
| Đơn hàng không có dòng sản phẩm nào | `orders.order_id -> order_items.order_id` | 775 | 0.78 | 775 |
| Đơn hàng không có thông tin thanh toán | `orders.order_id -> payments.order_id` | 1 | 0.0 | 1 |
| Sản phẩm có danh mục chưa có bản dịch tiếng Anh | `products.product_category_name -> category_translation.product_category_name` | 13 | 0.04 | 2 |
| Khách hàng có mã zip không có trong bảng toạ độ | `customers.customer_zip_code_prefix -> geolocation.geolocation_zip_code_prefix` | 278 | 0.28 | 157 |
| Người bán có mã zip không có trong bảng toạ độ | `sellers.seller_zip_code_prefix -> geolocation.geolocation_zip_code_prefix` | 7 | 0.23 | 7 |

## 5. Chi tiết từng bảng

### 5.1. `orders` (99,441 dòng)

Khoá chính: `order_id`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `order_id` | string | 0 | 0.0 | 99,770 | - |
| `customer_id` | string | 0 | 0.0 | 102,585 | - |
| `order_status` | string | 0 | 0.0 | 8 | - |
| `order_purchase_timestamp` | timestamp | 0 | 0.0 | 94,718 | 0 |
| `order_approved_at` | timestamp | 160 | 0.16 | 85,174 | 0 |
| `order_delivered_carrier_date` | timestamp | 1,783 | 1.79 | 79,259 | 0 |
| `order_delivered_customer_date` | timestamp | 2,965 | 2.98 | 97,053 | 0 |
| `order_estimated_delivery_date` | timestamp | 0 | 0.0 | 454 | 0 |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Đơn 'delivered' nhưng thiếu ngày giao cho khách | 8 | 0.01 |
| Ngày duyệt đơn sớm hơn ngày mua | 0 | 0.0 |
| Ngày giao cho đơn vị vận chuyển sớm hơn ngày mua | 166 | 0.17 |
| Ngày khách nhận hàng sớm hơn ngày mua | 0 | 0.0 |
| Đơn giao trễ so với ngày dự kiến | 7,827 | 7.87 |

**Phân bố giá trị `order_status`** (top 15)

| Giá trị | Số dòng | % |
|---|---:|---:|
| delivered | 96,478 | 97.02 |
| shipped | 1,107 | 1.11 |
| canceled | 625 | 0.63 |
| unavailable | 609 | 0.61 |
| invoiced | 314 | 0.32 |
| processing | 301 | 0.3 |
| created | 5 | 0.01 |
| approved | 2 | 0.0 |

**Bản ghi mẫu**

| `order_id` | `customer_id` | `order_status` | `order_purchase_timestamp` | `order_approved_at` | `order_delivered_carrier_date` | `order_delivered_customer_date` | `order_estimated_delivery_date` |
|---|---|---|---|---|---|---|---|
| e481f51cbdc54678b7c… | 9ef432eb6251297304e… | delivered | 2017-10-02 10:56:33 | 2017-10-02 11:07:15 | 2017-10-04 19:55:00 | 2017-10-10 21:25:13 | 2017-10-18 00:00:00 |
| 53cdb2fc8bc7dce0b67… | b0830fb4747a6c6d20d… | delivered | 2018-07-24 20:41:37 | 2018-07-26 03:24:27 | 2018-07-26 14:31:00 | 2018-08-07 15:27:45 | 2018-08-13 00:00:00 |
| 47770eb9100c2d0c449… | 41ce2a54c0b03bf3443… | delivered | 2018-08-08 08:38:49 | 2018-08-08 08:55:23 | 2018-08-08 13:50:00 | 2018-08-17 18:06:29 | 2018-09-04 00:00:00 |

### 5.2. `order_items` (112,650 dòng)

Khoá chính: `order_id, order_item_id`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `order_id` | string | 0 | 0.0 | 99,770 | - |
| `order_item_id` | int | 0 | 0.0 | 21 | 0 |
| `product_id` | string | 0 | 0.0 | 32,938 | - |
| `seller_id` | string | 0 | 0.0 | 3,080 | - |
| `shipping_limit_date` | timestamp | 0 | 0.0 | 91,195 | 0 |
| `price` | double | 0 | 0.0 | 5,973 | 0 |
| `freight_value` | double | 0 | 0.0 | 7,228 | 0 |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Giá sản phẩm <= 0 | 0 | 0.0 |
| Phí vận chuyển < 0 | 0 | 0.0 |
| Phí vận chuyển = 0 (miễn phí ship) | 383 | 0.34 |

**Bản ghi mẫu**

| `order_id` | `order_item_id` | `product_id` | `seller_id` | `shipping_limit_date` | `price` | `freight_value` |
|---|---|---|---|---|---|---|
| 00010242fe8c5a6d1ba… | 1 | 4244733e06e7ecb4970… | 48436dade18ac8b2bce… | 2017-09-19 09:45:35 | 58.90 | 13.29 |
| 00018f77f2f0320c557… | 1 | e5f2d52b802189ee658… | dd7ddc04e1b6c2c6143… | 2017-05-03 11:05:13 | 239.90 | 19.93 |
| 000229ec398224ef6ca… | 1 | c777355d18b72b67abb… | 5b51032eddd242adc84… | 2018-01-18 14:48:30 | 199.00 | 17.87 |

### 5.3. `payments` (103,886 dòng)

Khoá chính: `order_id, payment_sequential`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `order_id` | string | 0 | 0.0 | 99,770 | - |
| `payment_sequential` | int | 0 | 0.0 | 30 | 0 |
| `payment_type` | string | 0 | 0.0 | 5 | - |
| `payment_installments` | int | 0 | 0.0 | 25 | 0 |
| `payment_value` | double | 0 | 0.0 | 29,289 | 0 |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Phương thức thanh toán 'not_defined' | 3 | 0.0 |
| Số kỳ trả góp = 0 | 2 | 0.0 |
| Số tiền thanh toán <= 0 | 9 | 0.01 |

**Phân bố giá trị `payment_type`** (top 15)

| Giá trị | Số dòng | % |
|---|---:|---:|
| credit_card | 76,795 | 73.92 |
| boleto | 19,784 | 19.04 |
| voucher | 5,775 | 5.56 |
| debit_card | 1,529 | 1.47 |
| not_defined | 3 | 0.0 |

**Bản ghi mẫu**

| `order_id` | `payment_sequential` | `payment_type` | `payment_installments` | `payment_value` |
|---|---|---|---|---|
| b81ef226f3fe1789b1e… | 1 | credit_card | 8 | 99.33 |
| a9810da82917af2d9ae… | 1 | credit_card | 1 | 24.39 |
| 25e8ea4e93396b6fa0d… | 1 | credit_card | 1 | 65.71 |

### 5.4. `reviews` (99,224 dòng)

Khoá chính: `review_id`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `review_id` | string | 0 | 0.0 | 93,594 | - |
| `order_id` | string | 0 | 0.0 | 99,105 | - |
| `review_score` | int | 0 | 0.0 | 5 | 0 |
| `review_comment_title` | string | 87,658 | 88.34 | 4,655 | - |
| `review_comment_message` | string | 58,256 | 58.71 | 35,020 | - |
| `review_creation_date` | timestamp | 0 | 0.0 | 592 | 0 |
| `review_answer_timestamp` | timestamp | 0 | 0.0 | 101,384 | 0 |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Điểm review ngoài khoảng 1-5 | 0 | 0.0 |
| Thời điểm trả lời sớm hơn ngày tạo review | 0 | 0.0 |
| Review không có nội dung bình luận | 58,256 | 58.71 |

**Chỉ số bổ sung**

| Chỉ số | Giá trị |
|---|---:|
| Số đơn hàng có nhiều hơn 1 review | 547 |

**Phân bố giá trị `review_score`** (top 15)

| Giá trị | Số dòng | % |
|---|---:|---:|
| 5 | 57,328 | 57.78 |
| 4 | 19,142 | 19.29 |
| 1 | 11,424 | 11.51 |
| 3 | 8,179 | 8.24 |
| 2 | 3,151 | 3.18 |

**Bản ghi mẫu**

| `review_id` | `order_id` | `review_score` | `review_comment_title` | `review_comment_message` | `review_creation_date` | `review_answer_timestamp` |
|---|---|---|---|---|---|---|
| 7bc2406110b926393aa… | 73fc7af87114b39712e… | 4 | *NULL* | *NULL* | 2018-01-18 00:00:00 | 2018-01-18 21:46:59 |
| 80e641a11e56f04c1ad… | a548910a1c6147796b9… | 5 | *NULL* | *NULL* | 2018-03-10 00:00:00 | 2018-03-11 03:05:13 |
| 228ce5500dc1d8e020d… | f9e4b658b201a9f2ecd… | 5 | *NULL* | *NULL* | 2018-02-17 00:00:00 | 2018-02-18 14:36:24 |

### 5.5. `customers` (99,441 dòng)

Khoá chính: `customer_id`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `customer_id` | string | 0 | 0.0 | 102,585 | - |
| `customer_unique_id` | string | 0 | 0.0 | 103,785 | - |
| `customer_zip_code_prefix` | string | 0 | 0.0 | 15,475 | - |
| `customer_city` | string | 0 | 0.0 | 4,211 | - |
| `customer_state` | string | 0 | 0.0 | 27 | - |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Mã zip không đủ 5 chữ số | 0 | 0.0 |

**Chỉ số bổ sung**

| Chỉ số | Giá trị |
|---|---:|
| Số khách hàng thật (customer_unique_id khác nhau) | 96,096 |

**Phân bố giá trị `customer_state`** (top 15)

| Giá trị | Số dòng | % |
|---|---:|---:|
| SP | 41,746 | 41.98 |
| RJ | 12,852 | 12.92 |
| MG | 11,635 | 11.7 |
| RS | 5,466 | 5.5 |
| PR | 5,045 | 5.07 |
| SC | 3,637 | 3.66 |
| BA | 3,380 | 3.4 |
| DF | 2,140 | 2.15 |
| ES | 2,033 | 2.04 |
| GO | 2,020 | 2.03 |
| PE | 1,652 | 1.66 |
| CE | 1,336 | 1.34 |
| PA | 975 | 0.98 |
| MT | 907 | 0.91 |
| MA | 747 | 0.75 |

**Bản ghi mẫu**

| `customer_id` | `customer_unique_id` | `customer_zip_code_prefix` | `customer_city` | `customer_state` |
|---|---|---|---|---|
| 06b8999e2fba1a1fbc8… | 861eff4711a542e4b93… | 14409 | franca | SP |
| 18955e83d337fd6b2de… | 290c77bc529b7ac935b… | 09790 | sao bernardo do cam… | SP |
| 4e7b3e00288586ebd08… | 060e732b5b29e8181a1… | 01151 | sao paulo | SP |

### 5.6. `sellers` (3,095 dòng)

Khoá chính: `seller_id`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `seller_id` | string | 0 | 0.0 | 3,080 | - |
| `seller_zip_code_prefix` | string | 0 | 0.0 | 2,123 | - |
| `seller_city` | string | 0 | 0.0 | 618 | - |
| `seller_state` | string | 0 | 0.0 | 24 | - |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Mã zip không đủ 5 chữ số | 0 | 0.0 |

**Phân bố giá trị `seller_state`** (top 15)

| Giá trị | Số dòng | % |
|---|---:|---:|
| SP | 1,849 | 59.74 |
| PR | 349 | 11.28 |
| MG | 244 | 7.88 |
| SC | 190 | 6.14 |
| RJ | 171 | 5.53 |
| RS | 129 | 4.17 |
| GO | 40 | 1.29 |
| DF | 30 | 0.97 |
| ES | 23 | 0.74 |
| BA | 19 | 0.61 |
| CE | 13 | 0.42 |
| PE | 9 | 0.29 |
| PB | 6 | 0.19 |
| MS | 5 | 0.16 |
| RN | 5 | 0.16 |

**Bản ghi mẫu**

| `seller_id` | `seller_zip_code_prefix` | `seller_city` | `seller_state` |
|---|---|---|---|
| 3442f8959a84dea7ee1… | 13023 | campinas | SP |
| d1b65fc7debc3361ea8… | 13844 | mogi guacu | SP |
| ce3ad9de960102d0677… | 20031 | rio de janeiro | RJ |

### 5.7. `products` (32,951 dòng)

Khoá chính: `product_id`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `product_id` | string | 0 | 0.0 | 32,938 | - |
| `product_category_name` | string | 610 | 1.85 | 68 | - |
| `product_name_lenght` | int | 610 | 1.85 | 67 | 0 |
| `product_description_lenght` | int | 610 | 1.85 | 2,843 | 0 |
| `product_photos_qty` | int | 610 | 1.85 | 19 | 0 |
| `product_weight_g` | double | 2 | 0.01 | 2,165 | 0 |
| `product_length_cm` | double | 2 | 0.01 | 103 | 0 |
| `product_height_cm` | double | 2 | 0.01 | 105 | 0 |
| `product_width_cm` | double | 2 | 0.01 | 99 | 0 |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Sản phẩm không có danh mục (category) | 610 | 1.85 |
| Khối lượng = 0 gram | 4 | 0.01 |

**Bản ghi mẫu**

| `product_id` | `product_category_name` | `product_name_lenght` | `product_description_lenght` | `product_photos_qty` | `product_weight_g` | `product_length_cm` | `product_height_cm` | `product_width_cm` |
|---|---|---|---|---|---|---|---|---|
| 1e9e8ef04dbcff4541e… | perfumaria | 40 | 287 | 1 | 225 | 16 | 10 | 14 |
| 3aa071139cb16b67ca9… | artes | 44 | 276 | 1 | 1000 | 30 | 18 | 20 |
| 96bd76ec8810374ed1b… | esporte_lazer | 46 | 250 | 1 | 154 | 18 | 9 | 15 |

### 5.8. `geolocation` (1,000,163 dòng)

Khoá chính: `(không có khoá duy nhất)`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `geolocation_zip_code_prefix` | string | 0 | 0.0 | 19,855 | - |
| `geolocation_lat` | double | 0 | 0.0 | 750,914 | 0 |
| `geolocation_lng` | double | 0 | 0.0 | 690,215 | 0 |
| `geolocation_city` | string | 0 | 0.0 | 8,324 | - |
| `geolocation_state` | string | 0 | 0.0 | 27 | - |

**Kiểm tra luật nghiệp vụ**

| Luật | Số dòng vi phạm | % |
|---|---:|---:|
| Toạ độ nằm ngoài lãnh thổ Brazil | 42 | 0.0 |
| Tên thành phố có ký tự có dấu / đặc biệt (cần chuẩn hoá) | 73,459 | 7.34 |

**Chỉ số bổ sung**

| Chỉ số | Giá trị |
|---|---:|
| Số mã zip khác nhau | 19,015 |
| Trung bình số dòng toạ độ / mã zip | 52.6 |
| Nhiều nhất số dòng toạ độ / mã zip | 1,146 |
| Mã zip có >1 cách viết tên thành phố (dữ liệu gốc) | 8,556 |
| Mã zip có >1 cách viết tên thành phố (sau khi chuẩn hoá) | 547 |
| Số cách viết khác nhau của tên thành phố gốc | 8,011 |
| Số tên thành phố sau khi chuẩn hoá | 5,964 |

**Phân bố giá trị `geolocation_state`** (top 15)

| Giá trị | Số dòng | % |
|---|---:|---:|
| SP | 404,268 | 40.42 |
| MG | 126,336 | 12.63 |
| RJ | 121,169 | 12.11 |
| RS | 61,851 | 6.18 |
| PR | 57,859 | 5.78 |
| SC | 38,328 | 3.83 |
| BA | 36,045 | 3.6 |
| GO | 20,139 | 2.01 |
| ES | 16,748 | 1.67 |
| PE | 16,432 | 1.64 |
| DF | 12,986 | 1.3 |
| MT | 12,031 | 1.2 |
| CE | 11,674 | 1.17 |
| PA | 10,853 | 1.09 |
| MS | 10,431 | 1.04 |

**Bản ghi mẫu**

| `geolocation_zip_code_prefix` | `geolocation_lat` | `geolocation_lng` | `geolocation_city` | `geolocation_state` |
|---|---|---|---|---|
| 01037 | -23.54562128115268 | -46.63929204800168 | sao paulo | SP |
| 01046 | -23.546081127035535 | -46.64482029837157 | sao paulo | SP |
| 01046 | -23.54612896641469 | -46.64295148361138 | sao paulo | SP |

### 5.9. `category_translation` (71 dòng)

Khoá chính: `product_category_name`

| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) | Sai định dạng |
|---|---|---:|---:|---:|---:|
| `product_category_name` | string | 0 | 0.0 | 67 | - |
| `product_category_name_english` | string | 0 | 0.0 | 71 | - |

**Bản ghi mẫu**

| `product_category_name` | `product_category_name_english` |
|---|---|
| beleza_saude | health_beauty |
| informatica_acessor… | computers_accessori… |
| automotivo | auto |

