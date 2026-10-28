# Báo cáo làm sạch dữ liệu - raw CSV → silver Parquet

> Sinh tự động bởi `jobs/02_clean.py`. Giải thích chi tiết: `docs/03-lam-sach-du-lieu.md`.

## 1. Thông tin lần chạy

| Thuộc tính | Giá trị |
|---|---|
| Thời điểm chạy | 2026-10-28 03:02:29 |
| Spark master | spark://spark-master:7077 (2 executor, 4 core) |
| Dung lượng CSV gốc | 120.35 MB |
| Dung lượng Parquet silver | 40.37 MB |
| Số dòng đầu vào / đầu ra | 1,550,922 / 569,698 |
| Thời gian làm sạch + ghi | 46.3 s |
| Thời gian kiểm tra lại | 10.05 s |
| Tổng thời gian | 70.07 s |

## 2. Tổng quan trước / sau

| Bảng silver | Nguồn CSV | Dòng trước | Dòng sau | Cột trước → sau | CSV (MB) | Parquet (MB) | Số file | Trùng khoá sau | Thời gian (s) |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|
| geolocation | `olist_geolocation_dataset.csv` | 1,000,163 | 19,010 | 5 → 6 | 58.44 | 0.5 | 2 | 0 | 19.91 |
| customers | `olist_customers_dataset.csv` + `olist_geolocation_dataset.csv` | 99,441 | 99,441 | 5 → 8 | 8.62 | 7.82 | 5 | 0 | 4.82 |
| sellers | `olist_sellers_dataset.csv` + `olist_geolocation_dataset.csv` | 3,095 | 3,095 | 4 → 7 | 0.17 | 0.17 | 1 | 0 | 1.51 |
| products | `olist_products_dataset.csv` + `product_category_name_translation.csv` | 32,951 | 32,951 | 9 → 13 | 2.27 | 1.32 | 2 | 0 | 2.82 |
| orders | `olist_orders_dataset.csv` | 99,441 | 99,441 | 8 → 17 | 16.84 | 9.98 | 25 | 0 | 7.91 |
| order_items | `olist_order_items_dataset.csv` | 112,650 | 112,650 | 7 → 8 | 14.72 | 7.87 | 5 | 0 | 2.4 |
| payments | `olist_order_payments_dataset.csv` | 103,886 | 103,886 | 5 → 8 | 5.51 | 3.59 | 4 | 0 | 1.92 |
| reviews | `olist_order_reviews_dataset.csv` | 99,224 | 99,224 | 7 → 9 | 13.78 | 9.12 | 5 | 0 | 4.45 |

## 3. Các thao tác đã áp dụng

Cột *Số dòng*: số dòng bị tác động; `-` = áp dụng cho toàn bộ bảng.

### `geolocation`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Loại bỏ | Dòng trùng lặp hoàn toàn | 261,831 |
| Loại bỏ | Toạ độ nằm ngoài lãnh thổ Brazil | 33 |
| Sửa | Chuẩn hoá tên thành phố (chữ thường, bỏ dấu, bỏ ký tự rác) | 63,454 |
| Sửa | Gộp các điểm toạ độ của cùng 1 mã zip thành 1 dòng (toạ độ trung bình, tên thành phố/bang phổ biến nhất) | 738,299 |

### `customers`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Loại bỏ | Trùng khoá chính `customer_id` | 0 |
| Sửa | Chuẩn hoá tên thành phố | 0 |
| Bổ sung | Thêm toạ độ `lat`, `lng` từ bảng geolocation (broadcast join) | - |
| Gắn cờ | `dq_zip_not_in_geolocation`: mã zip không có trong bảng toạ độ | 279 |

### `sellers`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Loại bỏ | Trùng khoá chính `seller_id` | 0 |
| Sửa | Chuẩn hoá tên thành phố | 25 |
| Bổ sung | Thêm toạ độ `lat`, `lng` từ bảng geolocation (broadcast join) | - |
| Gắn cờ | `dq_zip_not_in_geolocation`: mã zip không có trong bảng toạ độ | 7 |

### `products`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Sửa | Đổi tên cột sai chính tả `product_name_lenght`, `product_description_lenght` -> `..._length` | - |
| Loại bỏ | Trùng khoá chính `product_id` | 0 |
| Sửa | Danh mục rỗng -> 'unknown'; gắn cờ `dq_missing_category` | 610 |
| Sửa | Khối lượng 0 gram -> NULL; gắn cờ `dq_zero_weight` | 4 |
| Bổ sung | Thêm tên danh mục tiếng Anh (broadcast join bảng dịch + 2 bản dịch thủ công) | 13 |

### `orders`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Sửa | Ép 5 cột ngày giờ từ chuỗi sang timestamp | - |
| Loại bỏ | Trùng khoá chính `order_id` | 0 |
| Bổ sung | Thêm cột `purchase_year`, `purchase_month`, `delivery_days`, `delivery_delay_days`, `is_late` | - |
| Gắn cờ | `dq_delivered_without_date`: Đơn 'delivered' nhưng thiếu ngày khách nhận hàng | 8 |
| Gắn cờ | `dq_carrier_before_purchase`: Ngày giao cho đơn vị vận chuyển sớm hơn ngày mua | 166 |
| Gắn cờ | `dq_no_items`: Đơn không có dòng sản phẩm nào | 775 |
| Gắn cờ | `dq_no_payment`: Đơn không có thông tin thanh toán | 1 |

### `order_items`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Sửa | Ép kiểu: `order_item_id` -> int, ngày -> timestamp, tiền -> decimal(12,2) | - |
| Loại bỏ | Trùng khoá chính (`order_id`, `order_item_id`) | 0 |
| Bổ sung | Thêm `item_total` = giá + phí vận chuyển | - |

### `payments`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Sửa | Ép kiểu số và tiền decimal(12,2) | - |
| Loại bỏ | Trùng khoá chính (`order_id`, `payment_sequential`) | 0 |
| Gắn cờ | `dq_undefined_type`: phương thức 'not_defined' (đều là đơn đã huỷ) | 3 |
| Gắn cờ | `dq_zero_installments`: số kỳ trả góp = 0 | 2 |
| Gắn cờ | `dq_zero_value`: số tiền = 0 (chủ yếu voucher 0 đồng) | 9 |

### `reviews`

| Loại | Thao tác | Số dòng |
|---|---|---:|
| Loại bỏ | Trùng khoá chính (`review_id`, `order_id`) | 0 |
| Sửa | Bình luận nhiều dòng -> 1 dòng (thay xuống dòng bằng dấu cách) | 3,852 |
| Bổ sung | Thêm `has_comment` (khách có viết bình luận hay chỉ chấm sao) | - |
| Gắn cờ | `dq_review_id_reused`: cùng `review_id` dùng cho nhiều đơn | 1,603 |

## 4. Schema và giá trị NULL của các bảng silver

### `geolocation` (19,010 dòng, khoá: `zip_code_prefix`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `zip_code_prefix` | string | 0 | 0.0 |
| `city` | string | 0 | 0.0 |
| `state` | string | 0 | 0.0 |
| `lat` | double | 0 | 0.0 |
| `lng` | double | 0 | 0.0 |
| `n_points` | bigint | 0 | 0.0 |

**Bản ghi mẫu**

| `zip_code_prefix` | `city` | `state` | `lat` | `lng` | `n_points` |
|---|---|---|---|---|---|
| 12230 | sao jose dos campos | SP | -23.22588 | -45.887548 | 169 |
| 12232 | sao jose dos campos | SP | -23.25729 | -45.8876 | 88 |

### `customers` (99,441 dòng, khoá: `customer_id`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `zip_code_prefix` | string | 0 | 0.0 |
| `customer_id` | string | 0 | 0.0 |
| `customer_unique_id` | string | 0 | 0.0 |
| `city` | string | 0 | 0.0 |
| `state` | string | 0 | 0.0 |
| `lat` | double | 279 | 0.28 |
| `lng` | double | 279 | 0.28 |
| `dq_zip_not_in_geolocation` | boolean | 0 | 0.0 |

**Bản ghi mẫu**

| `zip_code_prefix` | `customer_id` | `customer_unique_id` | `city` | `state` | `lat` | `lng` | `dq_zip_not_in_geolocation` |
|---|---|---|---|---|---|---|---|
| 06273 | 00012a2ce6f8dcda20d… | 248ffe10d632bebe4f7… | osasco | SP | -23.499149 | -46.767812 | False |
| 35550 | 000161a058600d5901f… | b0015e09bb4b6e47c52… | itapecerica | MG | -20.469287 | -45.123459 | False |

### `sellers` (3,095 dòng, khoá: `seller_id`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `zip_code_prefix` | string | 0 | 0.0 |
| `seller_id` | string | 0 | 0.0 |
| `city` | string | 0 | 0.0 |
| `state` | string | 0 | 0.0 |
| `lat` | double | 7 | 0.23 |
| `lng` | double | 7 | 0.23 |
| `dq_zip_not_in_geolocation` | boolean | 0 | 0.0 |

**Bản ghi mẫu**

| `zip_code_prefix` | `seller_id` | `city` | `state` | `lat` | `lng` | `dq_zip_not_in_geolocation` |
|---|---|---|---|---|---|---|
| 09080 | 0015a82c2db000af6aa… | santo andre | SP | -23.640202 | -46.542079 | False |
| 29156 | 001cca7ae9ae17fb1ca… | cariacica | ES | -20.279224 | -40.411514 | False |

### `products` (32,951 dòng, khoá: `product_id`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `product_id` | string | 0 | 0.0 |
| `product_category_name` | string | 0 | 0.0 |
| `product_category_name_english` | string | 0 | 0.0 |
| `product_name_length` | int | 610 | 1.85 |
| `product_description_length` | int | 610 | 1.85 |
| `product_photos_qty` | int | 610 | 1.85 |
| `product_weight_g` | double | 6 | 0.02 |
| `product_length_cm` | double | 2 | 0.01 |
| `product_height_cm` | double | 2 | 0.01 |
| `product_width_cm` | double | 2 | 0.01 |
| `dq_missing_category` | boolean | 0 | 0.0 |
| `dq_zero_weight` | boolean | 0 | 0.0 |
| `dq_translation_added_manually` | boolean | 0 | 0.0 |

**Bản ghi mẫu**

| `product_id` | `product_category_name` | `product_category_name_english` | `product_name_length` | `product_description_length` | `product_photos_qty` | `product_weight_g` | `product_length_cm` | `product_height_cm` | `product_width_cm` | `dq_missing_category` | `dq_zero_weight` | `dq_translation_added_manually` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 00088930e925c41fd95… | automotivo | auto | 56 | 752 | 4 | 1225.0 | 55.0 | 10.0 | 26.0 | False | False | False |
| 0009406fd7479715e4b… | cama_mesa_banho | bed_bath_table | 50 | 266 | 2 | 300.0 | 45.0 | 15.0 | 35.0 | False | False | False |

### `orders` (99,441 dòng, khoá: `order_id`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `order_id` | string | 0 | 0.0 |
| `customer_id` | string | 0 | 0.0 |
| `order_status` | string | 0 | 0.0 |
| `order_purchase_timestamp` | timestamp | 0 | 0.0 |
| `order_approved_at` | timestamp | 160 | 0.16 |
| `order_delivered_carrier_date` | timestamp | 1,783 | 1.79 |
| `order_delivered_customer_date` | timestamp | 2,965 | 2.98 |
| `order_estimated_delivery_date` | timestamp | 0 | 0.0 |
| `delivery_days` | int | 2,965 | 2.98 |
| `delivery_delay_days` | int | 2,965 | 2.98 |
| `is_late` | boolean | 2,965 | 2.98 |
| `dq_delivered_without_date` | boolean | 0 | 0.0 |
| `dq_carrier_before_purchase` | boolean | 0 | 0.0 |
| `dq_no_items` | boolean | 0 | 0.0 |
| `dq_no_payment` | boolean | 0 | 0.0 |
| `purchase_year` | int | 0 | 0.0 |
| `purchase_month` | int | 0 | 0.0 |

**Bản ghi mẫu**

| `order_id` | `customer_id` | `order_status` | `order_purchase_timestamp` | `order_approved_at` | `order_delivered_carrier_date` | `order_delivered_customer_date` | `order_estimated_delivery_date` | `delivery_days` | `delivery_delay_days` | `is_late` | `dq_delivered_without_date` | `dq_carrier_before_purchase` | `dq_no_items` | `dq_no_payment` | `purchase_year` | `purchase_month` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0030d783f979fbc5981… | 84ddc138522822dfb51… | delivered | 2017-11-24 20:03:02 | 2017-11-24 23:13:38 | 2017-12-15 19:34:13 | 2018-01-06 00:25:43 | 2017-12-15 00:00:00 | 43 | 22 | True | False | False | False | False | 2017 | 11 |
| 0035c0b07126fe9c24a… | c8c4991d84dbfef1f61… | delivered | 2017-11-24 01:08:37 | 2017-11-24 01:29:45 | 2017-11-28 17:17:17 | 2017-12-07 16:59:21 | 2018-01-03 00:00:00 | 13 | -27 | False | False | False | False | False | 2017 | 11 |

### `order_items` (112,650 dòng, khoá: `order_id, order_item_id`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `order_id` | string | 0 | 0.0 |
| `order_item_id` | int | 0 | 0.0 |
| `product_id` | string | 0 | 0.0 |
| `seller_id` | string | 0 | 0.0 |
| `shipping_limit_date` | timestamp | 0 | 0.0 |
| `price` | decimal(12,2) | 0 | 0.0 |
| `freight_value` | decimal(12,2) | 0 | 0.0 |
| `item_total` | decimal(12,2) | 0 | 0.0 |

**Bản ghi mẫu**

| `order_id` | `order_item_id` | `product_id` | `seller_id` | `shipping_limit_date` | `price` | `freight_value` | `item_total` |
|---|---|---|---|---|---|---|---|
| 00042b26cf59d7ce69d… | 1 | ac6c3623068f30de030… | df560393f3a51e74553… | 2017-02-13 13:57:51 | 199.90 | 18.14 | 218.04 |
| 00054e8431b9d767580… | 1 | 8d4f2bb7e93e6710a28… | 7040e82f899a04d1b43… | 2017-12-14 12:10:31 | 19.90 | 11.85 | 31.75 |

### `payments` (103,886 dòng, khoá: `order_id, payment_sequential`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `order_id` | string | 0 | 0.0 |
| `payment_sequential` | int | 0 | 0.0 |
| `payment_type` | string | 0 | 0.0 |
| `payment_installments` | int | 0 | 0.0 |
| `payment_value` | decimal(12,2) | 0 | 0.0 |
| `dq_undefined_type` | boolean | 0 | 0.0 |
| `dq_zero_installments` | boolean | 0 | 0.0 |
| `dq_zero_value` | boolean | 0 | 0.0 |

**Bản ghi mẫu**

| `order_id` | `payment_sequential` | `payment_type` | `payment_installments` | `payment_value` | `dq_undefined_type` | `dq_zero_installments` | `dq_zero_value` |
|---|---|---|---|---|---|---|---|
| 00042b26cf59d7ce69d… | 1 | credit_card | 3 | 218.04 | False | False | False |
| 00054e8431b9d767580… | 1 | credit_card | 1 | 31.75 | False | False | False |

### `reviews` (99,224 dòng, khoá: `review_id, order_id`)

| Cột | Kiểu | NULL | % NULL |
|---|---|---:|---:|
| `review_id` | string | 0 | 0.0 |
| `order_id` | string | 0 | 0.0 |
| `review_score` | int | 0 | 0.0 |
| `review_comment_title` | string | 87,658 | 88.34 |
| `review_comment_message` | string | 58,274 | 58.73 |
| `review_creation_date` | timestamp | 0 | 0.0 |
| `review_answer_timestamp` | timestamp | 0 | 0.0 |
| `has_comment` | boolean | 0 | 0.0 |
| `dq_review_id_reused` | boolean | 0 | 0.0 |

**Bản ghi mẫu**

| `review_id` | `order_id` | `review_score` | `review_comment_title` | `review_comment_message` | `review_creation_date` | `review_answer_timestamp` | `has_comment` | `dq_review_id_reused` |
|---|---|---|---|---|---|---|---|---|
| 00032b0141443497c89… | 04fb47576993a3cb0c1… | 5 | *NULL* | *NULL* | 2017-05-30 00:00:00 | 2017-06-01 23:28:55 | False | False |
| 000359bceee1b6fe876… | 97868e4c884bc85f450… | 5 | *NULL* | *NULL* | 2018-02-06 00:00:00 | 2018-02-06 20:54:37 | False | False |

