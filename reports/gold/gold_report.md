# Báo cáo tầng gold - silver → fact & bảng tổng hợp

> Sinh tự động bởi `jobs/03_gold.py`. Giải thích chi tiết: `docs/04-ket-hop-tong-hop.md`.

## 1. Thông tin lần chạy

| Thuộc tính | Giá trị |
|---|---|
| Thời điểm chạy | 2026-10-28 03:13:23 |
| Spark master | spark://spark-master:7077 (2 executor, 4 core) |
| Dữ liệu vào (silver) | 569,698 dòng, 40.37 MB |
| Dữ liệu ra (gold) | 7 bảng, 33.3 MB |
| Thời gian tạo bảng | 36.21 s |
| Thời gian đối chiếu số liệu | 6.92 s |
| Tổng thời gian | 60.11 s |

## 2. Các bảng gold

| Bảng | Loại | 1 dòng = | Số dòng | Số cột | Parquet (MB) | Số file | Broadcast join | Sort-merge join | Thời gian (s) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| `fact_order_items` | fact | 1 sản phẩm trong 1 đơn hàng | 112,650 | 27 | 16.27 | 24 | 6 | 0 | 12.92 |
| `fact_orders` | fact | 1 đơn hàng | 99,441 | 29 | 12.66 | 25 | 4 | 0 | 9.68 |
| `agg_sales_monthly` | agg | 1 tháng | 24 | 12 | 0.0 | 1 | 0 | 0 | 2.79 |
| `agg_category_monthly` | agg | 1 danh mục x 1 tháng | 1,282 | 9 | 0.03 | 1 | 0 | 0 | 2.0 |
| `agg_customer_state` | agg | 1 bang của khách hàng | 27 | 11 | 0.0 | 1 | 0 | 0 | 1.99 |
| `agg_seller_performance` | agg | 1 người bán | 3,053 | 15 | 0.19 | 1 | 0 | 0 | 3.57 |
| `agg_customer_rfm` | agg | 1 khách hàng thật | 94,983 | 9 | 4.15 | 1 | 0 | 0 | 2.5 |

## 3. Đối chiếu số liệu (reconciliation)

**11/11 kiểm tra đạt.**

| Kiểm tra | Vế trái | Vế phải | Kết quả |
|---|---:|---:|---|
| fact_order_items giữ nguyên số dòng của silver order_items (join không mất/nhân dòng) | 112,650 | 112,650 | ĐẠT |
| Tổng giá sản phẩm không đổi sau khi join | 13591643.70 | 13591643.70 | ĐẠT |
| fact_orders giữ nguyên số đơn của silver orders | 99,441 | 99,441 | ĐẠT |
| fact_orders: tổng giá trị hàng khớp silver order_items | 13591643.70 | 13591643.70 | ĐẠT |
| Hai bảng fact thống nhất về doanh thu đơn hợp lệ | 13494400.74 | 13494400.74 | ĐẠT |
| Doanh thu các tháng cộng lại = doanh thu đơn hợp lệ | 13494400.74 | 13494400.74 | ĐẠT |
| Doanh thu các danh mục cộng lại = doanh thu theo tháng | 13494400.74 | 13494400.74 | ĐẠT |
| Doanh thu các bang cộng lại = doanh thu theo tháng | 13494400.74 | 13494400.74 | ĐẠT |
| Doanh thu các người bán cộng lại = doanh thu theo tháng | 13494400.74 | 13494400.74 | ĐẠT |
| Số đơn trong RFM = số đơn theo tháng | 98,199 | 98,199 | ĐẠT |
| Số khách trong RFM = số khách thật có đơn hợp lệ | 94,983 | 94,983 | ĐẠT |

## 4. Xem trước kết quả

### Doanh thu theo tháng

| year | month | orders | customers | revenue | gmv | avg_order_value | avg_review_score | avg_delivery_days | late_delivery_pct |
|---|---|---|---|---|---|---|---|---|---|
| 2016 | 9 | 2 | 2 | 207.86 | 279.69 | 139.85 | 1.0 | 55.0 | 100.0 |
| 2016 | 10 | 290 | 287 | 44507.30 | 51354.52 | 177.08 | 3.82 | 19.6 | 1.13 |
| 2016 | 12 | 1 | 1 | 10.90 | 19.62 | 19.62 | 5.0 | 5.0 | 0.0 |
| 2017 | 1 | 787 | 753 | 120098.27 | 136943.46 | 174.01 | 4.09 | 12.8 | 3.07 |
| 2017 | 2 | 1,718 | 1,693 | 244959.35 | 283561.69 | 165.05 | 4.1 | 13.3 | 3.21 |
| 2017 | 3 | 2,617 | 2,577 | 368341.32 | 425617.96 | 162.64 | 4.13 | 13.0 | 5.58 |
| 2017 | 4 | 2,377 | 2,345 | 353842.98 | 405848.61 | 170.74 | 4.07 | 15.0 | 7.86 |
| 2017 | 5 | 3,640 | 3,569 | 503159.19 | 582710.83 | 160.09 | 4.18 | 11.4 | 3.61 |
| 2017 | 6 | 3,205 | 3,142 | 429916.61 | 499652.24 | 155.90 | 4.18 | 12.0 | 3.86 |
| 2017 | 7 | 3,946 | 3,872 | 492287.30 | 578753.73 | 146.67 | 4.23 | 11.5 | 3.43 |
| 2017 | 8 | 4,272 | 4,191 | 568245.79 | 661903.52 | 154.94 | 4.27 | 11.0 | 3.32 |
| 2017 | 9 | 4,227 | 4,154 | 621415.91 | 717102.72 | 169.65 | 4.22 | 11.7 | 5.2 |
| 2017 | 10 | 4,547 | 4,481 | 660179.62 | 764756.03 | 168.19 | 4.17 | 11.7 | 5.29 |
| 2017 | 11 | 7,421 | 7,313 | 1003862.14 | 1172191.68 | 157.96 | 3.95 | 15.1 | 14.31 |
| 2017 | 12 | 5,618 | 5,551 | 742183.79 | 861526.77 | 153.35 | 4.04 | 15.3 | 8.38 |
| 2018 | 1 | 7,187 | 7,088 | 945456.29 | 1101920.01 | 153.32 | 4.07 | 14.0 | 6.56 |
| 2018 | 2 | 6,624 | 6,468 | 837895.43 | 979486.16 | 147.87 | 3.86 | 16.9 | 15.99 |
| 2018 | 3 | 7,168 | 7,077 | 981051.06 | 1152656.99 | 160.81 | 3.76 | 16.2 | 21.36 |
| 2018 | 4 | 6,919 | 6,863 | 993592.98 | 1156248.89 | 167.11 | 4.17 | 11.4 | 5.31 |
| 2018 | 5 | 6,833 | 6,775 | 992871.75 | 1145686.46 | 167.67 | 4.21 | 11.3 | 8.24 |
| 2018 | 6 | 6,145 | 6,106 | 863265.53 | 1020381.90 | 166.05 | 4.29 | 9.2 | 1.36 |
| 2018 | 7 | 6,233 | 6,172 | 878044.27 | 1039783.58 | 166.82 | 4.29 | 8.9 | 4.48 |
| 2018 | 8 | 6,421 | 6,380 | 848860.10 | 996973.51 | 155.27 | 4.29 | 7.7 | 10.39 |
| 2018 | 9 | 1 | 1 | 145.00 | 166.46 | 166.46 | 1.0 | *NULL* | *NULL* |

### Top 10 danh mục theo doanh thu (cả giai đoạn)

| category | orders | items | revenue | avg_price |
|---|---|---|---|---|
| health_beauty | 8,800 | 9,634 | 1255695.13 | 130.34 |
| watches_gifts | 5,604 | 5,970 | 1198185.21 | 200.70 |
| bed_bath_table | 9,399 | 11,097 | 1035964.06 | 93.36 |
| sports_leisure | 7,673 | 8,590 | 979740.92 | 114.06 |
| computers_accessories | 6,654 | 7,781 | 904322.02 | 116.22 |
| furniture_decor | 6,425 | 8,298 | 727465.05 | 87.67 |
| housewares | 5,847 | 6,915 | 626825.80 | 90.65 |
| cool_stuff | 3,616 | 3,779 | 620770.49 | 164.27 |
| auto | 3,872 | 4,204 | 586585.73 | 139.53 |
| garden_tools | 3,505 | 4,328 | 481009.94 | 111.14 |

### Top 10 bang theo doanh thu

| customer_state | orders | customers | revenue | avg_order_value | freight_pct_of_revenue | avg_delivery_days | late_delivery_pct | avg_review_score |
|---|---|---|---|---|---|---|---|---|
| SP | 41,125 | 39,747 | 5163867.22 | 142.93 | 13.83 | 8.7 | 5.89 | 4.21 |
| RJ | 12,697 | 12,241 | 1811623.42 | 166.63 | 16.78 | 15.2 | 13.47 | 3.91 |
| MG | 11,496 | 11,134 | 1573508.20 | 160.32 | 17.13 | 11.9 | 5.61 | 4.16 |
| RS | 5,415 | 5,233 | 742559.78 | 162.01 | 18.14 | 15.2 | 7.15 | 4.15 |
| PR | 4,982 | 4,824 | 676883.06 | 159.41 | 17.33 | 11.9 | 5.0 | 4.21 |
| SC | 3,599 | 3,501 | 518578.28 | 168.94 | 17.25 | 14.9 | 9.76 | 4.1 |
| BA | 3,344 | 3,244 | 507108.83 | 181.49 | 19.68 | 19.3 | 14.04 | 3.88 |
| DF | 2,120 | 2,057 | 300886.45 | 165.72 | 16.76 | 12.9 | 7.07 | 4.09 |
| GO | 1,998 | 1,934 | 287870.46 | 170.44 | 18.30 | 15.5 | 8.18 | 4.07 |
| ES | 2,018 | 1,950 | 273532.13 | 160.10 | 18.11 | 15.7 | 12.23 | 4.05 |

### Top 10 người bán theo doanh thu

| seller_id | seller_state | orders | revenue | avg_review_score | avg_delivery_days | late_delivery_pct | avg_distance_km | active_months |
|---|---|---|---|---|---|---|---|---|
| 4869f7a5dfa277a7dca6462… | SP | 1,131 | 229237.63 | 4.13 | 14.9 | 11.59 | 610.0 | 17 |
| 53243585a1d6dc2643021fd… | BA | 358 | 222776.05 | 4.08 | 13.3 | 4.0 | 1195.3 | 12 |
| 4a3ca9315b744ce9f8e9374… | SP | 1,804 | 200326.12 | 3.8 | 14.4 | 10.98 | 563.6 | 20 |
| fa1c13f2614d7b5c4749cbc… | SP | 584 | 192842.13 | 4.34 | 13.3 | 10.19 | 684.4 | 20 |
| 7c67e1448b00f6e969d365c… | SP | 982 | 187923.89 | 3.35 | 22.3 | 9.59 | 545.9 | 20 |
| 7e93a43ef30c4f03f38b393… | SP | 332 | 172583.88 | 4.24 | 11.3 | 5.59 | 664.1 | 20 |
| da8622b14eb17ae2831f4ac… | SP | 1,314 | 160236.57 | 4.07 | 11.1 | 7.3 | 401.7 | 19 |
| 7a67c85e85bb2ce8582c35f… | SP | 1,159 | 141715.54 | 4.24 | 11.1 | 5.89 | 622.1 | 20 |
| 1025f0e2d44d7041d6cf58b… | SP | 915 | 138968.55 | 3.85 | 12.0 | 9.23 | 573.7 | 14 |
| 955fee9216a65b617aa5c05… | SP | 1,286 | 135159.70 | 4.05 | 10.7 | 8.08 | 493.5 | 14 |

### Phân bố số đơn mỗi khách (Frequency trong RFM)

| orders_per_customer | customers | pct |
|---|---|---|
| 1 | 92,096 | 96.96 |
| 2 | 2,651 | 2.79 |
| 3 | 188 | 0.2 |
| 4 | 29 | 0.03 |
| 5 | 9 | 0.01 |
| 6 | 5 | 0.01 |
| 7 | 3 | 0.0 |
| 9 | 1 | 0.0 |
| 16 | 1 | 0.0 |

## 5. Schema các bảng gold

<details><summary><code>fact_order_items</code> (27 cột)</summary>

| Cột | Kiểu |
|---|---|
| `order_id` | string |
| `order_item_id` | int |
| `product_id` | string |
| `seller_id` | string |
| `customer_id` | string |
| `customer_unique_id` | string |
| `order_status` | string |
| `is_valid_sale` | boolean |
| `order_purchase_timestamp` | timestamp |
| `purchase_date` | date |
| `category` | string |
| `price` | decimal(12,2) |
| `freight_value` | decimal(12,2) |
| `item_total` | decimal(12,2) |
| `customer_state` | string |
| `customer_city` | string |
| `seller_state` | string |
| `seller_city` | string |
| `distance_km` | double |
| `delivery_days` | int |
| `delivery_delay_days` | int |
| `is_late` | boolean |
| `review_score` | int |
| `main_payment_type` | string |
| `max_installments` | int |
| `purchase_year` | int |
| `purchase_month` | int |

</details>

<details><summary><code>fact_orders</code> (29 cột)</summary>

| Cột | Kiểu |
|---|---|
| `order_id` | string |
| `customer_id` | string |
| `customer_unique_id` | string |
| `order_status` | string |
| `is_valid_sale` | boolean |
| `order_purchase_timestamp` | timestamp |
| `purchase_date` | date |
| `customer_state` | string |
| `customer_city` | string |
| `items` | bigint |
| `sellers` | bigint |
| `items_value` | decimal(22,2) |
| `freight_value` | decimal(22,2) |
| `order_value` | decimal(23,2) |
| `payment_total` | decimal(22,2) |
| `payment_count` | bigint |
| `main_payment_type` | string |
| `max_installments` | int |
| `delivery_days` | int |
| `delivery_delay_days` | int |
| `is_late` | boolean |
| `review_score` | int |
| `review_has_comment` | boolean |
| `dq_delivered_without_date` | boolean |
| `dq_carrier_before_purchase` | boolean |
| `dq_no_items` | boolean |
| `dq_no_payment` | boolean |
| `purchase_year` | int |
| `purchase_month` | int |

</details>

<details><summary><code>agg_sales_monthly</code> (12 cột)</summary>

| Cột | Kiểu |
|---|---|
| `purchase_year` | int |
| `purchase_month` | int |
| `orders` | bigint |
| `customers` | bigint |
| `items` | bigint |
| `revenue` | decimal(32,2) |
| `freight` | decimal(32,2) |
| `gmv` | decimal(33,2) |
| `avg_order_value` | decimal(24,2) |
| `avg_review_score` | double |
| `avg_delivery_days` | double |
| `late_delivery_pct` | double |

</details>

<details><summary><code>agg_category_monthly</code> (9 cột)</summary>

| Cột | Kiểu |
|---|---|
| `purchase_year` | int |
| `purchase_month` | int |
| `category` | string |
| `orders` | bigint |
| `items` | bigint |
| `revenue` | decimal(22,2) |
| `freight` | decimal(22,2) |
| `avg_price` | decimal(13,2) |
| `avg_review_score` | double |

</details>

<details><summary><code>agg_customer_state</code> (11 cột)</summary>

| Cột | Kiểu |
|---|---|
| `customer_state` | string |
| `orders` | bigint |
| `customers` | bigint |
| `revenue` | decimal(32,2) |
| `gmv` | decimal(33,2) |
| `avg_order_value` | decimal(24,2) |
| `avg_freight` | decimal(23,2) |
| `freight_pct_of_revenue` | decimal(35,2) |
| `avg_delivery_days` | double |
| `late_delivery_pct` | double |
| `avg_review_score` | double |

</details>

<details><summary><code>agg_seller_performance</code> (15 cột)</summary>

| Cột | Kiểu |
|---|---|
| `seller_id` | string |
| `seller_state` | string |
| `seller_city` | string |
| `orders` | bigint |
| `items` | bigint |
| `customers` | bigint |
| `revenue` | decimal(22,2) |
| `avg_price` | decimal(13,2) |
| `avg_review_score` | double |
| `avg_delivery_days` | double |
| `late_delivery_pct` | double |
| `avg_distance_km` | double |
| `first_sale_date` | date |
| `last_sale_date` | date |
| `active_months` | bigint |

</details>

<details><summary><code>agg_customer_rfm</code> (9 cột)</summary>

| Cột | Kiểu |
|---|---|
| `customer_unique_id` | string |
| `customer_state` | string |
| `first_purchase_date` | date |
| `last_purchase_date` | date |
| `recency_days` | int |
| `frequency` | bigint |
| `monetary` | decimal(33,2) |
| `avg_order_value` | decimal(24,2) |
| `avg_review_score` | double |

</details>

