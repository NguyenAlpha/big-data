# Đồ án Nhập môn Dữ liệu lớn - Giai đoạn **Storage → Processing**

Làm sạch, chuẩn hoá, kết hợp và tổng hợp bộ dữ liệu thương mại điện tử **Olist (Brazil)**
bằng **Apache Spark (PySpark)** chạy trên cụm Spark Standalone dựng bằng Docker.

| | |
|---|---|
| **Input** | 9 file CSV thô của Olist (~120 MB, ~1,55 triệu bản ghi), là dữ liệu nhóm Storage bàn giao |
| **Output** | Dữ liệu đã làm sạch (Parquet, *silver*) và các bảng tổng hợp (*gold*) bàn giao cho nhóm Analytics, kèm báo cáo chất lượng và thời gian xử lý |
| **Công nghệ** | Apache Spark 3.5.3, PySpark, Spark SQL, Docker Compose |

## Tiến độ

| Bước | Nội dung | Trạng thái | Tài liệu |
|---|---|---|---|
| 1 | Xác định phạm vi, input/output | Xong | README này |
| 2 | Dựng cụm Spark bằng Docker (1 master + 2 worker) | Xong | [docs/01-moi-truong-spark.md](docs/01-moi-truong-spark.md) |
| 3 | Data profiling: khảo sát chất lượng dữ liệu **trước** xử lý | Xong | [docs/02-data-profiling.md](docs/02-data-profiling.md) |
| 4 | Làm sạch và chuẩn hoá, lưu Parquet (silver) | Chưa làm | |
| 5 | Kết hợp và tổng hợp bằng Spark SQL (gold) | Chưa làm | |
| 6 | Thực nghiệm: chất lượng sau xử lý, thời gian, mở rộng quy mô | Chưa làm | |

## Cấu trúc thư mục

```
.
├── docker-compose.yml        # Cụm Spark: spark-master, spark-worker-1, spark-worker-2
├── dataset/                  # CSV gốc của Olist (KHÔNG có trên git, xem bên dưới)
├── data/                     # Dữ liệu sinh ra bởi job (silver/gold), không có trên git
├── jobs/
│   ├── olist_tables.py       # "Data contract": mô tả bảng, khoá, kiểu, luật kiểm tra
│   └── 01_profiling.py       # Bước 3: job profiling
├── reports/
│   └── profiling/            # Kết quả bước 3 (báo cáo .md, số liệu .json, log)
└── docs/                     # Tài liệu giải thích từng bước
```

## Hướng dẫn chạy

### Yêu cầu
- **Docker Desktop** (đã bật). Không cần cài Java, Spark hay Python trên máy.
- Máy có ít nhất 4 core và 8 GB RAM (cụm dùng 4 core, 4 GB).

### 1. Tải dữ liệu
Tải bộ [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
trên Kaggle, giải nén **9 file CSV** vào thư mục `dataset/`:

```
dataset/
├── olist_customers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_orders_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

### 2. Bật cụm Spark
```bash
docker compose up -d        # lần đầu sẽ tải image apache/spark (~1 GB)
docker compose ps           # 3 container phải ở trạng thái "Up"
```
Mở http://localhost:8080: phải thấy **Alive Workers: 2**, **Cores: 4**.

### 3. Chạy job profiling
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /opt/jobs/01_profiling.py
```
> **Dùng Git Bash trên Windows?** Thêm `MSYS_NO_PATHCONV=1` vào đầu lệnh. Nếu không, Git
> Bash sẽ tự đổi `/opt/...` thành đường dẫn Windows và lệnh lỗi.
> PowerShell và CMD không bị vấn đề này (viết lệnh trên 1 dòng, bỏ dấu `\`).

Job chạy khoảng 1 phút. Trong lúc chạy, mở http://localhost:4040 để xem tiến trình.
Kết quả nằm trong `reports/profiling/`:

| File | Nội dung |
|---|---|
| `profiling_report.md` | Báo cáo dễ đọc: số dòng, % thiếu, trùng lặp, vi phạm luật, khoá ngoại, bản ghi mẫu |
| `profiling_summary.json` | Cùng số liệu ở dạng JSON, để bước sau so sánh trước/sau |
| `profiling_run.log` | Log thời gian xử lý từng bảng |

### 4. Tắt cụm
```bash
docker compose down
```

## Xử lý sự cố

| Triệu chứng | Cách khắc phục |
|---|---|
| `failed to connect to the docker API` | Docker Desktop chưa bật, hãy mở Docker Desktop và chờ biểu tượng chuyển xanh |
| `Bind for 0.0.0.0:8080 failed: port is already allocated` | Cổng đang bị app khác dùng. Đổi cổng bên trái trong `docker-compose.yml`, vd `"18080:8080"` |
| `Path does not exist: file:/data/raw/...` | Chưa đặt CSV vào `dataset/`, hoặc đặt sai tên file |
| Job treo ở `Initial job has not accepted any resources` | Worker chưa đăng ký. Kiểm tra http://localhost:8080 và chạy `docker compose restart` |
| Lỗi `/opt/...` bị đổi thành `C:/Program Files/Git/opt/...` | Dùng Git Bash: thêm `MSYS_NO_PATHCONV=1` trước lệnh |
