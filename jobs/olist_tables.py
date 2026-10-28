"""
Mô tả 9 bảng của bộ dữ liệu Olist - dùng chung cho các job (profiling, clean, ...).

Ý tưởng: thay vì viết cứng logic kiểm tra trong code, ta khai báo "hợp đồng dữ liệu"
(data contract) cho từng bảng ở đây:
  - file      : tên file CSV trong thư mục raw
  - multiline : (tuỳ chọn) True nếu file có giá trị chứa dấu xuống dòng
  - keys      : khoá chính (primary key) - dùng để tìm bản ghi trùng
  - types     : kiểu dữ liệu MONG ĐỢI của các cột không phải chuỗi.
                Lúc đọc, mọi cột đều là string; profiling sẽ thử ép kiểu
                để đếm bao nhiêu giá trị sai định dạng.
  - rules     : các luật nghiệp vụ, viết bằng điều kiện Spark SQL.
                Mỗi luật mô tả một trường hợp "BẤT THƯỜNG"; profiling đếm số dòng vi phạm.
  - categorical: các cột dạng phân loại (enum) - in ra phân bố giá trị.

Ghi chú: trong các điều kiện SQL ta dùng try_cast(...) thay vì cast(...):
try_cast trả về NULL khi không ép kiểu được thay vì làm job bị lỗi.
"""

# Đường dẫn bên trong container (xem phần volumes trong docker-compose.yml)
RAW_DIR = "/data/raw"
REPORT_DIR = "/opt/reports"
# Data lake theo kiến trúc "medallion": raw (bronze) -> silver (đã làm sạch) -> gold (tổng hợp)
LAKE_DIR = "/data/lake"
SILVER_DIR = LAKE_DIR + "/silver"
GOLD_DIR = LAKE_DIR + "/gold"

# Khung toạ độ (bounding box) của Brazil - dùng để phát hiện toạ độ sai
BRAZIL_LAT = (-33.75, 5.27)
BRAZIL_LNG = (-73.99, -34.79)

TS = "timestamp"
INT = "int"
DBL = "double"


def ts(col):
    """Viết tắt: ép một cột string sang timestamp an toàn (sai định dạng -> NULL)."""
    return "try_cast({} AS timestamp)".format(col)


TABLES = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "keys": ["order_id"],
        "types": {
            "order_purchase_timestamp": TS,
            "order_approved_at": TS,
            "order_delivered_carrier_date": TS,
            "order_delivered_customer_date": TS,
            "order_estimated_delivery_date": TS,
        },
        "rules": {
            "Đơn 'delivered' nhưng thiếu ngày giao cho khách":
                "order_status = 'delivered' AND order_delivered_customer_date IS NULL",
            "Ngày duyệt đơn sớm hơn ngày mua":
                "{} < {}".format(ts("order_approved_at"), ts("order_purchase_timestamp")),
            "Ngày giao cho đơn vị vận chuyển sớm hơn ngày mua":
                "{} < {}".format(ts("order_delivered_carrier_date"), ts("order_purchase_timestamp")),
            "Ngày khách nhận hàng sớm hơn ngày mua":
                "{} < {}".format(ts("order_delivered_customer_date"), ts("order_purchase_timestamp")),
            "Đơn giao trễ so với ngày dự kiến":
                "{} > {}".format(ts("order_delivered_customer_date"), ts("order_estimated_delivery_date")),
        },
        "categorical": ["order_status"],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        # Một đơn có nhiều dòng hàng -> khoá là cặp (order_id, order_item_id)
        "keys": ["order_id", "order_item_id"],
        "types": {
            "order_item_id": INT,
            "shipping_limit_date": TS,
            "price": DBL,
            "freight_value": DBL,
        },
        "rules": {
            "Giá sản phẩm <= 0": "try_cast(price AS double) <= 0",
            "Phí vận chuyển < 0": "try_cast(freight_value AS double) < 0",
            "Phí vận chuyển = 0 (miễn phí ship)": "try_cast(freight_value AS double) = 0",
        },
        "categorical": [],
    },
    "payments": {
        "file": "olist_order_payments_dataset.csv",
        # Một đơn có thể trả bằng nhiều phương thức -> khoá (order_id, payment_sequential)
        "keys": ["order_id", "payment_sequential"],
        "types": {
            "payment_sequential": INT,
            "payment_installments": INT,
            "payment_value": DBL,
        },
        "rules": {
            "Phương thức thanh toán 'not_defined'": "payment_type = 'not_defined'",
            "Số kỳ trả góp = 0": "try_cast(payment_installments AS int) = 0",
            "Số tiền thanh toán <= 0": "try_cast(payment_value AS double) <= 0",
        },
        "categorical": ["payment_type"],
    },
    "reviews": {
        "file": "olist_order_reviews_dataset.csv",
        # Bình luận có xuống dòng bên trong dấu "..." -> phải đọc ở chế độ multiLine
        "multiline": True,
        "keys": ["review_id"],
        "types": {
            "review_score": INT,
            "review_creation_date": TS,
            "review_answer_timestamp": TS,
        },
        "rules": {
            "Điểm review ngoài khoảng 1-5":
                "NOT (try_cast(review_score AS int) BETWEEN 1 AND 5)",
            "Thời điểm trả lời sớm hơn ngày tạo review":
                "{} < {}".format(ts("review_answer_timestamp"), ts("review_creation_date")),
            "Review không có nội dung bình luận":
                "review_comment_message IS NULL OR trim(review_comment_message) = ''",
        },
        "categorical": ["review_score"],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "keys": ["customer_id"],
        "types": {},
        "rules": {
            "Mã zip không đủ 5 chữ số": "NOT customer_zip_code_prefix RLIKE '^[0-9]{5}$'",
        },
        "categorical": ["customer_state"],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "keys": ["seller_id"],
        "types": {},
        "rules": {
            "Mã zip không đủ 5 chữ số": "NOT seller_zip_code_prefix RLIKE '^[0-9]{5}$'",
        },
        "categorical": ["seller_state"],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "keys": ["product_id"],
        # Lưu ý: tên cột gốc bị sai chính tả "lenght" (đúng là "length") - giữ nguyên ở bước raw
        "types": {
            "product_name_lenght": INT,
            "product_description_lenght": INT,
            "product_photos_qty": INT,
            "product_weight_g": DBL,
            "product_length_cm": DBL,
            "product_height_cm": DBL,
            "product_width_cm": DBL,
        },
        "rules": {
            "Sản phẩm không có danh mục (category)":
                "product_category_name IS NULL",
            "Khối lượng = 0 gram":
                "try_cast(product_weight_g AS double) = 0",
        },
        "categorical": [],
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        # Bảng này KHÔNG có khoá duy nhất: một zip có rất nhiều dòng toạ độ
        "keys": [],
        "types": {
            "geolocation_lat": DBL,
            "geolocation_lng": DBL,
        },
        "rules": {
            "Toạ độ nằm ngoài lãnh thổ Brazil":
                "NOT (try_cast(geolocation_lat AS double) BETWEEN {} AND {} "
                "AND try_cast(geolocation_lng AS double) BETWEEN {} AND {})".format(
                    BRAZIL_LAT[0], BRAZIL_LAT[1], BRAZIL_LNG[0], BRAZIL_LNG[1]),
            "Tên thành phố có ký tự có dấu / đặc biệt (cần chuẩn hoá)":
                "geolocation_city RLIKE '[^a-z0-9 \\'-]'",
        },
        "categorical": ["geolocation_state"],
    },
    "category_translation": {
        "file": "product_category_name_translation.csv",
        "keys": ["product_category_name"],
        "types": {},
        "rules": {},
        "categorical": [],
    },
}

# Kiểm tra toàn vẹn tham chiếu (giống foreign key trong SQL):
# (bảng con, cột con, bảng cha, cột cha, mô tả)
# Đếm số dòng ở bảng con mà giá trị khoá ngoại KHÔNG tồn tại ở bảng cha.
FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id",
     "Đơn hàng trỏ tới khách hàng không tồn tại"),
    ("order_items", "order_id", "orders", "order_id",
     "Dòng hàng trỏ tới đơn hàng không tồn tại"),
    ("order_items", "product_id", "products", "product_id",
     "Dòng hàng trỏ tới sản phẩm không tồn tại"),
    ("order_items", "seller_id", "sellers", "seller_id",
     "Dòng hàng trỏ tới người bán không tồn tại"),
    ("payments", "order_id", "orders", "order_id",
     "Thanh toán trỏ tới đơn hàng không tồn tại"),
    ("reviews", "order_id", "orders", "order_id",
     "Review trỏ tới đơn hàng không tồn tại"),
    ("orders", "order_id", "order_items", "order_id",
     "Đơn hàng không có dòng sản phẩm nào"),
    ("orders", "order_id", "payments", "order_id",
     "Đơn hàng không có thông tin thanh toán"),
    ("products", "product_category_name", "category_translation", "product_category_name",
     "Sản phẩm có danh mục chưa có bản dịch tiếng Anh"),
    ("customers", "customer_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix",
     "Khách hàng có mã zip không có trong bảng toạ độ"),
    ("sellers", "seller_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix",
     "Người bán có mã zip không có trong bảng toạ độ"),
]
