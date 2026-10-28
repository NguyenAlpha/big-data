"""
BƯỚC 4 - LÀM SẠCH & CHUẨN HOÁ (raw CSV -> silver Parquet)
=========================================================

Đọc 9 file CSV thô, áp dụng các luật làm sạch rút ra từ báo cáo profiling
(docs/02-data-profiling.md, mục 4.1), rồi ghi thành 8 bảng Parquet "silver"
trong data/silver/. Đây là đầu vào cho bước 5 (join & tổng hợp) và nhóm Analytics.

Cách chạy (từ thư mục gốc project, sau khi `docker compose up -d`):
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/jobs/02_clean.py

Đầu ra:
    data/silver/<bảng>/            - dữ liệu sạch dạng Parquet
    reports/cleaning/cleaning_report.md     - báo cáo trước/sau, số dòng bị tác động
    reports/cleaning/cleaning_summary.json  - số liệu thô
    reports/cleaning/cleaning_run.log       - log thời gian

-----------------------------------------------------------------------------
NGUYÊN TẮC LÀM SẠCH (áp dụng nhất quán cho mọi bảng)

  LOẠI BỎ   chỉ khi dòng chắc chắn là rác: trùng lặp hoàn toàn, trùng khoá chính,
            toạ độ nằm ngoài Brazil.
  SỬA       lỗi kỹ thuật không làm thay đổi ý nghĩa: ép kiểu, đổi tên cột sai
            chính tả, chuẩn hoá chữ hoa/thường/dấu, bổ sung bản dịch.
  GẮN CỜ    bất thường nghiệp vụ (vd: giao hàng trước ngày mua). GIỮ dòng, thêm
            cột boolean `dq_...` (dq = data quality). Nhóm Analytics tự quyết định
            lọc hay không: `WHERE NOT dq_carrier_before_purchase`.
  BỔ SUNG   thêm cột tính sẵn hay dùng (số ngày giao hàng, trễ hạn, toạ độ...).

Lý do không xoá dòng bất thường: xoá là mất dữ liệu vĩnh viễn và làm lệch số
liệu (vd: xoá đơn có ngày lạ thì doanh thu bị thiếu). Gắn cờ giữ được cả hai lựa chọn.

-----------------------------------------------------------------------------
VÌ SAO LƯU PARQUET THAY VÌ CSV?

  - Lưu theo CỘT: truy vấn chỉ cần 2/10 cột thì chỉ đọc 2 cột đó.
  - Có schema: kiểu dữ liệu (timestamp, decimal...) được lưu kèm, đọc lại
    không cần ép kiểu.
  - Nén tốt (snappy) và chia mảnh được -> đọc song song (CSV multiLine thì không).
  - Có thống kê min/max mỗi khối -> Spark bỏ qua được khối không liên quan.
  Tương tự: CSV ~ file JSON log, Parquet ~ bảng database có index.
"""

import json
import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from common import (Timer, dir_size_mb, fmt, md_cell, normalize_text, parquet_files,
                    read_raw_csv, setup_logger, wait_for_executors, write_parquet)
from olist_tables import BRAZIL_LAT, BRAZIL_LNG, REPORT_DIR, SILVER_DIR, TABLES

OUT_DIR = os.path.join(REPORT_DIR, "cleaning")
log = setup_logger("cleaning", os.path.join(OUT_DIR, "cleaning_run.log"))

MONEY = DecimalType(12, 2)  # Tiền dùng DECIMAL thay vì double để không bị lỗi làm tròn 0.1+0.2

# 2 danh mục không có trong file dịch gốc (phát hiện ở bước profiling) - dịch thủ công
MANUAL_TRANSLATIONS = [
    ("pc_gamer", "pc_gamer"),
    ("portateis_cozinha_e_preparadores_de_alimentos", "portable_kitchen_food_processors"),
]

# Khoá chính của từng bảng silver (dùng để loại trùng và để kiểm tra sau khi ghi)
SILVER_KEYS = {
    "geolocation": ["zip_code_prefix"],
    "customers": ["customer_id"],
    "sellers": ["seller_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    # profiling cho thấy review_id bị lặp ở nhiều đơn -> khoá đúng là cặp này
    "reviews": ["review_id", "order_id"],
}

# Bảng orders được chia thư mục theo năm/tháng mua hàng (xem hàm write_silver)
PARTITION_BY = {"orders": ["purchase_year", "purchase_month"]}


# =============================================================================
# Tiện ích
# =============================================================================
def to_ts(col):
    """Chuỗi -> timestamp. try_cast: sai định dạng thì ra NULL thay vì làm job lỗi."""
    return F.expr("try_cast(`{}` AS timestamp)".format(col))


def to_num(col, dtype):
    return F.expr("try_cast(`{}` AS {})".format(col, dtype))


def flag(condition):
    """
    Cột cờ dq_* luôn là true/false, không bao giờ NULL.
    (So sánh với NULL trong SQL cho ra NULL, vd NULL = 0 -> NULL; khi đó
    `WHERE NOT dq_x` sẽ loại mất dòng đó một cách âm thầm.)
    """
    return F.coalesce(condition, F.lit(False))


def count_rows(df, conditions):
    """
    Đếm số dòng thoả từng điều kiện, tất cả trong 1 lần quét dữ liệu.
    conditions: {mô tả: biểu thức Column boolean}  ->  {mô tả: số dòng}
    """
    if not conditions:
        return {}
    exprs = [F.sum(F.when(cond, 1).otherwise(0)).alias("c{}".format(i))
             for i, cond in enumerate(conditions.values())]
    row = df.agg(*exprs).collect()[0]
    return {desc: int(row["c{}".format(i)] or 0) for i, desc in enumerate(conditions)}


def drop_key_duplicates(df, keys):
    """Loại dòng trùng khoá chính, giữ 1 dòng. Trả về (df mới, số dòng bị loại)."""
    before = df.count()
    deduped = df.dropDuplicates(keys)
    return deduped, before - deduped.count()


def most_frequent(df, key, col):
    """
    Với mỗi giá trị `key`, lấy giá trị `col` xuất hiện nhiều nhất (mode).
    Dùng WINDOW FUNCTION - giống SQL:
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY count DESC, col) = 1
    """
    counts = df.where(F.col(col).isNotNull() & (F.col(col) != "")).groupBy(key, col).count()
    w = Window.partitionBy(key).orderBy(F.desc("count"), F.col(col))
    return (counts.withColumn("_rank", F.row_number().over(w))
            .where("_rank = 1").select(key, col))


class Actions:
    """Ghi lại các thao tác làm sạch đã áp dụng cho 1 bảng, để đưa vào báo cáo."""

    LABELS = {"drop": "Loại bỏ", "fix": "Sửa", "flag": "Gắn cờ", "add": "Bổ sung"}

    def __init__(self):
        self.items = []

    def add(self, kind, description, rows=None):
        self.items.append({"type": self.LABELS[kind], "action": description, "rows": rows})


# =============================================================================
# Làm sạch từng bảng.
# Mỗi hàm nhận DataFrame thô (mọi cột là string), trả về (DataFrame sạch, Actions).
# Lưu ý: các hàm chỉ XÂY kế hoạch biến đổi; dữ liệu thật chỉ được xử lý khi gặp
# action (count, collect) hoặc khi ghi file.
# =============================================================================
def clean_geolocation(raw):
    """
    1 triệu dòng -> ~19 nghìn dòng (1 dòng / mã zip).
    Đây là bảng "bẩn" nhất: 26% trùng, mỗi zip ~52 dòng, tên thành phố nhiều cách viết.
    """
    act = Actions()
    n_raw = raw.count()

    deduped = raw.dropDuplicates()
    n_dedup = deduped.count()
    act.add("drop", "Dòng trùng lặp hoàn toàn", n_raw - n_dedup)

    typed = deduped.select(
        F.col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
        to_num("geolocation_lat", "double").alias("lat"),
        to_num("geolocation_lng", "double").alias("lng"),
        normalize_text(F.col("geolocation_city")).alias("city"),
        F.upper(F.trim("geolocation_state")).alias("state"),
    )
    in_brazil = typed.where(F.col("lat").between(*BRAZIL_LAT) & F.col("lng").between(*BRAZIL_LNG))
    n_valid = in_brazil.count()
    act.add("drop", "Toạ độ nằm ngoài lãnh thổ Brazil", n_dedup - n_valid)

    changed = count_rows(deduped, {"city": F.col("geolocation_city") !=
                                   normalize_text(F.col("geolocation_city"))})["city"]
    act.add("fix", "Chuẩn hoá tên thành phố (chữ thường, bỏ dấu, bỏ ký tự rác)", changed)

    # Gộp mỗi zip thành 1 dòng: toạ độ trung bình + tên thành phố/bang phổ biến nhất
    points = in_brazil.groupBy("zip_code_prefix").agg(
        F.round(F.avg("lat"), 6).alias("lat"),
        F.round(F.avg("lng"), 6).alias("lng"),
        F.count(F.lit(1)).alias("n_points"),
    )
    result = (points
              .join(most_frequent(in_brazil, "zip_code_prefix", "city"), "zip_code_prefix", "left")
              .join(most_frequent(in_brazil, "zip_code_prefix", "state"), "zip_code_prefix", "left")
              .select("zip_code_prefix", "city", "state", "lat", "lng", "n_points"))
    act.add("fix", "Gộp các điểm toạ độ của cùng 1 mã zip thành 1 dòng "
                   "(toạ độ trung bình, tên thành phố/bang phổ biến nhất)", n_valid)
    return result, act


def clean_location_entity(raw, geo, prefix, key):
    """Dùng chung cho customers và sellers (cấu trúc giống nhau: id, zip, city, state)."""
    act = Actions()
    zip_raw = "{}_zip_code_prefix".format(prefix)
    city_raw = "{}_city".format(prefix)

    df = raw.select(
        *[c for c in raw.columns if c not in (zip_raw, city_raw, prefix + "_state")],
        F.lpad(F.trim(zip_raw), 5, "0").alias("zip_code_prefix"),   # luôn đủ 5 chữ số
        normalize_text(F.col(city_raw)).alias("city"),
        F.upper(F.trim(prefix + "_state")).alias("state"),
        F.col(city_raw).alias("_city_raw"),
    )
    df, dups = drop_key_duplicates(df, [key])
    act.add("drop", "Trùng khoá chính `{}`".format(key), dups)

    # BROADCAST JOIN: bảng geo nhỏ (~19 nghìn dòng) được gửi nguyên bản tới MỌI
    # executor, nên join tại chỗ mà không phải shuffle bảng lớn qua mạng.
    # Giống việc cache 1 bảng tra cứu nhỏ trong RAM của từng server.
    geo_small = F.broadcast(geo.select("zip_code_prefix", "lat", "lng"))
    df = df.join(geo_small, "zip_code_prefix", "left")
    df = df.withColumn("dq_zip_not_in_geolocation", F.col("lat").isNull())

    stats = count_rows(df, {
        "city": F.col("_city_raw") != F.col("city"),
        "geo": F.col("dq_zip_not_in_geolocation"),
    })
    act.add("fix", "Chuẩn hoá tên thành phố", stats["city"])
    act.add("add", "Thêm toạ độ `lat`, `lng` từ bảng geolocation (broadcast join)", None)
    act.add("flag", "`dq_zip_not_in_geolocation`: mã zip không có trong bảng toạ độ", stats["geo"])
    return df.drop("_city_raw"), act


def clean_products(raw, translation):
    act = Actions()
    df = raw.select(
        "product_id",
        F.col("product_category_name").alias("_category_raw"),
        # Sửa tên cột sai chính tả "lenght" -> "length"
        to_num("product_name_lenght", "int").alias("product_name_length"),
        to_num("product_description_lenght", "int").alias("product_description_length"),
        to_num("product_photos_qty", "int").alias("product_photos_qty"),
        to_num("product_weight_g", "double").alias("product_weight_g"),
        to_num("product_length_cm", "double").alias("product_length_cm"),
        to_num("product_height_cm", "double").alias("product_height_cm"),
        to_num("product_width_cm", "double").alias("product_width_cm"),
    )
    act.add("fix", "Đổi tên cột sai chính tả `product_name_lenght`, "
                   "`product_description_lenght` -> `..._length`", None)
    df, dups = drop_key_duplicates(df, ["product_id"])
    act.add("drop", "Trùng khoá chính `product_id`", dups)

    df = (df
          .withColumn("dq_missing_category", F.col("_category_raw").isNull())
          .withColumn("product_category_name", F.coalesce(F.trim("_category_raw"), F.lit("unknown")))
          .withColumn("dq_zero_weight", flag(F.col("product_weight_g") == 0))
          # Khối lượng 0 gram là vô lý -> coi như không biết (NULL)
          .withColumn("product_weight_g",
                      F.when(F.col("product_weight_g") == 0, None)
                      .otherwise(F.col("product_weight_g"))))

    # Bảng dịch = file gốc + bản dịch thủ công cho 2 danh mục còn thiếu
    spark = raw.sparkSession
    manual = spark.createDataFrame(MANUAL_TRANSLATIONS,
                                   ["product_category_name", "product_category_name_english"])
    dictionary = translation.unionByName(manual).dropDuplicates(["product_category_name"])
    df = df.join(F.broadcast(dictionary), "product_category_name", "left")
    df = df.withColumn("dq_translation_added_manually",
                       F.col("product_category_name").isin([m[0] for m in MANUAL_TRANSLATIONS]))
    df = df.withColumn("product_category_name_english",
                       F.coalesce("product_category_name_english", "product_category_name"))

    stats = count_rows(df, {
        "missing": F.col("dq_missing_category"),
        "weight": F.col("dq_zero_weight"),
        "manual": F.col("dq_translation_added_manually"),
    })
    act.add("fix", "Danh mục rỗng -> 'unknown'; gắn cờ `dq_missing_category`", stats["missing"])
    act.add("fix", "Khối lượng 0 gram -> NULL; gắn cờ `dq_zero_weight`", stats["weight"])
    act.add("add", "Thêm tên danh mục tiếng Anh (broadcast join bảng dịch + 2 bản dịch thủ công)",
            stats["manual"])
    columns = ["product_id", "product_category_name", "product_category_name_english",
               "product_name_length", "product_description_length", "product_photos_qty",
               "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
               "dq_missing_category", "dq_zero_weight", "dq_translation_added_manually"]
    return df.select(*columns), act


def clean_orders(raw, items_raw, payments_raw):
    act = Actions()
    ts_cols = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"]
    df = raw.select("order_id", "customer_id",
                    F.lower(F.trim("order_status")).alias("order_status"),
                    *[to_ts(c).alias(c) for c in ts_cols])
    act.add("fix", "Ép 5 cột ngày giờ từ chuỗi sang timestamp", None)
    df, dups = drop_key_duplicates(df, ["order_id"])
    act.add("drop", "Trùng khoá chính `order_id`", dups)

    purchase = F.col("order_purchase_timestamp")
    delivered = F.col("order_delivered_customer_date")
    estimated = F.col("order_estimated_delivery_date")
    df = (df
          .withColumn("purchase_year", F.year(purchase))
          .withColumn("purchase_month", F.month(purchase))
          # Số ngày từ lúc mua tới lúc khách nhận (NULL nếu chưa giao)
          .withColumn("delivery_days", F.datediff(delivered, purchase))
          # Âm = giao sớm hơn dự kiến, dương = trễ
          .withColumn("delivery_delay_days", F.datediff(delivered, estimated))
          .withColumn("is_late", delivered > estimated))

    # Đơn có dòng sản phẩm / có thanh toán không? -> left join với danh sách order_id
    has_items = items_raw.select("order_id").distinct().withColumn("_has_items", F.lit(True))
    has_pay = payments_raw.select("order_id").distinct().withColumn("_has_pay", F.lit(True))
    df = df.join(has_items, "order_id", "left").join(has_pay, "order_id", "left")

    flags = {
        "dq_delivered_without_date": "Đơn 'delivered' nhưng thiếu ngày khách nhận hàng",
        "dq_carrier_before_purchase": "Ngày giao cho đơn vị vận chuyển sớm hơn ngày mua",
        "dq_no_items": "Đơn không có dòng sản phẩm nào",
        "dq_no_payment": "Đơn không có thông tin thanh toán",
    }
    df = (df
          .withColumn("dq_delivered_without_date",
                      (F.col("order_status") == "delivered") & delivered.isNull())
          .withColumn("dq_carrier_before_purchase",
                      flag(F.col("order_delivered_carrier_date") < purchase))
          .withColumn("dq_no_items", F.col("_has_items").isNull())
          .withColumn("dq_no_payment", F.col("_has_pay").isNull())
          .drop("_has_items", "_has_pay"))

    stats = count_rows(df, {name: F.col(name) for name in flags})
    act.add("add", "Thêm cột `purchase_year`, `purchase_month`, `delivery_days`, "
                   "`delivery_delay_days`, `is_late`", None)
    for name, desc in flags.items():
        act.add("flag", "`{}`: {}".format(name, desc), stats[name])
    return df, act


def clean_order_items(raw):
    act = Actions()
    df = raw.select(
        "order_id",
        to_num("order_item_id", "int").alias("order_item_id"),
        "product_id", "seller_id",
        to_ts("shipping_limit_date").alias("shipping_limit_date"),
        F.col("price").cast(MONEY).alias("price"),
        F.col("freight_value").cast(MONEY).alias("freight_value"),
    ).withColumn("item_total", (F.col("price") + F.col("freight_value")).cast(MONEY))
    act.add("fix", "Ép kiểu: `order_item_id` -> int, ngày -> timestamp, tiền -> decimal(12,2)", None)
    df, dups = drop_key_duplicates(df, ["order_id", "order_item_id"])
    act.add("drop", "Trùng khoá chính (`order_id`, `order_item_id`)", dups)
    act.add("add", "Thêm `item_total` = giá + phí vận chuyển", None)
    return df, act


def clean_payments(raw):
    act = Actions()
    df = raw.select(
        "order_id",
        to_num("payment_sequential", "int").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        to_num("payment_installments", "int").alias("payment_installments"),
        F.col("payment_value").cast(MONEY).alias("payment_value"),
    )
    act.add("fix", "Ép kiểu số và tiền decimal(12,2)", None)
    df, dups = drop_key_duplicates(df, ["order_id", "payment_sequential"])
    act.add("drop", "Trùng khoá chính (`order_id`, `payment_sequential`)", dups)

    df = (df
          .withColumn("dq_undefined_type", flag(F.col("payment_type") == "not_defined"))
          .withColumn("dq_zero_installments", flag(F.col("payment_installments") == 0))
          .withColumn("dq_zero_value", flag(F.col("payment_value") <= 0)))
    stats = count_rows(df, {c: F.col(c) for c in
                            ["dq_undefined_type", "dq_zero_installments", "dq_zero_value"]})
    act.add("flag", "`dq_undefined_type`: phương thức 'not_defined' (đều là đơn đã huỷ)",
            stats["dq_undefined_type"])
    act.add("flag", "`dq_zero_installments`: số kỳ trả góp = 0", stats["dq_zero_installments"])
    act.add("flag", "`dq_zero_value`: số tiền = 0 (chủ yếu voucher 0 đồng)", stats["dq_zero_value"])
    return df, act


def clean_reviews(raw):
    act = Actions()

    def clean_comment(col):
        # Gộp xuống dòng/tab/khoảng trắng thừa thành 1 dấu cách; chuỗi rỗng -> NULL
        c = F.trim(F.regexp_replace(F.col(col), "\\s+", " "))
        return F.when(c == "", None).otherwise(c)

    df = raw.select(
        "review_id", "order_id",
        to_num("review_score", "int").alias("review_score"),
        clean_comment("review_comment_title").alias("review_comment_title"),
        clean_comment("review_comment_message").alias("review_comment_message"),
        to_ts("review_creation_date").alias("review_creation_date"),
        to_ts("review_answer_timestamp").alias("review_answer_timestamp"),
        F.col("review_comment_message").rlike("[\\r\\n]").alias("_multiline"),
    )
    df, dups = drop_key_duplicates(df, ["review_id", "order_id"])
    act.add("drop", "Trùng khoá chính (`review_id`, `order_id`)", dups)

    w = Window.partitionBy("review_id")
    df = (df
          .withColumn("has_comment", F.col("review_comment_message").isNotNull())
          .withColumn("dq_review_id_reused", F.count(F.lit(1)).over(w) > 1))
    stats = count_rows(df, {"multi": F.coalesce(F.col("_multiline"), F.lit(False)),
                            "reused": F.col("dq_review_id_reused")})
    act.add("fix", "Bình luận nhiều dòng -> 1 dòng (thay xuống dòng bằng dấu cách)", stats["multi"])
    act.add("add", "Thêm `has_comment` (khách có viết bình luận hay chỉ chấm sao)", None)
    act.add("flag", "`dq_review_id_reused`: cùng `review_id` dùng cho nhiều đơn", stats["reused"])
    return df.drop("_multiline"), act


# =============================================================================
# Ghi & kiểm tra lại
# =============================================================================
def write_silver(df, name):
    """Ghi 1 bảng silver ra Parquet (orders chia thư mục theo năm/tháng - xem common.write_parquet)."""
    return write_parquet(df, os.path.join(SILVER_DIR, name), PARTITION_BY.get(name))


def validate_silver(spark, name, path):
    """Đọc lại Parquet vừa ghi để chứng minh dữ liệu dùng được và khoá chính là duy nhất."""
    df = spark.read.parquet(path)
    keys = SILVER_KEYS[name]
    rows = df.count()
    key_dups = rows - df.select(*keys).distinct().count()
    null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    nulls = df.agg(*null_exprs).collect()[0].asDict()
    files = parquet_files(path)
    return {
        "rows": rows,
        "key_columns": keys,
        "key_duplicate_rows": key_dups,
        "schema": [(f.name, f.dataType.simpleString()) for f in df.schema.fields],
        "nulls": {c: int(v or 0) for c, v in nulls.items()},
        "parquet_files": files,
        "size_mb": dir_size_mb(path),
        "samples": [r.asDict() for r in df.limit(2).collect()],
    }


# =============================================================================
# Báo cáo
# =============================================================================
def write_markdown(summary, path):
    run, tables = summary["run"], summary["tables"]
    L = ["# Báo cáo làm sạch dữ liệu - raw CSV → silver Parquet\n",
         "> Sinh tự động bởi `jobs/02_clean.py`. Giải thích chi tiết: "
         "`docs/03-lam-sach-du-lieu.md`.\n",
         "## 1. Thông tin lần chạy\n",
         "| Thuộc tính | Giá trị |\n|---|---|"]
    for k, v in [("Thời điểm chạy", run["started_at"]),
                 ("Spark master", "{} ({} executor, {} core)".format(
                     run["master"], run["executors"], run["default_parallelism"])),
                 ("Dung lượng CSV gốc", "{} MB".format(run["raw_size_mb"])),
                 ("Dung lượng Parquet silver", "{} MB".format(run["silver_size_mb"])),
                 ("Số dòng đầu vào / đầu ra", "{} / {}".format(fmt(run["rows_in"]),
                                                            fmt(run["rows_out"]))),
                 ("Thời gian làm sạch + ghi", "{} s".format(run["clean_seconds"])),
                 ("Thời gian kiểm tra lại", "{} s".format(run["validate_seconds"])),
                 ("Tổng thời gian", "{} s".format(run["total_seconds"]))]:
        L.append("| {} | {} |".format(k, v))

    L.append("\n## 2. Tổng quan trước / sau\n")
    L.append("| Bảng silver | Nguồn CSV | Dòng trước | Dòng sau | Cột trước → sau | "
             "CSV (MB) | Parquet (MB) | Số file | Trùng khoá sau | Thời gian (s) |")
    L.append("|---|---|---:|---:|---|---:|---:|---:|---:|---:|")
    for name, t in tables.items():
        v = t["validation"]
        L.append("| {} | {} | {} | {} | {} → {} | {} | {} | {} | {} | {} |".format(
            name, " + ".join("`{}`".format(s) for s in t["sources"]), fmt(t["rows_in"]),
            fmt(v["rows"]), t["columns_in"], len(v["schema"]), t["raw_size_mb"], v["size_mb"],
            v["parquet_files"], fmt(v["key_duplicate_rows"]), t["seconds"]))

    L.append("\n## 3. Các thao tác đã áp dụng\n")
    L.append("Cột *Số dòng*: số dòng bị tác động; `-` = áp dụng cho toàn bộ bảng.\n")
    for name, t in tables.items():
        L.append("### `{}`\n".format(name))
        L.append("| Loại | Thao tác | Số dòng |\n|---|---|---:|")
        for a in t["actions"]:
            L.append("| {} | {} | {} |".format(a["type"], a["action"],
                                               "-" if a["rows"] is None else fmt(a["rows"])))
        L.append("")

    L.append("## 4. Schema và giá trị NULL của các bảng silver\n")
    for name, t in tables.items():
        v = t["validation"]
        L.append("### `{}` ({} dòng, khoá: `{}`)\n".format(
            name, fmt(v["rows"]), ", ".join(v["key_columns"])))
        L.append("| Cột | Kiểu | NULL | % NULL |\n|---|---|---:|---:|")
        for col, dtype in v["schema"]:
            n = v["nulls"][col]
            L.append("| `{}` | {} | {} | {} |".format(
                col, dtype, fmt(n), round(100.0 * n / v["rows"], 2) if v["rows"] else 0))
        cols = [c for c, _ in v["schema"]]
        L.append("\n**Bản ghi mẫu**\n")
        L.append("| " + " | ".join("`{}`".format(c) for c in cols) + " |")
        L.append("|" + "---|" * len(cols))
        for s in v["samples"]:
            L.append("| " + " | ".join(md_cell(s[c], 20) for c in cols) + " |")
        L.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")


# =============================================================================
# MAIN
# =============================================================================
def main():
    job_timer = Timer().__enter__()
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    spark = (SparkSession.builder.appName("olist-02-clean")
             # Số partition sau mỗi lần shuffle (join/groupBy). Mặc định 200 là cho cụm
             # lớn; với cụm 4 core và dữ liệu ~100 MB, 200 partition = 200 task tí hon,
             # tốn thời gian điều phối hơn là xử lý. AQE (bật sẵn) sẽ tự gộp bớt thêm.
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    executors = wait_for_executors(sc)
    log.info("Spark %s | master=%s | executors=%d | parallelism=%d",
             spark.version, sc.master, executors, sc.defaultParallelism)

    # --- Đọc toàn bộ CSV thô (cache vì nhiều bảng được dùng nhiều lần) ---
    raw, raw_size = {}, {}
    for name, spec in TABLES.items():
        df, path, _ = read_raw_csv(spark, spec["file"], spec.get("multiline", False))
        raw[name] = df.cache()
        raw_size[name] = dir_size_mb(path)

    # --- Kế hoạch làm sạch: (bảng silver, các bảng nguồn, hàm làm sạch) ---
    # geolocation phải chạy trước vì customers/sellers cần toạ độ của nó.
    silver = {}
    plan = [
        ("geolocation", ["geolocation"], lambda: clean_geolocation(raw["geolocation"])),
        ("customers", ["customers", "geolocation"],
         lambda: clean_location_entity(raw["customers"], silver["geolocation"],
                                       "customer", "customer_id")),
        ("sellers", ["sellers", "geolocation"],
         lambda: clean_location_entity(raw["sellers"], silver["geolocation"],
                                       "seller", "seller_id")),
        ("products", ["products", "category_translation"],
         lambda: clean_products(raw["products"], raw["category_translation"])),
        ("orders", ["orders"],
         lambda: clean_orders(raw["orders"], raw["order_items"], raw["payments"])),
        ("order_items", ["order_items"], lambda: clean_order_items(raw["order_items"])),
        ("payments", ["payments"], lambda: clean_payments(raw["payments"])),
        ("reviews", ["reviews"], lambda: clean_reviews(raw["reviews"])),
    ]

    results, paths = {}, {}
    with Timer() as clean_timer:
        for name, sources, clean_fn in plan:
            with Timer() as t:
                df, actions = clean_fn()
                paths[name] = write_silver(df, name)
                # Các bảng sau dùng lại bản ĐÃ GHI (đọc từ Parquet) thay vì tính lại
                silver[name] = spark.read.parquet(paths[name])
            main_source = sources[0]
            results[name] = {
                "sources": [TABLES[s]["file"] for s in sources],
                "rows_in": raw[main_source].count(),
                "columns_in": len(raw[main_source].columns),
                "raw_size_mb": raw_size[main_source],
                "actions": actions.items,
                "seconds": t.seconds,
            }
            log.info("Làm sạch %-12s %9s dòng vào | %.2fs", name,
                     fmt(results[name]["rows_in"]), t.seconds)

    with Timer() as validate_timer:
        for name in results:
            v = validate_silver(spark, name, paths[name])
            results[name]["validation"] = v
            log.info("Kiểm tra %-12s %9s dòng ra | trùng khoá=%d | %d file | %.2f MB",
                     name, fmt(v["rows"]), v["key_duplicate_rows"], v["parquet_files"],
                     v["size_mb"])

    job_timer.__exit__()
    summary = {
        "run": {
            "started_at": started_at,
            "spark_version": spark.version,
            "master": sc.master,
            "executors": executors,
            "default_parallelism": sc.defaultParallelism,
            "raw_size_mb": round(sum(raw_size.values()), 2),
            "silver_size_mb": round(sum(r["validation"]["size_mb"] for r in results.values()), 2),
            "rows_in": sum(raw[n].count() for n in TABLES),
            "rows_out": sum(r["validation"]["rows"] for r in results.values()),
            "clean_seconds": clean_timer.seconds,
            "validate_seconds": validate_timer.seconds,
            "total_seconds": job_timer.seconds,
        },
        "tables": results,
    }
    with open(os.path.join(OUT_DIR, "cleaning_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    write_markdown(summary, os.path.join(OUT_DIR, "cleaning_report.md"))
    log.info("Xong sau %.2fs. Báo cáo: %s", job_timer.seconds,
             os.path.join(OUT_DIR, "cleaning_report.md"))
    spark.stop()


if __name__ == "__main__":
    main()
