"""
BƯỚC 6 - SINH DỮ LIỆU NHÂN BẢN để thí nghiệm khả năng mở rộng (scalability)
===========================================================================

Bộ Olist gốc chỉ ~120 MB. Để kiểm chứng pipeline chạy được khi dữ liệu lớn hơn
nhiều, ta nhân bản dữ liệu thô lên N lần, GIỮ NGUYÊN định dạng CSV gốc, rồi cho
CÙNG pipeline (02_clean, 03_gold) chạy trên đó.

Cách nhân bản - mỗi bản sao k = 1..N-1 là một "sàn thương mại song song":
  - Mọi cột ID (order_id, customer_id, product_id, seller_id, review_id,
    customer_unique_id) được thêm hậu tố "_k" -> ID không trùng giữa các bản sao,
    nhưng quan hệ khoá ngoại vẫn đúng (order_items của bản k trỏ tới orders của bản k).
  - Ngày tháng, giá tiền giữ nguyên -> phân bố dữ liệu giống hệt bản gốc, nên khi
    đo thời gian, khác biệt chỉ đến từ KHỐI LƯỢNG dữ liệu.
  - geolocation được nhân nguyên văn (không có ID) -> tạo thêm dòng trùng, giống
    dữ liệu thô thật (bước làm sạch sẽ phải loại trùng nhiều hơn).
  - Bảng dịch danh mục (71 dòng) giữ nguyên - danh mục không tăng theo số đơn.
Vì cách nhân bản có quy luật, kết quả tổng hợp ở x N phải đúng bằng N lần x1
(vd doanh thu) - đây cũng là một cách kiểm tra tính đúng của pipeline.

Cách chạy:
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/jobs/scale_data.py 10
Đầu ra: data/scale/x10/<tên file gốc>.csv/part-*.csv  (mỗi "file" là 1 thư mục)
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import Timer, dir_size_mb, read_raw_csv, wait_for_executors
from olist_tables import TABLES

SCALE_ROOT = "/data/lake/scale"   # = thư mục data/scale trên máy thật

# Cột ID cần thêm hậu tố ở từng bảng
ID_COLUMNS = {
    "orders": ["order_id", "customer_id"],
    "order_items": ["order_id", "product_id", "seller_id"],
    "payments": ["order_id"],
    "reviews": ["review_id", "order_id"],
    "customers": ["customer_id", "customer_unique_id"],
    "sellers": ["seller_id"],
    "products": ["product_id"],
    "geolocation": [],
}
NOT_SCALED = {"category_translation"}


def scale_table(spark, df, id_cols, factor):
    """
    Nhân df lên `factor` lần bằng CROSS JOIN với bảng số 0..factor-1
    (giống `SELECT * FROM t CROSS JOIN generate_series(0, N-1) AS copy` trong SQL).
    """
    copies = spark.range(factor).withColumnRenamed("id", "_copy")
    out = df.crossJoin(F.broadcast(copies))
    for c in id_cols:
        # Bản sao 0 giữ nguyên ID gốc; bản k>0 thành "<id>_k"
        out = out.withColumn(c, F.when(F.col("_copy") == 0, F.col(c))
                             .otherwise(F.concat(F.col(c), F.lit("_"), F.col("_copy"))))
    return out.drop("_copy")


def main():
    factor = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    out_dir = os.path.join(SCALE_ROOT, "x{}".format(factor))
    spark = SparkSession.builder.appName("olist-scale-x{}".format(factor)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    wait_for_executors(spark.sparkContext)

    with Timer() as total:
        for name, spec in TABLES.items():
            df, _, _ = read_raw_csv(spark, spec["file"], spec.get("multiline", False))
            if name not in NOT_SCALED:
                df = scale_table(spark, df, ID_COLUMNS[name], factor)
                # Chia thành `factor` file (tối thiểu 4) cho mỗi bảng. Với reviews (đọc
                # multiLine, không cắt file được) số file = số task đọc song song.
                df = df.repartition(max(4, factor))
            path = os.path.join(out_dir, spec["file"])
            # Ghi đúng kiểu CSV gốc: có header, dấu " bên trong chuỗi viết thành ""
            (df.write.mode("overwrite")
             .option("header", True).option("quote", '"').option("escape", '"')
             .csv(path))
            print("x{} {:<22} {:>8.1f} MB".format(factor, name, dir_size_mb(path)))
    print("Xong x{}: {} MB trong {:.1f}s -> {}".format(
        factor, dir_size_mb(out_dir), total.seconds, out_dir))
    spark.stop()


if __name__ == "__main__":
    main()
