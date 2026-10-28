"""
Các hàm dùng chung cho mọi job (profiling, clean, ...).

Chỉ chạy trên DRIVER (đọc file, log, đo giờ), nên không cần gửi file này cho executor.
"""

import logging
import os
import sys
import time

from pyspark.sql import functions as F

from olist_tables import RAW_DIR

BOM = "﻿"  # Ký tự ẩn "Byte Order Mark" mà Excel/Windows hay chèn vào đầu file

# Ký tự có dấu tiếng Bồ Đào Nha -> không dấu (dùng để chuẩn hoá tên địa danh)
ACCENTED = "áàâãäéèêëíìîïóòôõöúùûüçñ"
PLAIN = "aaaaaeeeeiiiiooooouuuucn"


# =============================================================================
# Log & đo thời gian
# =============================================================================
def setup_logger(name, log_path):
    """Logger ghi đồng thời ra màn hình và ra file (ghi đè mỗi lần chạy)."""
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for handler in (logging.StreamHandler(sys.stdout),
                    logging.FileHandler(log_path, mode="w", encoding="utf-8")):
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


class Timer:
    """Dùng với `with Timer() as t:` rồi đọc t.seconds - giống console.time() trong JS."""

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = round(time.perf_counter() - self.start, 2)


# =============================================================================
# Spark
# =============================================================================
def wait_for_executors(sc, timeout=60):
    """
    Chờ các worker cấp executor cho job. Ngay sau khi tạo SparkSession, executor
    có thể chưa kịp đăng ký; nếu đọc dữ liệu luôn thì Spark tưởng cụm chỉ có ít
    core và chia dữ liệu thành ít partition hơn.
    """
    count = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # trừ driver
        if count > 0:
            time.sleep(3)  # chờ thêm chút cho các executor còn lại
            return sc._jsc.sc().getExecutorMemoryStatus().size() - 1
        time.sleep(1)
    return count


def read_raw_csv(spark, file_name, multiline=False):
    """
    Đọc 1 file CSV thành DataFrame, GIỮ NGUYÊN mọi cột ở dạng chuỗi (string).
    Trả về (df, đường dẫn file, danh sách vấn đề phát hiện khi đọc).

    Vì sao không để Spark tự đoán kiểu (inferSchema)? Vì nếu Spark tự ép kiểu, giá
    trị sai định dạng sẽ âm thầm biến thành NULL và ta không biết dữ liệu gốc sai ở
    đâu. Ta tự ép kiểu có kiểm soát (profiling: để đếm lỗi; clean: để sửa).

    Về multiLine: bình thường Spark cắt 1 file CSV lớn thành nhiều mảnh (partition)
    theo vị trí byte, để nhiều executor đọc song song. Nhưng nếu một giá trị chứa
    dấu xuống dòng thì không cắt bừa được (có thể cắt giữa một bản ghi), nên ở chế
    độ multiLine Spark buộc phải đọc CẢ FILE bằng 1 task duy nhất. Vì vậy chỉ bật
    multiLine cho bảng thật sự cần (reviews).
    """
    path = os.path.join(RAW_DIR, file_name)
    df = (spark.read
          .option("header", True)          # Dòng đầu là tên cột
          .option("multiLine", multiline)
          .option("quote", '"')
          .option("escape", '"')           # Trong CSV chuẩn, dấu " bên trong chuỗi được viết là ""
          .option("encoding", "UTF-8")
          .csv(path))

    # Phát hiện & sửa BOM trong tên cột (nếu không sửa, join theo tên cột sẽ lỗi)
    issues = []
    for col in df.columns:
        if col.startswith(BOM):
            issues.append("Tên cột '{}' bắt đầu bằng ký tự BOM (U+FEFF) - đã bỏ BOM khi đọc"
                          .format(col.lstrip(BOM)))
            df = df.withColumnRenamed(col, col.lstrip(BOM))
    return df, path, issues


def write_parquet(df, path, partition_cols=None, single_file=False):
    """
    Ghi DataFrame ra Parquet (ghi đè). Spark ghi thành 1 THƯ MỤC chứa nhiều file
    part-xxxx.parquet (mỗi partition 1 file, các executor ghi song song).

    partition_cols: vd ["purchase_year", "purchase_month"] tạo cấu trúc
        <path>/purchase_year=2017/purchase_month=10/part-....parquet
      Truy vấn `WHERE purchase_year = 2018` chỉ đọc thư mục 2018 (partition
      pruning), giống index theo ngày trong database. Trước khi ghi ta
      repartition theo đúng các cột đó để mỗi thư mục chỉ có 1 file; nếu không,
      mỗi task ghi 1 file nhỏ vào MỌI thư mục ("small files problem").
    single_file: gộp về 1 file - dùng cho bảng tổng hợp nhỏ (vài nghìn dòng),
      để công cụ BI đọc cho tiện. KHÔNG dùng cho bảng lớn vì dồn hết vào 1 task.
    """
    if partition_cols:
        df.repartition(*partition_cols).write.mode("overwrite") \
            .partitionBy(*partition_cols).parquet(path)
    elif single_file:
        df.coalesce(1).write.mode("overwrite").parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)
    return path


def parquet_files(path):
    """Số file .parquet trong thư mục (đệ quy)."""
    return sum(1 for _, _, fs in os.walk(path) for f in fs if f.endswith(".parquet"))


def normalize_text(col):
    """
    Chuẩn hoá tên địa danh: "  São  Paulo " -> "sao paulo".
    chữ thường -> bỏ dấu -> thay dấu nháy lạ (` ´) bằng ' -> bỏ ký tự rác
    -> gộp nhiều khoảng trắng thành 1 -> cắt khoảng trắng 2 đầu.
    Trả về một biểu thức cột (Column), chưa chạy gì cho tới khi có action.
    """
    c = F.translate(F.lower(col), ACCENTED, PLAIN)
    c = F.regexp_replace(c, "[`´]", "'")
    c = F.regexp_replace(c, "[^a-z0-9 '()-]", "")
    c = F.regexp_replace(c, "\\s+", " ")
    return F.trim(c)


# =============================================================================
# Định dạng cho báo cáo Markdown
# =============================================================================
def fmt(n):
    """1234567 -> '1,234,567'"""
    return "{:,}".format(n) if isinstance(n, int) else str(n)


def md_cell(value, max_len=40):
    """Làm sạch giá trị để nhét vào ô bảng Markdown."""
    if value is None:
        return "*NULL*"
    s = str(value).replace("\n", " ").replace("\r", " ").replace("|", "\\|")
    return s if len(s) <= max_len else s[:max_len - 1] + "…"


def dir_size_mb(path):
    """Tổng dung lượng 1 thư mục (Spark ghi Parquet thành thư mục nhiều file)."""
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return round(total / 1024 / 1024, 2)
