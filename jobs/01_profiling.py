"""
BƯỚC 3 - DATA PROFILING (khảo sát chất lượng dữ liệu TRƯỚC khi xử lý)
=====================================================================

Mục đích: đọc 9 file CSV gốc của Olist bằng Spark và trả lời các câu hỏi
  - Mỗi bảng có bao nhiêu dòng, cột, chiếm bao nhiêu dung lượng?
  - Cột nào bị thiếu dữ liệu (NULL / rỗng), thiếu bao nhiêu %?
  - Có dòng nào bị trùng (trùng toàn bộ / trùng khoá chính) không?
  - Giá trị có đúng định dạng không (ngày tháng, số)?
  - Có vi phạm luật nghiệp vụ không (giao hàng trước khi mua, giá <= 0, ...)?
  - Các bảng có liên kết đúng với nhau không (khoá ngoại "mồ côi")?
Kết quả là phần "dữ liệu TRƯỚC xử lý" để so sánh với "SAU xử lý" ở bước làm sạch.

Cách chạy (từ thư mục gốc project, sau khi `docker compose up -d`):
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/jobs/01_profiling.py

Đầu ra (trong thư mục reports/profiling/):
    profiling_report.md     - báo cáo dễ đọc (dán vào báo cáo đồ án)
    profiling_summary.json  - số liệu thô dạng JSON (cho các bước sau so sánh)
    profiling_run.log       - log thời gian chạy từng bước

-----------------------------------------------------------------------------
3 khái niệm Spark cần biết để đọc file này (so sánh với lập trình web):

1. DataFrame ~ một bảng SQL / mảng object, NHƯNG được chia thành nhiều mảnh
   (partition) nằm rải rác trên các worker. Code của ta mô tả "làm gì với bảng",
   còn Spark tự lo chia việc cho từng mảnh.

2. Lazy evaluation (thực thi lười): các lệnh như select/filter/groupBy chỉ XÂY
   kế hoạch, chưa chạy gì cả - giống query builder (Knex, Eloquent) chưa gọi
   .get(). Chỉ khi gọi "action" như .count(), .collect() thì Spark mới thật sự
   đọc file và tính toán trên cụm.

3. Driver vs Executor: file Python này chạy trên DRIVER (ở container master).
   Các phép tính trên DataFrame được gửi tới EXECUTOR (trên worker) chạy song
   song. Chỉ kết quả nhỏ (vài con số) được .collect() về driver để ghi báo cáo.
   Không bao giờ .collect() cả bảng lớn về driver - sẽ tràn RAM.
-----------------------------------------------------------------------------
"""

import json
import logging
import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from olist_tables import FOREIGN_KEYS, RAW_DIR, REPORT_DIR, TABLES

OUT_DIR = os.path.join(REPORT_DIR, "profiling")
BOM = "﻿"  # Ký tự ẩn "Byte Order Mark" mà Excel/Windows hay chèn vào đầu file

# Ký tự có dấu tiếng Bồ Đào Nha -> không dấu (dùng cho kiểm tra tên thành phố)
ACCENTED = "áàâãäéèêëíìîïóòôõöúùûüç"
PLAIN = "aaaaaeeeeiiiiooooouuuuc"


# =============================================================================
# Tiện ích: log ra màn hình + file, và đo thời gian
# =============================================================================
def setup_logger():
    os.makedirs(OUT_DIR, exist_ok=True)
    logger = logging.getLogger("profiling")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for handler in (logging.StreamHandler(sys.stdout),
                    logging.FileHandler(os.path.join(OUT_DIR, "profiling_run.log"), mode="w",
                                        encoding="utf-8")):
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


log = setup_logger()


class Timer:
    """Dùng với `with Timer() as t:` rồi đọc t.seconds - giống console.time() trong JS."""

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = round(time.perf_counter() - self.start, 2)


# =============================================================================
# 1. Đọc dữ liệu thô
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

    Vì sao không để Spark tự đoán kiểu (inferSchema)? Vì ta đang khảo sát dữ liệu
    THÔ: nếu Spark tự ép kiểu, giá trị sai định dạng sẽ âm thầm biến thành NULL
    và ta không biết được dữ liệu gốc sai ở đâu.

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


# =============================================================================
# 2. Profiling 1 bảng
# =============================================================================
def profile_table(name, df, spec):
    """
    Tính toàn bộ chỉ số chất lượng cho 1 bảng.

    Kỹ thuật chính: gom TẤT CẢ phép đếm (null, sai định dạng, vi phạm luật...)
    vào MỘT lệnh df.agg(...). Spark chỉ cần quét dữ liệu 1 lần, mỗi executor
    đếm trên phần dữ liệu của mình rồi cộng dồn lại (đây chính là mô hình
    MapReduce: map = đếm cục bộ, reduce = cộng tổng).
    """
    columns = df.columns
    rules = list(spec["rules"].items())

    exprs = [F.count(F.lit(1)).alias("rows")]
    for i, c in enumerate(columns):
        col = F.col(c)
        # Thiếu dữ liệu = NULL hoặc chuỗi rỗng/toàn khoảng trắng
        exprs.append(F.sum(F.when(col.isNull() | (F.trim(col) == ""), 1).otherwise(0))
                     .alias("null_{}".format(i)))
        # Số giá trị khác nhau - dùng thuật toán xấp xỉ HyperLogLog (sai số ~5%)
        # vì đếm chính xác trên dữ liệu lớn rất tốn RAM. Đây là kỹ thuật Big Data phổ biến.
        exprs.append(F.approx_count_distinct(col).alias("distinct_{}".format(i)))
        # Sai định dạng = có giá trị nhưng không ép được sang kiểu mong đợi
        if c in spec["types"]:
            cast_expr = F.expr("try_cast(`{}` AS {})".format(c, spec["types"][c]))
            exprs.append(F.sum(F.when(col.isNotNull() & cast_expr.isNull(), 1).otherwise(0))
                         .alias("invalid_{}".format(i)))
    for j, (_, condition) in enumerate(rules):
        exprs.append(F.sum(F.when(F.expr(condition), 1).otherwise(0)).alias("rule_{}".format(j)))

    agg = df.agg(*exprs).collect()[0].asDict()   # <- ACTION: Spark bắt đầu chạy thật
    rows = agg["rows"]

    def pct(x):
        return round(100.0 * x / rows, 2) if rows else 0.0

    column_stats = []
    for i, c in enumerate(columns):
        stat = {
            "column": c,
            "expected_type": spec["types"].get(c, "string"),
            "missing": agg["null_{}".format(i)],
            "missing_pct": pct(agg["null_{}".format(i)]),
            "approx_distinct": agg["distinct_{}".format(i)],
        }
        if c in spec["types"]:
            stat["invalid_format"] = agg["invalid_{}".format(i)]
        column_stats.append(stat)

    rule_stats = [{"rule": desc, "condition": cond,
                   "violations": agg["rule_{}".format(j)],
                   "violations_pct": pct(agg["rule_{}".format(j)])}
                  for j, (desc, cond) in enumerate(rules)]

    # Trùng lặp: so sánh tổng số dòng với số dòng sau khi loại trùng (distinct)
    full_duplicates = rows - df.distinct().count()
    key_duplicates = None
    if spec["keys"]:
        key_duplicates = rows - df.select(*spec["keys"]).distinct().count()

    # Phân bố giá trị các cột phân loại (giống SELECT col, COUNT(*) GROUP BY col)
    distributions = {}
    for c in spec["categorical"]:
        top = df.groupBy(c).count().orderBy(F.desc("count")).limit(15).collect()
        distributions[c] = [{"value": r[c], "count": r["count"], "pct": pct(r["count"])}
                            for r in top]

    # Vài bản ghi mẫu (chỉ 3 dòng nên .collect() an toàn)
    samples = [r.asDict() for r in df.limit(3).collect()]

    return {
        "rows": rows,
        "columns": len(columns),
        "full_duplicate_rows": full_duplicates,
        "key_columns": spec["keys"],
        "key_duplicate_rows": key_duplicates,
        "column_stats": column_stats,
        "rule_checks": rule_stats,
        "distributions": distributions,
        "samples": samples,
    }


# =============================================================================
# 3. Các kiểm tra riêng cho từng bảng (không viết được bằng 1 điều kiện đơn giản)
# =============================================================================
def extra_checks(name, df):
    extras = {}
    if name == "geolocation":
        # Một mã zip có nhiều dòng toạ độ -> bảng này rất "phình" và cần gộp lại
        per_zip = df.groupBy("geolocation_zip_code_prefix").count()
        z = per_zip.agg(F.count(F.lit(1)).alias("zips"),
                        F.avg("count").alias("avg"),
                        F.max("count").alias("max")).collect()[0]
        extras["Số mã zip khác nhau"] = z["zips"]
        extras["Trung bình số dòng toạ độ / mã zip"] = round(z["avg"], 1)
        extras["Nhiều nhất số dòng toạ độ / mã zip"] = z["max"]

        # Cùng 1 zip nhưng tên thành phố viết khác nhau (vd: "são paulo" vs "sao paulo")
        normalized = F.lower(F.trim(F.translate("geolocation_city", ACCENTED, PLAIN)))
        city = (df.groupBy("geolocation_zip_code_prefix")
                .agg(F.countDistinct("geolocation_city").alias("raw"),
                     F.countDistinct(normalized).alias("norm")))
        c = city.agg(F.sum(F.when(F.col("raw") > 1, 1).otherwise(0)).alias("raw"),
                     F.sum(F.when(F.col("norm") > 1, 1).otherwise(0)).alias("norm")).collect()[0]
        extras["Mã zip có >1 cách viết tên thành phố (dữ liệu gốc)"] = c["raw"]
        extras["Mã zip có >1 cách viết tên thành phố (sau khi bỏ dấu + chữ thường)"] = c["norm"]
        extras["Số cách viết khác nhau của tên thành phố gốc"] = (
            df.select("geolocation_city").distinct().count())
        extras["Số tên thành phố sau khi bỏ dấu + chữ thường"] = (
            df.select(normalized.alias("c")).distinct().count())

    if name == "reviews":
        per_order = df.groupBy("order_id").count()
        extras["Số đơn hàng có nhiều hơn 1 review"] = per_order.filter("count > 1").count()

    if name == "customers":
        # customer_id là id theo TỪNG ĐƠN; customer_unique_id mới là "người thật"
        extras["Số khách hàng thật (customer_unique_id khác nhau)"] = (
            df.select("customer_unique_id").distinct().count())
    return extras


# =============================================================================
# 4. Kiểm tra khoá ngoại giữa các bảng
# =============================================================================
def check_foreign_keys(frames):
    """
    Với mỗi quan hệ con -> cha, đếm số dòng ở bảng con có khoá ngoại không tìm
    thấy ở bảng cha. Dùng LEFT ANTI JOIN = "lấy các dòng bên trái KHÔNG khớp
    bên phải" (tương đương WHERE NOT EXISTS trong SQL).
    Join là phép tốn kém nhất trong Spark vì phải "shuffle" - chuyển dữ liệu
    giữa các worker qua mạng để các dòng có cùng khoá nằm chung một chỗ.
    """
    results = []
    for child, ccol, parent, pcol, desc in FOREIGN_KEYS:
        with Timer() as t:
            child_df = frames[child].where(F.col(ccol).isNotNull())
            parent_keys = frames[parent].select(F.col(pcol).alias("_parent_key")).distinct()
            orphans = child_df.join(parent_keys, child_df[ccol] == parent_keys["_parent_key"],
                                    "left_anti")
            stat = orphans.agg(F.count(F.lit(1)).alias("rows"),
                               F.countDistinct(ccol).alias("values")).collect()[0]
            total = child_df.count()
        results.append({
            "check": desc,
            "relation": "{}.{} -> {}.{}".format(child, ccol, parent, pcol),
            "orphan_rows": stat["rows"],
            "orphan_pct": round(100.0 * stat["rows"] / total, 2) if total else 0.0,
            "orphan_distinct_values": stat["values"],
            "seconds": t.seconds,
        })
        log.info("FK  %-60s mồ côi=%d (%.2fs)", results[-1]["relation"], stat["rows"], t.seconds)
    return results


# =============================================================================
# 5. Xuất báo cáo Markdown
# =============================================================================
def md_cell(value, max_len=40):
    """Làm sạch giá trị để nhét vào ô bảng Markdown."""
    if value is None:
        return "*NULL*"
    s = str(value).replace("\n", " ").replace("\r", " ").replace("|", "\\|")
    return s if len(s) <= max_len else s[:max_len - 1] + "…"


def fmt(n):
    return "{:,}".format(n) if isinstance(n, int) else str(n)


def write_markdown(summary, path):
    run = summary["run"]
    L = []
    L.append("# Báo cáo Data Profiling - dữ liệu Olist (TRƯỚC xử lý)\n")
    L.append("> File này được sinh tự động bởi `jobs/01_profiling.py`. "
             "Cách đọc báo cáo: xem `docs/02-data-profiling.md`.\n")

    L.append("## 1. Thông tin lần chạy\n")
    L.append("| Thuộc tính | Giá trị |\n|---|---|")
    for k, v in [("Thời điểm chạy", run["started_at"]),
                 ("Phiên bản Spark", run["spark_version"]),
                 ("Spark master", run["master"]),
                 ("Số executor", run["executors"]),
                 ("Số core xử lý song song (defaultParallelism)", run["default_parallelism"]),
                 ("Tổng dung lượng dữ liệu gốc", "{} MB".format(run["total_size_mb"])),
                 ("Tổng số bản ghi", fmt(run["total_rows"])),
                 ("Thời gian profiling 9 bảng", "{} s".format(run["tables_seconds"])),
                 ("Thời gian kiểm tra khoá ngoại", "{} s".format(run["fk_seconds"])),
                 ("Tổng thời gian chạy job", "{} s".format(run["total_seconds"]))]:
        L.append("| {} | {} |".format(k, v))

    L.append("\n## 2. Tổng quan các bảng\n")
    L.append("| Bảng | File | Dung lượng (MB) | Số dòng | Số cột | Partition | "
             "Dòng trùng hoàn toàn | Trùng khoá chính | Thời gian (s) |")
    L.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for name, t in summary["tables"].items():
        key_dup = "-" if t["key_duplicate_rows"] is None else fmt(t["key_duplicate_rows"])
        L.append("| {} | `{}` | {} | {} | {} | {} | {} | {} | {} |".format(
            name, t["file"], t["size_mb"], fmt(t["rows"]), t["columns"], t["partitions"],
            fmt(t["full_duplicate_rows"]), key_dup, t["seconds"]))

    L.append("\n## 3. Vấn đề phát hiện khi đọc file\n")
    for issue in summary["issues"] or ["Không có (Spark đọc đúng header của cả 9 file)."]:
        L.append("- " + issue)

    L.append("\n## 4. Kiểm tra khoá ngoại (toàn vẹn tham chiếu)\n")
    L.append("| Kiểm tra | Quan hệ | Số dòng mồ côi | % | Số giá trị khác nhau |")
    L.append("|---|---|---:|---:|---:|")
    for fk in summary["foreign_keys"]:
        L.append("| {} | `{}` | {} | {} | {} |".format(
            fk["check"], fk["relation"], fmt(fk["orphan_rows"]), fk["orphan_pct"],
            fmt(fk["orphan_distinct_values"])))

    L.append("\n## 5. Chi tiết từng bảng\n")
    for name, t in summary["tables"].items():
        L.append("### 5.{}. `{}` ({} dòng)\n".format(
            list(summary["tables"]).index(name) + 1, name, fmt(t["rows"])))
        key = ", ".join(t["key_columns"]) if t["key_columns"] else "(không có khoá duy nhất)"
        L.append("Khoá chính: `{}`\n".format(key))

        L.append("| Cột | Kiểu mong đợi | Thiếu (NULL/rỗng) | % thiếu | Số giá trị khác nhau (≈) "
                 "| Sai định dạng |")
        L.append("|---|---|---:|---:|---:|---:|")
        for c in t["column_stats"]:
            L.append("| `{}` | {} | {} | {} | {} | {} |".format(
                c["column"], c["expected_type"], fmt(c["missing"]), c["missing_pct"],
                fmt(c["approx_distinct"]), fmt(c.get("invalid_format", "-"))))

        if t["rule_checks"]:
            L.append("\n**Kiểm tra luật nghiệp vụ**\n")
            L.append("| Luật | Số dòng vi phạm | % |\n|---|---:|---:|")
            for r in t["rule_checks"]:
                L.append("| {} | {} | {} |".format(r["rule"], fmt(r["violations"]),
                                                    r["violations_pct"]))

        if t["extras"]:
            L.append("\n**Chỉ số bổ sung**\n")
            L.append("| Chỉ số | Giá trị |\n|---|---:|")
            for k, v in t["extras"].items():
                L.append("| {} | {} |".format(k, fmt(v)))

        for col, dist in t["distributions"].items():
            L.append("\n**Phân bố giá trị `{}`** (top 15)\n".format(col))
            L.append("| Giá trị | Số dòng | % |\n|---|---:|---:|")
            for d in dist:
                L.append("| {} | {} | {} |".format(md_cell(d["value"]), fmt(d["count"]), d["pct"]))

        if t["samples"]:
            cols = list(t["samples"][0].keys())
            L.append("\n**Bản ghi mẫu**\n")
            L.append("| " + " | ".join("`{}`".format(c) for c in cols) + " |")
            L.append("|" + "---|" * len(cols))
            for s in t["samples"]:
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

    # SparkSession = "cổng kết nối" tới cụm Spark (giống tạo DB connection).
    # Địa chỉ master được truyền qua tham số --master của spark-submit.
    spark = SparkSession.builder.appName("olist-01-profiling").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Giảm bớt log của Spark cho dễ đọc
    sc = spark.sparkContext
    executors = wait_for_executors(sc)
    log.info("Spark %s | master=%s | executors=%d | parallelism=%d",
             spark.version, sc.master, executors, sc.defaultParallelism)

    frames, tables, issues = {}, {}, []
    with Timer() as tables_timer:
        for name, spec in TABLES.items():
            with Timer() as t:
                df, path, file_issues = read_raw_csv(spark, spec["file"],
                                                     spec.get("multiline", False))
                issues.extend("`{}`: {}".format(spec["file"], i) for i in file_issues)
                # cache(): giữ bảng trong RAM của executor sau lần đọc đầu tiên,
                # vì ta sẽ quét bảng này nhiều lần (đếm, distinct, join...).
                # Không cache thì mỗi lần đều phải đọc & parse lại file CSV.
                df = df.cache()
                frames[name] = df
                result = profile_table(name, df, spec)
                result["extras"] = extra_checks(name, df)
            result.update({
                "file": spec["file"],
                "size_mb": round(os.path.getsize(path) / 1024 / 1024, 2),
                # Số mảnh dữ liệu Spark chia ra = số task chạy song song khi quét bảng
                "partitions": df.rdd.getNumPartitions(),
                "seconds": t.seconds,
            })
            tables[name] = result
            log.info("Bảng %-22s %9s dòng | %2d cột | %d partition | trùng=%d | %.2fs",
                     name, fmt(result["rows"]), result["columns"], result["partitions"],
                     result["full_duplicate_rows"], t.seconds)

    with Timer() as fk_timer:
        foreign_keys = check_foreign_keys(frames)

    job_timer.__exit__()
    summary = {
        "run": {
            "started_at": started_at,
            "spark_version": spark.version,
            "master": sc.master,
            "executors": executors,
            "default_parallelism": sc.defaultParallelism,
            "total_size_mb": round(sum(t["size_mb"] for t in tables.values()), 2),
            "total_rows": sum(t["rows"] for t in tables.values()),
            "tables_seconds": tables_timer.seconds,
            "fk_seconds": fk_timer.seconds,
            "total_seconds": job_timer.seconds,
        },
        "issues": issues,
        "tables": tables,
        "foreign_keys": foreign_keys,
    }

    with open(os.path.join(OUT_DIR, "profiling_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    write_markdown(summary, os.path.join(OUT_DIR, "profiling_report.md"))
    log.info("Xong sau %.2fs. Báo cáo: reports/profiling/profiling_report.md", job_timer.seconds)
    spark.stop()


if __name__ == "__main__":
    main()
