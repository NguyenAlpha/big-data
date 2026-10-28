"""
BƯỚC 5 - KẾT HỢP & TỔNG HỢP (silver Parquet -> gold Parquet) bằng Spark SQL
==========================================================================

Đọc 8 bảng silver, JOIN thành 2 bảng fact rồi GROUP BY thành 5 bảng tổng hợp
(KPI) cho nhóm Analytics / dashboard. Toàn bộ logic nghiệp vụ viết bằng SQL
trong thư mục jobs/sql/ - file Python này chỉ điều phối:
    đọc SQL -> chạy -> ghi Parquet -> đọc lại -> đối chiếu số liệu -> báo cáo.

Cách chạy (cần chạy 02_clean.py trước để có dữ liệu silver):
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/jobs/03_gold.py

Đầu ra:
    data/gold/<bảng>/                - 2 bảng fact + 5 bảng tổng hợp (Parquet)
    reports/gold/gold_report.md      - số dòng, đối chiếu số liệu, xem trước KPI
    reports/gold/gold_summary.json   - số liệu thô
    reports/gold/gold_run.log        - log thời gian
    reports/gold/plans/<bảng>.txt    - kế hoạch thực thi (execution plan) của Spark

-----------------------------------------------------------------------------
Spark SQL vs DataFrame API: hai cách viết, CÙNG một bộ máy thực thi.
  spark.sql("SELECT ... GROUP BY ...")  ==  df.groupBy(...).agg(...)
Spark dịch cả hai thành cùng một kế hoạch (qua bộ tối ưu Catalyst) rồi chạy
phân tán trên cụm. Ở bước này ta chọn SQL vì logic là join/group by thuần tuý,
và SQL dễ đọc, dễ review với người quen database.

"Temp view" = đặt tên cho 1 DataFrame để dùng được trong câu SQL (giống CREATE
VIEW, nhưng chỉ tồn tại trong phiên chạy này, không ghi gì ra đĩa).
"""

import io
import json
import os
import re
from datetime import datetime

from pyspark.sql import SparkSession

from common import (Timer, dir_size_mb, fmt, md_cell, parquet_files, setup_logger,
                    wait_for_executors, write_parquet)
from olist_tables import GOLD_DIR, REPORT_DIR, SILVER_DIR

OUT_DIR = os.path.join(REPORT_DIR, "gold")
PLAN_DIR = os.path.join(OUT_DIR, "plans")
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
log = setup_logger("gold", os.path.join(OUT_DIR, "gold_run.log"))

SILVER_TABLES = ["customers", "sellers", "products", "orders",
                 "order_items", "payments", "reviews", "geolocation"]

TIME_PARTITION = ["purchase_year", "purchase_month"]

# Các bước, chạy THEO THỨ TỰ (bước sau dùng kết quả bước trước):
#   (tên = tên file SQL = tên view/bảng, loại, mô tả "1 dòng = ?")
#   view: chỉ đăng ký view trung gian, không ghi file
#   fact: bảng chi tiết lớn -> ghi chia thư mục theo năm/tháng
#   agg : bảng tổng hợp nhỏ -> ghi 1 file
STEPS = [
    ("order_payments", "view", "1 đơn hàng (gộp các lần thanh toán)"),
    ("order_reviews", "view", "1 đơn hàng (review mới nhất)"),
    ("fact_order_items", "fact", "1 sản phẩm trong 1 đơn hàng"),
    ("fact_orders", "fact", "1 đơn hàng"),
    ("agg_sales_monthly", "agg", "1 tháng"),
    ("agg_category_monthly", "agg", "1 danh mục x 1 tháng"),
    ("agg_customer_state", "agg", "1 bang của khách hàng"),
    ("agg_seller_performance", "agg", "1 người bán"),
    ("agg_customer_rfm", "agg", "1 khách hàng thật"),
]

# ĐỐI CHIẾU SỐ LIỆU (reconciliation): mỗi cặp câu SQL phải ra CÙNG một con số.
# Đây là cách kiểm chứng join không làm mất / nhân bản dòng, và các bảng tổng hợp
# cộng lại khớp với dữ liệu chi tiết. Nếu một kiểm tra sai -> job báo lỗi.
RECONCILIATION = [
    ("fact_order_items giữ nguyên số dòng của silver order_items (join không mất/nhân dòng)",
     "SELECT COUNT(*) FROM fact_order_items", "SELECT COUNT(*) FROM order_items"),
    ("Tổng giá sản phẩm không đổi sau khi join",
     "SELECT SUM(price) FROM fact_order_items", "SELECT SUM(price) FROM order_items"),
    ("fact_orders giữ nguyên số đơn của silver orders",
     "SELECT COUNT(*) FROM fact_orders", "SELECT COUNT(*) FROM orders"),
    ("fact_orders: tổng giá trị hàng khớp silver order_items",
     "SELECT SUM(items_value) FROM fact_orders", "SELECT SUM(price) FROM order_items"),
    ("Hai bảng fact thống nhất về doanh thu đơn hợp lệ",
     "SELECT SUM(items_value) FROM fact_orders WHERE is_valid_sale",
     "SELECT SUM(price) FROM fact_order_items WHERE is_valid_sale"),
    ("Doanh thu các tháng cộng lại = doanh thu đơn hợp lệ",
     "SELECT SUM(revenue) FROM agg_sales_monthly",
     "SELECT SUM(items_value) FROM fact_orders WHERE is_valid_sale"),
    ("Doanh thu các danh mục cộng lại = doanh thu theo tháng",
     "SELECT SUM(revenue) FROM agg_category_monthly", "SELECT SUM(revenue) FROM agg_sales_monthly"),
    ("Doanh thu các bang cộng lại = doanh thu theo tháng",
     "SELECT SUM(revenue) FROM agg_customer_state", "SELECT SUM(revenue) FROM agg_sales_monthly"),
    ("Doanh thu các người bán cộng lại = doanh thu theo tháng",
     "SELECT SUM(revenue) FROM agg_seller_performance", "SELECT SUM(revenue) FROM agg_sales_monthly"),
    ("Số đơn trong RFM = số đơn theo tháng",
     "SELECT SUM(frequency) FROM agg_customer_rfm", "SELECT SUM(orders) FROM agg_sales_monthly"),
    ("Số khách trong RFM = số khách thật có đơn hợp lệ",
     "SELECT COUNT(*) FROM agg_customer_rfm",
     "SELECT COUNT(DISTINCT customer_unique_id) FROM fact_orders WHERE is_valid_sale"),
]

# Xem trước kết quả trong báo cáo (để kiểm tra nhanh bằng mắt & dùng cho demo)
PREVIEWS = [
    ("Doanh thu theo tháng",
     "SELECT purchase_year AS year, purchase_month AS month, orders, customers, revenue, gmv, "
     "avg_order_value, avg_review_score, avg_delivery_days, late_delivery_pct "
     "FROM agg_sales_monthly ORDER BY year, month"),
    ("Top 10 danh mục theo doanh thu (cả giai đoạn)",
     "SELECT category, SUM(orders) AS orders, SUM(items) AS items, SUM(revenue) AS revenue, "
     "ROUND(SUM(revenue) / SUM(items), 2) AS avg_price "
     "FROM agg_category_monthly GROUP BY category ORDER BY revenue DESC LIMIT 10"),
    ("Top 10 bang theo doanh thu",
     "SELECT customer_state, orders, customers, revenue, avg_order_value, freight_pct_of_revenue, "
     "avg_delivery_days, late_delivery_pct, avg_review_score "
     "FROM agg_customer_state ORDER BY revenue DESC LIMIT 10"),
    ("Top 10 người bán theo doanh thu",
     "SELECT seller_id, seller_state, orders, revenue, avg_review_score, avg_delivery_days, "
     "late_delivery_pct, avg_distance_km, active_months "
     "FROM agg_seller_performance ORDER BY revenue DESC LIMIT 10"),
    ("Phân bố số đơn mỗi khách (Frequency trong RFM)",
     "SELECT frequency AS orders_per_customer, COUNT(*) AS customers, "
     "ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct "
     "FROM agg_customer_rfm GROUP BY frequency ORDER BY frequency"),
]


# =============================================================================
# Tiện ích
# =============================================================================
def read_sql(name):
    with io.open(os.path.join(SQL_DIR, name + ".sql"), encoding="utf-8") as f:
        return f.read()


def scalar(spark, sql):
    """Chạy câu SQL trả về đúng 1 giá trị."""
    return spark.sql(sql).collect()[0][0]


def save_plan(spark, df, name):
    """
    Lưu kế hoạch thực thi (giống EXPLAIN trong MySQL) ra file, và đếm loại join
    Spark đã chọn:
      - BroadcastHashJoin: bảng nhỏ được gửi tới mọi executor -> không shuffle bảng lớn
      - SortMergeJoin:     cả 2 bảng đều lớn -> shuffle cả 2 theo khoá rồi ghép
    Lưu ý: với AQE (Adaptive Query Execution), Spark có thể đổi chiến lược lúc chạy
    dựa trên kích thước thật; kế hoạch cuối cùng xem ở tab SQL của Spark UI (:4040).
    """
    plan = spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    os.makedirs(PLAN_DIR, exist_ok=True)
    with io.open(os.path.join(PLAN_DIR, name + ".txt"), "w", encoding="utf-8") as f:
        f.write(plan)
    tree = plan.split("\n\n")[0]  # phần cây kế hoạch (trước phần chi tiết từng bước)
    return {j: len(re.findall(j, tree)) for j in
            ("BroadcastHashJoin", "SortMergeJoin", "BroadcastNestedLoopJoin")}


NO_THOUSANDS = {"year", "month", "orders_per_customer"}  # không in 2016 thành "2,016"


def md_table(rows, max_len=24):
    if not rows:
        return ["*(không có dữ liệu)*"]
    cols = list(rows[0].keys())
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        cells = [r[c] if c in NO_THOUSANDS or not isinstance(r[c], int) else fmt(r[c])
                 for c in cols]
        lines.append("| " + " | ".join(md_cell(v, max_len) for v in cells) + " |")
    return lines


# =============================================================================
# Báo cáo
# =============================================================================
def write_markdown(summary, path):
    run = summary["run"]
    L = ["# Báo cáo tầng gold - silver → fact & bảng tổng hợp\n",
         "> Sinh tự động bởi `jobs/03_gold.py`. Giải thích chi tiết: "
         "`docs/04-ket-hop-tong-hop.md`.\n",
         "## 1. Thông tin lần chạy\n",
         "| Thuộc tính | Giá trị |\n|---|---|"]
    for k, v in [("Thời điểm chạy", run["started_at"]),
                 ("Spark master", "{} ({} executor, {} core)".format(
                     run["master"], run["executors"], run["default_parallelism"])),
                 ("Dữ liệu vào (silver)", "{} dòng, {} MB".format(
                     fmt(run["silver_rows"]), run["silver_size_mb"])),
                 ("Dữ liệu ra (gold)", "{} bảng, {} MB".format(
                     len(summary["tables"]), run["gold_size_mb"])),
                 ("Thời gian tạo bảng", "{} s".format(run["build_seconds"])),
                 ("Thời gian đối chiếu số liệu", "{} s".format(run["reconcile_seconds"])),
                 ("Tổng thời gian", "{} s".format(run["total_seconds"]))]:
        L.append("| {} | {} |".format(k, v))

    L.append("\n## 2. Các bảng gold\n")
    L.append("| Bảng | Loại | 1 dòng = | Số dòng | Số cột | Parquet (MB) | Số file | "
             "Broadcast join | Sort-merge join | Thời gian (s) |")
    L.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for name, t in summary["tables"].items():
        L.append("| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            name, t["kind"], t["grain"], fmt(t["rows"]), len(t["schema"]), t["size_mb"],
            t["files"], t["joins"]["BroadcastHashJoin"], t["joins"]["SortMergeJoin"],
            t["seconds"]))

    L.append("\n## 3. Đối chiếu số liệu (reconciliation)\n")
    passed = sum(1 for c in summary["reconciliation"] if c["ok"])
    L.append("**{}/{} kiểm tra đạt.**\n".format(passed, len(summary["reconciliation"])))
    L.append("| Kiểm tra | Vế trái | Vế phải | Kết quả |\n|---|---:|---:|---|")
    for c in summary["reconciliation"]:
        L.append("| {} | {} | {} | {} |".format(c["check"], fmt(c["left"]), fmt(c["right"]),
                                               "ĐẠT" if c["ok"] else "**SAI**"))

    L.append("\n## 4. Xem trước kết quả\n")
    for title, rows in summary["previews"].items():
        L.append("### {}\n".format(title))
        L.extend(md_table(rows))
        L.append("")

    L.append("## 5. Schema các bảng gold\n")
    for name, t in summary["tables"].items():
        L.append("<details><summary><code>{}</code> ({} cột)</summary>\n".format(
            name, len(t["schema"])))
        L.append("| Cột | Kiểu |\n|---|---|")
        for col, dtype in t["schema"]:
            L.append("| `{}` | {} |".format(col, dtype))
        L.append("\n</details>\n")

    with io.open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")


# =============================================================================
# MAIN
# =============================================================================
def main():
    job_timer = Timer().__enter__()
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    spark = (SparkSession.builder.appName("olist-03-gold")
             .config("spark.sql.shuffle.partitions", "16")  # xem giải thích ở 02_clean.py
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    executors = wait_for_executors(sc)
    log.info("Spark %s | master=%s | executors=%d | parallelism=%d",
             spark.version, sc.master, executors, sc.defaultParallelism)

    # --- Đăng ký 8 bảng silver thành view để dùng trong SQL ---
    silver_rows, silver_size = 0, 0.0
    for name in SILVER_TABLES:
        path = os.path.join(SILVER_DIR, name)
        if not os.path.isdir(path):
            raise SystemExit("Không thấy {} - hãy chạy jobs/02_clean.py trước.".format(path))
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(name)
        silver_rows += df.count()
        silver_size += dir_size_mb(path)

    # --- Chạy từng bước SQL ---
    tables = {}
    with Timer() as build_timer:
        for name, kind, grain in STEPS:
            with Timer() as t:
                df = spark.sql(read_sql(name))  # chỉ lập kế hoạch, chưa chạy
                if kind == "view":
                    df.createOrReplaceTempView(name)
                    continue
                joins = save_plan(spark, df, name)
                path = os.path.join(GOLD_DIR, name)
                write_parquet(df, path,  # <- ACTION: Spark chạy thật ở đây
                              partition_cols=TIME_PARTITION if kind == "fact" else None,
                              single_file=(kind == "agg"))
                # Các bước sau dùng bản ĐÃ GHI thay vì tính lại toàn bộ chuỗi join
                written = spark.read.parquet(path)
                written.createOrReplaceTempView(name)
                rows = written.count()
            tables[name] = {
                "kind": kind, "grain": grain, "rows": rows,
                "schema": [(f.name, f.dataType.simpleString()) for f in written.schema.fields],
                "size_mb": dir_size_mb(path), "files": parquet_files(path),
                "joins": joins, "seconds": t.seconds,
            }
            log.info("%-24s %9s dòng | %2d file | broadcast=%d sortmerge=%d | %.2fs",
                     name, fmt(rows), tables[name]["files"], joins["BroadcastHashJoin"],
                     joins["SortMergeJoin"], t.seconds)

    # --- Đối chiếu số liệu ---
    checks = []
    with Timer() as rec_timer:
        for desc, left_sql, right_sql in RECONCILIATION:
            left, right = scalar(spark, left_sql), scalar(spark, right_sql)
            checks.append({"check": desc, "left_sql": left_sql, "right_sql": right_sql,
                           "left": left, "right": right, "ok": left == right})
            log.info("Đối chiếu %-4s %s", "OK" if left == right else "SAI", desc)

    previews = {title: [r.asDict() for r in spark.sql(sql).collect()]
                for title, sql in PREVIEWS}

    job_timer.__exit__()
    summary = {
        "run": {
            "started_at": started_at,
            "spark_version": spark.version,
            "master": sc.master,
            "executors": executors,
            "default_parallelism": sc.defaultParallelism,
            "silver_rows": silver_rows,
            "silver_size_mb": round(silver_size, 2),
            "gold_size_mb": round(sum(t["size_mb"] for t in tables.values()), 2),
            "build_seconds": build_timer.seconds,
            "reconcile_seconds": rec_timer.seconds,
            "total_seconds": job_timer.seconds,
        },
        "tables": tables,
        "reconciliation": checks,
        "previews": previews,
    }
    with io.open(os.path.join(OUT_DIR, "gold_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    write_markdown(summary, os.path.join(OUT_DIR, "gold_report.md"))

    failed = [c["check"] for c in checks if not c["ok"]]
    log.info("Xong sau %.2fs. Đối chiếu: %d/%d đạt. Báo cáo: reports/gold/gold_report.md",
             job_timer.seconds, len(checks) - len(failed), len(checks))
    spark.stop()
    if failed:
        raise SystemExit("Đối chiếu số liệu SAI: " + "; ".join(failed))


if __name__ == "__main__":
    main()
