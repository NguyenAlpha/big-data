"""
BƯỚC 6 - CHẠY THỰC NGHIỆM
=========================

Script này chạy trên MÁY THẬT (không phải trong container), chỉ dùng thư viện
chuẩn của Python. Nó lần lượt gọi `docker compose exec ... spark-submit` để chạy
pipeline Processing (02_clean.py + 03_gold.py) với nhiều cấu hình khác nhau, đo
thời gian, và gom kết quả vào reports/experiments/results.csv.

Hai thí nghiệm:
  E1 - Quy mô dữ liệu: x1, x5, x10, x20 (120 MB -> ~2,4 GB), cụm đầy đủ 4 core.
       Câu hỏi: thời gian tăng thế nào khi dữ liệu tăng? Spark đổi cách join ra sao?
  E2 - Tài nguyên: cố định dữ liệu x5, chạy với 1 core / 2 core (1 worker) /
       4 core (2 worker).
       Câu hỏi: thêm core/worker thì nhanh hơn bao nhiêu (speedup)?

Mỗi cấu hình chạy REPEATS lần (lấy trung bình) vì lần chạy có thể dao động do
cache của hệ điều hành, JVM khởi động, v.v.

Cách chạy (từ thư mục gốc project, cụm Spark đang bật):
    python experiments/run_experiments.py              # chạy toàn bộ (~1,5-2 giờ)
    python experiments/run_experiments.py --quick      # thử nhanh: x1, x5; 1 lần/cấu hình
    python experiments/run_experiments.py --report     # chỉ tạo lại báo cáo từ results.csv

Đầu ra:
    reports/experiments/results.csv       - 1 dòng / 1 lần chạy (số liệu thô)
    reports/experiments/results.md        - bảng tổng hợp (trung bình theo cấu hình)
    reports/experiments/plans/            - execution plan của fact_order_items ở mỗi quy mô
    reports/experiments/runs/<run_id>/    - log & báo cáo đầy đủ từng lần chạy (không đưa lên git)
"""

import argparse
import csv
import io
import json
import os
import shutil
import subprocess
import sys
import time
from collections import OrderedDict
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "reports", "experiments")
RESULTS_CSV = os.path.join(OUT, "results.csv")

SCALE_FACTORS = [1, 5, 10, 20]
E2_FACTOR = 5
# Cấu hình tài nguyên: tên -> (tổng số core, số core mỗi executor)
#   --executor-cores 2 buộc mỗi executor lấy trọn 2 core của 1 worker,
#   nên "2 core" = đúng 1 worker, "4 core" = 2 worker.
CORE_CONFIGS = OrderedDict([
    ("1 core", (1, 1)),
    ("2 core (1 worker)", (2, 2)),
    ("4 core (2 worker)", (4, 2)),
])
FULL = "4 core (2 worker)"
REPEATS = 2

FIELDS = ["run_id", "experiment", "scale", "cores_config", "total_cores", "repeat",
          "started_at", "raw_mb", "rows_in", "silver_mb", "gold_mb",
          "clean_s", "clean_validate_s", "gold_build_s", "gold_reconcile_s",
          "processing_s", "wall_clean_s", "wall_gold_s", "rows_per_s", "mb_per_s",
          "broadcast_joins", "sortmerge_joins", "reconciliation_passed",
          "reconciliation_total", "valid_revenue"]


def log(msg):
    print("[{}] {}".format(datetime.now().strftime("%H:%M:%S"), msg), flush=True)


def spark_submit(script, env, cores=None, args=(), out_file=None):
    """Chạy 1 job trên cụm qua `docker compose exec`. Trả về số giây (wall clock)."""
    cmd = ["docker", "compose", "exec", "-T"]
    for k, v in env.items():
        cmd += ["-e", "{}={}".format(k, v)]
    cmd += ["spark-master", "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077"]
    if cores:
        total, per_executor = cores
        cmd += ["--total-executor-cores", str(total), "--executor-cores", str(per_executor)]
    cmd += ["/opt/jobs/" + script] + [str(a) for a in args]

    start = time.perf_counter()
    with io.open(out_file or os.devnull, "w", encoding="utf-8") as f:
        proc = subprocess.run(cmd, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT)
    seconds = round(time.perf_counter() - start, 2)
    if proc.returncode != 0:
        raise RuntimeError("{} lỗi (exit {}), xem {}".format(script, proc.returncode, out_file))
    return seconds


def ensure_scaled_data(factor):
    """Sinh dữ liệu x<factor> nếu chưa có (x1 dùng luôn dataset gốc)."""
    if factor == 1:
        return "/data/raw"
    marker = os.path.join(ROOT, "data", "scale", "x{}".format(factor),
                          "product_category_name_translation.csv", "_SUCCESS")
    if not os.path.exists(marker):
        log("Sinh dữ liệu x{} ...".format(factor))
        os.makedirs(os.path.join(OUT, "runs"), exist_ok=True)
        secs = spark_submit("scale_data.py", {}, args=[factor],
                            out_file=os.path.join(OUT, "runs", "scale_x{}.out".format(factor)))
        log("  xong sau {}s".format(secs))
    return "/data/lake/scale/x{}".format(factor)


def load_json(path):
    with io.open(path, encoding="utf-8") as f:
        return json.load(f)


def run_once(experiment, factor, cores_name, repeat):
    total_cores, _ = CORE_CONFIGS[cores_name]
    run_id = "x{}_c{}_r{}".format(factor, total_cores, repeat)
    raw_dir = ensure_scaled_data(factor)
    run_dir = os.path.join(OUT, "runs", run_id)
    lake_host = os.path.join(ROOT, "data", "experiments", run_id)
    os.makedirs(run_dir, exist_ok=True)
    env = {
        "OLIST_RAW_DIR": raw_dir,
        "OLIST_LAKE_DIR": "/data/lake/experiments/" + run_id,
        "OLIST_REPORT_DIR": "/opt/reports/experiments/runs/" + run_id,
    }
    cores = CORE_CONFIGS[cores_name]
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("{} | x{} | {} | lần {} ...".format(experiment, factor, cores_name, repeat))

    wall_clean = spark_submit("02_clean.py", env, cores,
                              out_file=os.path.join(run_dir, "spark_clean.out"))
    wall_gold = spark_submit("03_gold.py", env, cores,
                             out_file=os.path.join(run_dir, "spark_gold.out"))

    clean = load_json(os.path.join(run_dir, "cleaning", "cleaning_summary.json"))
    gold = load_json(os.path.join(run_dir, "gold", "gold_summary.json"))
    c, g = clean["run"], gold["run"]
    processing = round(c["clean_seconds"] + g["build_seconds"], 2)
    joins = [t["joins"] for t in gold["tables"].values()]
    revenue = next(x["left"] for x in gold["reconciliation"]
                   if x["check"].startswith("Doanh thu các tháng"))
    row = {
        "run_id": run_id, "experiment": experiment, "scale": factor,
        "cores_config": cores_name, "total_cores": total_cores, "repeat": repeat,
        "started_at": started, "raw_mb": c["raw_size_mb"], "rows_in": c["rows_in"],
        "silver_mb": c["silver_size_mb"], "gold_mb": g["gold_size_mb"],
        "clean_s": c["clean_seconds"], "clean_validate_s": c["validate_seconds"],
        "gold_build_s": g["build_seconds"], "gold_reconcile_s": g["reconcile_seconds"],
        "processing_s": processing, "wall_clean_s": wall_clean, "wall_gold_s": wall_gold,
        "rows_per_s": round(c["rows_in"] / processing),
        "mb_per_s": round(c["raw_size_mb"] / processing, 2),
        "broadcast_joins": sum(j["BroadcastHashJoin"] for j in joins),
        "sortmerge_joins": sum(j["SortMergeJoin"] for j in joins),
        "reconciliation_passed": sum(1 for x in gold["reconciliation"] if x["ok"]),
        "reconciliation_total": len(gold["reconciliation"]),
        "valid_revenue": revenue,
    }
    append_result(row)

    # Giữ lại execution plan của bảng fact ở mỗi quy mô làm minh chứng
    plan_src = os.path.join(run_dir, "gold", "plans", "fact_order_items.txt")
    plan_dir = os.path.join(OUT, "plans")
    os.makedirs(plan_dir, exist_ok=True)
    shutil.copy(plan_src, os.path.join(plan_dir, "x{}_fact_order_items.txt".format(factor)))
    # Xoá dữ liệu silver/gold của lần chạy (có thể tới vài GB) - chỉ cần số đo
    shutil.rmtree(lake_host, ignore_errors=True)

    log("  xử lý {}s (làm sạch {}s + gold {}s) | {:,} dòng/s | join: {} broadcast, {} sort-merge"
        " | đối chiếu {}/{}".format(processing, c["clean_seconds"], g["build_seconds"],
                                    row["rows_per_s"], row["broadcast_joins"],
                                    row["sortmerge_joins"], row["reconciliation_passed"],
                                    row["reconciliation_total"]))
    return row


def append_result(row):
    os.makedirs(OUT, exist_ok=True)
    new = not os.path.exists(RESULTS_CSV)
    with io.open(RESULTS_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)


def done_runs():
    if not os.path.exists(RESULTS_CSV):
        return set()
    with io.open(RESULTS_CSV, encoding="utf-8") as f:
        return {r["run_id"] for r in csv.DictReader(f)}


# =============================================================================
# Báo cáo tổng hợp
# =============================================================================
def mean(values):
    return sum(values) / len(values)


def summarize(rows, key_fields):
    """Gom các lần chạy cùng cấu hình, lấy trung bình."""
    groups = OrderedDict()
    for r in rows:
        groups.setdefault(tuple(r[k] for k in key_fields), []).append(r)
    out = []
    for key, rs in groups.items():
        s = dict(zip(key_fields, key))
        s["runs"] = len(rs)
        for f in ("raw_mb", "rows_in", "clean_s", "gold_build_s", "processing_s",
                  "wall_clean_s", "wall_gold_s", "rows_per_s", "mb_per_s",
                  "broadcast_joins", "sortmerge_joins", "silver_mb", "gold_mb"):
            s[f] = mean([float(r[f]) for r in rs])
        s["min_processing_s"] = min(float(r["processing_s"]) for r in rs)
        s["max_processing_s"] = max(float(r["processing_s"]) for r in rs)
        s["reconciliation"] = "{}/{}".format(min(int(r["reconciliation_passed"]) for r in rs),
                                             rs[0]["reconciliation_total"])
        s["valid_revenue"] = rs[0]["valid_revenue"]
        out.append(s)
    return out


def write_report():
    with io.open(RESULTS_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["scale"] = int(r["scale"])
        r["total_cores"] = int(r["total_cores"])

    e1 = sorted([s for s in summarize([r for r in rows if r["cores_config"] == FULL],
                                      ["scale"])], key=lambda s: s["scale"])
    e2 = sorted(summarize([r for r in rows if r["scale"] == E2_FACTOR],
                          ["total_cores", "cores_config"]), key=lambda s: s["total_cores"])

    L = ["# Kết quả thực nghiệm - pipeline Processing (02_clean + 03_gold)\n",
         "> Sinh tự động bởi `experiments/run_experiments.py` từ `results.csv` "
         "({} lần chạy). Giải thích: `docs/05-thuc-nghiem.md`.\n".format(len(rows)),
         "*Thời gian xử lý* = làm sạch + tạo gold, **không** gồm khởi động Spark và các bước "
         "kiểm tra lại / đối chiếu. Mỗi cấu hình chạy nhiều lần, bảng ghi giá trị trung bình.\n"]

    if e1:
        base = e1[0]
        L.append("## E1 - Quy mô dữ liệu (4 core, 2 worker)\n")
        L.append("| Quy mô | CSV vào (MB) | Số dòng vào | Làm sạch (s) | Gold (s) | "
                 "Xử lý (s) | Min-max (s) | Dữ liệu tăng | Thời gian tăng | Dòng/giây | MB/giây | "
                 "Broadcast join | Sort-merge join | Đối chiếu | Doanh thu hợp lệ |")
        L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|")
        for s in e1:
            L.append("| x{} | {:,.0f} | {:,.0f} | {:.1f} | {:.1f} | **{:.1f}** | {:.0f}-{:.0f} | "
                     "{:.1f}x | {:.1f}x | {:,.0f} | {:.2f} | {:.0f} | {:.0f} | {} | {} |".format(
                         s["scale"], s["raw_mb"], s["rows_in"], s["clean_s"], s["gold_build_s"],
                         s["processing_s"], s["min_processing_s"], s["max_processing_s"],
                         s["raw_mb"] / base["raw_mb"], s["processing_s"] / base["processing_s"],
                         s["rows_per_s"], s["mb_per_s"], s["broadcast_joins"],
                         s["sortmerge_joins"], s["reconciliation"], s["valid_revenue"]))

    if e2:
        one = e2[0]
        L.append("\n## E2 - Số core / worker (dữ liệu x{})\n".format(E2_FACTOR))
        L.append("| Cấu hình | Số lần chạy | Làm sạch (s) | Gold (s) | Xử lý (s) | Min-max (s) | "
                 "Speedup so với 1 core | Hiệu suất (speedup / số core) | Dòng/giây |")
        L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for s in e2:
            speedup = one["processing_s"] / s["processing_s"]
            L.append("| {} | {} | {:.1f} | {:.1f} | **{:.1f}** | {:.0f}-{:.0f} | {:.2f}x | {:.0f}% | "
                     "{:,.0f} |".format(s["cores_config"], s["runs"], s["clean_s"],
                                        s["gold_build_s"], s["processing_s"],
                                        s["min_processing_s"], s["max_processing_s"], speedup,
                                        100 * speedup / s["total_cores"], s["rows_per_s"]))

    L.append("\n## Toàn bộ các lần chạy\n")
    cols = ["run_id", "started_at", "raw_mb", "rows_in", "clean_s", "gold_build_s",
            "processing_s", "wall_clean_s", "wall_gold_s", "sortmerge_joins",
            "reconciliation_passed"]
    L.append("| " + " | ".join(cols) + " |")
    L.append("|" + "---|" * len(cols))
    for r in rows:
        L.append("| " + " | ".join(str(r[c]) for c in cols) + " |")

    with io.open(os.path.join(OUT, "results.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    log("Đã ghi reports/experiments/results.md")


# =============================================================================
# MAIN
# =============================================================================
def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # in được tiếng Việt trên console Windows
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true", help="x1, x5; 1 lần mỗi cấu hình")
    parser.add_argument("--report", action="store_true", help="chỉ tạo lại results.md")
    opts = parser.parse_args()
    if opts.report:
        write_report()
        return

    factors = [1, 5] if opts.quick else SCALE_FACTORS
    repeats = 1 if opts.quick else REPEATS

    # Danh sách lần chạy: E1 (mọi quy mô, cụm đầy đủ) + E2 (x5, mọi cấu hình core)
    plan = [("E1", f, FULL, r) for r in range(1, repeats + 1) for f in factors]
    plan += [("E2", E2_FACTOR, name, r) for r in range(1, repeats + 1)
             for name in CORE_CONFIGS if name != FULL]

    skip = done_runs()  # chạy tiếp từ chỗ dừng nếu lần trước bị ngắt
    for experiment, factor, cores_name, repeat in plan:
        run_id = "x{}_c{}_r{}".format(factor, CORE_CONFIGS[cores_name][0], repeat)
        if run_id in skip:
            log("Bỏ qua {} (đã có kết quả)".format(run_id))
            continue
        run_once(experiment, factor, cores_name, repeat)
    write_report()


if __name__ == "__main__":
    main()
