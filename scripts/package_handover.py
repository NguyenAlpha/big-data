"""
Đóng gói dữ liệu gold thành 1 file zip để gửi cho nhóm Analytics.

Chạy trên MÁY THẬT, chỉ cần Python 3 (thư viện chuẩn), SAU KHI đã chạy
02_clean.py và 03_gold.py:
    python scripts/package_handover.py

Đầu ra: dist/olist_gold_handover_<ngày>.zip gồm
    HANDOVER.md      - tài liệu bàn giao (đọc file này trước)
    SCHEMA.md        - tên cột & kiểu dữ liệu của 7 bảng (sinh từ lần chạy thật)
    gold_report.md   - báo cáo lần chạy: số dòng, 11 kiểm tra đối chiếu, xem trước KPI
    MANIFEST.json    - số dòng, dung lượng, commit mã nguồn, thời điểm đóng gói
    gold/<bảng>/...  - dữ liệu Parquet

Script từ chối đóng gói nếu có kiểm tra đối chiếu số liệu bị SAI - không gửi dữ
liệu sai cho nhóm sau.
"""

import io
import json
import os
import subprocess
import sys
import zipfile
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(ROOT, "data", "gold")
SUMMARY = os.path.join(ROOT, "reports", "gold", "gold_summary.json")
REPORT = os.path.join(ROOT, "reports", "gold", "gold_report.md")
HANDOVER = os.path.join(ROOT, "HANDOVER.md")
DIST = os.path.join(ROOT, "dist")


def git_commit():
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True, check=True)
        dirty = subprocess.run(["git", "status", "--porcelain", "jobs"], cwd=ROOT,
                               capture_output=True, text=True).stdout.strip()
        return out.stdout.strip() + (" (code trong jobs/ có thay đổi chưa commit)" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "không xác định"


def schema_markdown(summary):
    L = ["# Schema các bảng gold\n",
         "Sinh tự động từ lần chạy `03_gold.py` lúc {}.\n".format(summary["run"]["started_at"])]
    for name, t in summary["tables"].items():
        L.append("## `{}` ({:,} dòng; 1 dòng = {})\n".format(name, t["rows"], t["grain"]))
        L.append("| Cột | Kiểu |\n|---|---|")
        for col, dtype in t["schema"]:
            L.append("| `{}` | {} |".format(col, dtype))
        L.append("")
    return "\n".join(L) + "\n"


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    for path in (GOLD_DIR, SUMMARY, REPORT, HANDOVER):
        if not os.path.exists(path):
            sys.exit("Thiếu {} - hãy chạy 02_clean.py và 03_gold.py trước.".format(path))

    with io.open(SUMMARY, encoding="utf-8") as f:
        summary = json.load(f)
    failed = [c["check"] for c in summary["reconciliation"] if not c["ok"]]
    if failed:
        sys.exit("KHÔNG đóng gói: đối chiếu số liệu bị SAI ở: " + "; ".join(failed))

    revenue = next(c["left"] for c in summary["reconciliation"]
                   if c["check"].startswith("Doanh thu các tháng"))
    manifest = {
        "dataset": "Olist Brazilian E-Commerce - tầng gold",
        "packaged_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "gold_built_at": summary["run"]["started_at"],
        "source_code": "https://github.com/NguyenAlpha/big-data",
        "source_commit": git_commit(),
        "reconciliation": "{}/{} đạt".format(len(summary["reconciliation"]),
                                            len(summary["reconciliation"])),
        "valid_revenue_brl": revenue,
        "tables": {name: {"rows": t["rows"], "grain": t["grain"], "kind": t["kind"],
                          "size_mb": t["size_mb"], "parquet_files": t["files"]}
                   for name, t in summary["tables"].items()},
    }

    os.makedirs(DIST, exist_ok=True)
    zip_path = os.path.join(DIST, "olist_gold_handover_{}.zip".format(
        datetime.now().strftime("%Y%m%d")))
    files = 0
    # File tài liệu được nén (DEFLATED); file Parquet vốn đã nén sẵn nên chỉ lưu (STORED)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(HANDOVER, "HANDOVER.md")
        z.write(REPORT, "gold_report.md")
        z.writestr("SCHEMA.md", schema_markdown(summary))
        z.writestr("MANIFEST.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        for root, _, names in os.walk(GOLD_DIR):
            for n in sorted(names):
                # Bỏ file phụ của Spark: _SUCCESS (đánh dấu ghi xong), .crc (checksum)
                if n.startswith("_") or n.endswith(".crc"):
                    continue
                full = os.path.join(root, n)
                arc = os.path.join("gold", os.path.relpath(full, GOLD_DIR)).replace(os.sep, "/")
                z.write(full, arc, compress_type=zipfile.ZIP_STORED)
                files += 1

    size = os.path.getsize(zip_path) / 1024 / 1024
    print("Đã đóng gói {} file Parquet của {} bảng -> {} ({:.1f} MB)".format(
        files, len(summary["tables"]), os.path.relpath(zip_path, ROOT), size))
    print("Đối chiếu số liệu: {} | Doanh thu hợp lệ: {} BRL | Commit: {}".format(
        manifest["reconciliation"], revenue, manifest["source_commit"]))


if __name__ == "__main__":
    main()
