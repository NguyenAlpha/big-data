"""
Vẽ biểu đồ kết quả thực nghiệm (PNG) để đưa vào báo cáo.

Cần: pip install matplotlib
Chạy: python experiments/plot_results.py
Đầu ra: reports/experiments/fig_e1_scale.png, reports/experiments/fig_e2_cores.png
"""

import csv
import io
import os

import matplotlib

matplotlib.use("Agg")  # vẽ ra file, không mở cửa sổ
import matplotlib.pyplot as plt  # noqa: E402

from run_experiments import E2_FACTOR, FULL, OUT, RESULTS_CSV, summarize  # noqa: E402

# Màu theo vai trò (bảng màu đã kiểm tra an toàn cho người mù màu)
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e6e5e1"
CLEAN = "#2a78d6"   # series 1 - làm sạch
GOLD = "#eb6834"    # series 2 - tổng hợp gold
IDEAL = "#9a9893"   # đường tham chiếu (lý tưởng)

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": TEXT_2, "text.color": TEXT,
    "xtick.color": TEXT_2, "ytick.color": TEXT_2,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.axisbelow": True, "axes.titleweight": "bold", "axes.titlesize": 11,
    "axes.titlelocation": "left", "legend.frameon": False,
})


def load():
    with io.open(RESULTS_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["scale"] = int(r["scale"])
        r["total_cores"] = int(r["total_cores"])
    return rows


def stacked_time(ax, labels, clean, gold, title, legend_loc="upper left"):
    """Cột chồng: thời gian làm sạch + gold, ghi tổng trên đầu cột."""
    x = range(len(labels))
    # Viền màu nền 2px tạo khe hở giữa 2 đoạn cột
    ax.bar(x, clean, width=0.55, color=CLEAN, edgecolor=SURFACE, linewidth=2,
           label="Làm sạch (02_clean)")
    ax.bar(x, gold, width=0.55, bottom=clean, color=GOLD, edgecolor=SURFACE, linewidth=2,
           label="Tổng hợp gold (03_gold)")
    for i, (c, g) in enumerate(zip(clean, gold)):
        ax.text(i, c + g, "{:.0f}s".format(c + g), ha="center", va="bottom",
                color=TEXT, fontsize=9, fontweight="bold")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel("Thời gian xử lý (giây)")
    ax.set_title(title)
    # Đặt chú thích ở góc trống (không đè lên cột cao nhất)
    ax.legend(loc=legend_loc, fontsize=9)
    ax.margins(y=0.12)


def plot_e1(rows):
    e1 = sorted(summarize([r for r in rows if r["cores_config"] == FULL], ["scale"]),
                key=lambda s: s["scale"])
    if not e1:
        return None
    labels = ["x{}\n{:,.0f} MB".format(s["scale"], s["raw_mb"]) for s in e1]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
    stacked_time(a1, labels, [s["clean_s"] for s in e1], [s["gold_build_s"] for s in e1],
                 "Thời gian xử lý theo quy mô dữ liệu")

    # Thông lượng: số dòng xử lý mỗi giây (1 series -> không cần chú thích)
    x = range(len(e1))
    thr = [s["rows_per_s"] / 1000 for s in e1]
    a2.bar(x, thr, width=0.55, color=CLEAN)
    for i, v in enumerate(thr):
        a2.text(i, v, "{:.1f}k".format(v), ha="center", va="bottom", color=TEXT, fontsize=9)
    a2.set_xticks(list(x))
    a2.set_xticklabels(labels)
    a2.set_ylabel("Nghìn dòng / giây")
    a2.set_title("Thông lượng (dòng đầu vào / giây xử lý)")
    a2.margins(y=0.12)

    fig.text(0.01, 0.01, "Cụm 2 worker x 2 core. Mỗi cột là trung bình {} lần chạy.".format(
        int(min(s["runs"] for s in e1))), color=TEXT_2, fontsize=8)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    path = os.path.join(OUT, "fig_e1_scale.png")
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def plot_e2(rows):
    e2 = sorted(summarize([r for r in rows if r["scale"] == E2_FACTOR],
                          ["total_cores", "cores_config"]), key=lambda s: s["total_cores"])
    if len(e2) < 2:
        return None
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
    stacked_time(a1, [s["cores_config"].replace(" (", "\n(") for s in e2],
                 [s["clean_s"] for s in e2], [s["gold_build_s"] for s in e2],
                 "Thời gian xử lý theo số core (dữ liệu x{})".format(E2_FACTOR),
                 legend_loc="upper right")

    cores = [s["total_cores"] for s in e2]
    base = e2[0]["processing_s"] * e2[0]["total_cores"]
    speedup = [base / s["processing_s"] for s in e2]
    a2.plot(cores, cores, color=IDEAL, linewidth=1.5, linestyle="--", label="Lý tưởng (tuyến tính)")
    a2.plot(cores, speedup, color=CLEAN, linewidth=2, marker="o", markersize=8,
            markeredgecolor=SURFACE, markeredgewidth=2, label="Thực tế")
    for c, s in zip(cores, speedup):
        a2.annotate("{:.2f}x".format(s), (c, s), textcoords="offset points", xytext=(0, -16),
                    ha="center", color=TEXT, fontsize=9, fontweight="bold")
    a2.set_xticks(cores)
    a2.set_xlabel("Số core")
    a2.set_ylabel("Speedup so với 1 core")
    a2.set_title("Speedup khi thêm core / worker")
    a2.set_ylim(0, max(cores) + 0.5)
    a2.legend(loc="upper left", fontsize=9)

    fig.text(0.01, 0.01, "Mỗi điểm là trung bình {} lần chạy. Cả cụm chạy trên 1 máy vật lý "
             "(12 core) nên các worker dùng chung ổ đĩa.".format(int(min(s["runs"] for s in e2))),
             color=TEXT_2, fontsize=8)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    path = os.path.join(OUT, "fig_e2_cores.png")
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


if __name__ == "__main__":
    rows = load()
    for p in (plot_e1(rows), plot_e2(rows)):
        if p:
            print("Đã vẽ", p)
