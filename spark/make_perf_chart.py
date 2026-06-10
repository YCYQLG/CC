"""A-3：读取 perf_results.json，绘制 Pandas / Spark(1 executor) / Spark(2 executor)
执行时间对比柱状图，输出到报告图片目录。"""
import json
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

# 中文字体（Windows 自带黑体）
for fp in [r"C:\Windows\Fonts\msyh.ttc", r"C:\Windows\Fonts\simhei.ttf"]:
    if os.path.exists(fp):
        font_manager.fontManager.addfont(fp)
        matplotlib.rcParams["font.family"] = font_manager.FontProperties(fname=fp).get_name()
        break
matplotlib.rcParams["axes.unicode_minus"] = False

with open(r"E:\cloud\CC\spark\perf_results.json", encoding="utf-8") as f:
    d = json.load(f)

labels = ["Pandas\n(单机)", "PySpark\n(1 executor)", "PySpark\n(2 executor)"]
keys = ["pandas", "spark_exec1", "spark_exec2"]
times = [d.get(k, 0) for k in keys]
colors = ["#4C72B0", "#DD8452", "#55A868"]

fig, ax = plt.subplots(figsize=(7, 4.5))
bars = ax.bar(labels, times, color=colors, width=0.55)
ax.set_ylabel("执行时间 (秒)")
ax.set_title("A-3 性能对比：按年代聚合查询（豆瓣电影 ~12万行）")
for b, t in zip(bars, times):
    ax.text(b.get_x() + b.get_width() / 2, t, f"{t:.2f}s",
            ha="center", va="bottom", fontsize=11)
# 加速比标注
if d.get("spark_exec1") and d.get("spark_exec2"):
    sp = d["spark_exec1"] / d["spark_exec2"]
    ax.text(0.03, 0.97, f"Spark 1→2 executor 加速比 = {sp:.2f}×\n(小数据集下并行反而更慢)",
            transform=ax.transAxes, ha="left", va="top", fontsize=10,
            bbox=dict(boxstyle="round", fc="#fff3cd", ec="#ffc107"))
ax.set_ylim(0, max(times) * 1.25)
plt.tight_layout()
out = r"E:\cloud\screenshots\a3_perf_chart.png"
plt.savefig(out, dpi=150)
print("saved:", out)
print("times:", dict(zip(keys, times)))
