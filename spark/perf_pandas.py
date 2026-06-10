"""A-3 性能对比（单机 Pandas 端）：在本地用 Pandas 完成与 Spark 相同的
"加载 douban + 按年代聚合平均评分"查询，记录耗时。"""
import sys
import time
import json
import pandas as pd

CSV = sys.argv[1] if len(sys.argv) > 1 else r"E:\cloud\douban_movies.csv"

t0 = time.time()
df = pd.read_csv(CSV)
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["rating_score"] = pd.to_numeric(df["rating_score"], errors="coerce")
sub = df[(df["year"] >= 1920) & (df["year"] <= 2025) & (df["rating_score"] > 0)].copy()
sub["decade"] = (sub["year"] // 10 * 10).astype(int)
result = (sub.groupby("decade")
          .agg(cnt=("rating_score", "size"), avg_r=("rating_score", "mean"))
          .reset_index().sort_values("decade"))
elapsed = time.time() - t0

print("==================== PERF (PANDAS) ====================")
print(result.to_string(index=False))
print(f"PERF_RESULT engine=pandas executor_instances=0 elapsed_seconds={elapsed:.3f}")
print("======================================================")

# 记录到 json，供绘图脚本使用
try:
    with open(r"E:\cloud\CC\spark\perf_results.json", "r", encoding="utf-8") as f:
        data = json.load(f)
except Exception:
    data = {}
data["pandas"] = round(elapsed, 3)
with open(r"E:\cloud\CC\spark\perf_results.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
