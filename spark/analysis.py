"""第二部分方向A：豆瓣电影数据分析。
A-1 数据清洗 + A-2 Spark SQL 统计分析。
数据来自 OBS：s3a://group17/data/douban_movies.csv
OBS 凭据通过环境变量（K8s Secret）注入。

数据特点：rating_score==0 表示"评分人数不足、豆瓣未出分"，等价于缺失值。
注：Spark SQL 列别名用英文（中文标识符需反引号），中文在报告中标注。"""
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType


def banner(title):
    print("\n" + "=" * 70)
    print("==  " + title)
    print("=" * 70)


def configure_obs(spark):
    hc = spark.sparkContext._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint", os.environ.get("OBS_ENDPOINT", "obs.cn-north-4.myhuaweicloud.com"))
    hc.set("fs.s3a.access.key", os.environ["OBS_AK"])
    hc.set("fs.s3a.secret.key", os.environ["OBS_SK"])
    hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set("fs.s3a.connection.ssl.enabled", "true")
    hc.set("fs.s3a.path.style.access", "false")


def main():
    spark = (SparkSession.builder.appName("DoubanAnalysis")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    configure_obs(spark)
    bucket = os.environ.get("OBS_BUCKET", "group17")
    path = f"s3a://{bucket}/data/douban_movies.csv"

    # ---------------- A-1 数据加载 ----------------
    banner("A-1  load data + schema + first 5 rows")
    raw = (spark.read
           .option("header", True)
           .option("multiLine", True)        # summary 字段含换行，必须开启
           .option("quote", '"')
           .option("escape", '"')
           .csv(path))
    df = (raw
          .withColumn("year", F.col("year").cast(DoubleType()).cast(IntegerType()))
          .withColumn("rating_score", F.col("rating_score").cast(DoubleType()))
          .withColumn("rating_count", F.col("rating_count").cast(LongType()))
          .withColumn("collect_count", F.col("collect_count").cast(LongType())))
    df.printSchema()
    df.show(5, truncate=30)
    total = df.count()
    print(f"raw rows = {total}")

    # ---------------- A-1 缺失值比例（rating_score==0 视为缺失） ----------------
    banner("A-1  missing-value ratio per column (rating_score==0 treated as missing)")
    miss_exprs = []
    for c, t in df.dtypes:
        if c == "rating_score":
            cond = F.col(c).isNull() | (F.col(c) == 0)
        elif t == "string":
            cond = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        else:
            cond = F.col(c).isNull()
        miss_exprs.append(F.round(F.sum(cond.cast("int")) / F.lit(total), 4).alias(c))
    miss_row = df.select(miss_exprs).collect()[0].asDict()
    for c, v in miss_row.items():
        print(f"  {c:16s} missing_ratio = {v:.2%}")

    # ---------------- A-1 清洗：两种不同策略 ----------------
    banner("A-1  clean: strategy1 year=dropna / strategy2 rating_score=fillna(mean)")
    mean_rated = df.filter(F.col("rating_score") > 0).select(F.avg("rating_score")).first()[0]
    mean_rated = round(mean_rated, 2) if mean_rated else 0.0
    print(f"  mean rating of rated movies (rating>0) = {mean_rated}")
    cleaned = (df
               .filter(F.col("year").isNotNull() & F.col("year").between(1920, 2025))
               .withColumn("rating_filled",
                           F.when((F.col("rating_score").isNull()) | (F.col("rating_score") == 0),
                                  F.lit(mean_rated)).otherwise(F.col("rating_score")))
               .fillna({"directors": "unknown", "genres": "unknown", "countries": "unknown", "summary": ""}))
    cleaned = cleaned.cache()
    after = cleaned.count()
    print(f"  rows before = {total}  ->  after year-dropna = {after}  (dropped {total - after})")

    rated = cleaned.filter(F.col("rating_score") > 0).cache()
    print(f"  rated movies (rating>0) = {rated.count()}")

    # ---------------- A-1 基本统计 ----------------
    banner("A-1  numeric columns describe (mean/std/min/max)")
    cleaned.select("year", "rating_score", "rating_filled", "rating_count", "collect_count").describe().show()

    rated.createOrReplaceTempView("movies")
    cleaned.createOrReplaceTempView("all_movies")
    genre_df = (rated.withColumn("genre", F.explode(F.split(F.col("genres"), "/")))
                .withColumn("genre", F.trim(F.col("genre"))).filter(F.col("genre") != ""))
    genre_df.createOrReplaceTempView("movies_genre")

    # ---------------- A-2 Q1：GROUP BY 聚合 ----------------
    banner("A-2  Q1 [GROUP BY] count + avg rating per genre, Top15 (rated movies)")
    spark.sql("""
        SELECT genre, COUNT(*) AS cnt, ROUND(AVG(rating_score),2) AS avg_rating
        FROM movies_genre
        GROUP BY genre ORDER BY cnt DESC LIMIT 15
    """).show(15, truncate=False)

    # ---------------- A-2 Q2：ORDER BY Top-N ----------------
    banner("A-2  Q2 [ORDER BY Top-N] Top10 high-rated movies with >=50k votes")
    spark.sql("""
        SELECT title, year, rating_score, rating_count
        FROM movies WHERE rating_count >= 50000
        ORDER BY rating_score DESC, rating_count DESC LIMIT 10
    """).show(10, truncate=False)

    # ---------------- A-2 Q3：时间维度趋势 ----------------
    banner("A-2  Q3 [time trend] movies count + avg rating per decade")
    spark.sql("""
        SELECT (year - year % 10) AS decade, COUNT(*) AS cnt, ROUND(AVG(rating_score),2) AS avg_rating
        FROM movies GROUP BY (year - year % 10) ORDER BY decade
    """).show(20, truncate=False)

    # ---------------- A-2 Q4：窗口函数 ----------------
    banner("A-2  Q4 [window] top-rated movie per genre (ROW_NUMBER partition)")
    spark.sql("""
        SELECT genre, title, rating_score, rating_count FROM (
            SELECT genre, title, rating_score, rating_count,
                   ROW_NUMBER() OVER (PARTITION BY genre ORDER BY rating_score DESC, rating_count DESC) AS rnk
            FROM movies_genre WHERE rating_count >= 10000
        ) t WHERE rnk = 1 ORDER BY rating_score DESC LIMIT 15
    """).show(15, truncate=False)

    # ---------------- A-2 Q5：JOIN ----------------
    banner("A-2  Q5 [JOIN] classics with >=100k votes above their decade average")
    spark.sql("""
        WITH decade_avg AS (
            SELECT (year - year % 10) AS decade, AVG(rating_score) AS d_avg
            FROM movies GROUP BY (year - year % 10)
        )
        SELECT m.title, m.year, m.rating_score, ROUND(d.d_avg,2) AS decade_avg
        FROM movies m JOIN decade_avg d ON (m.year - m.year % 10) = d.decade
        WHERE m.rating_count >= 100000 AND m.rating_score > d.d_avg
        ORDER BY m.rating_score DESC LIMIT 10
    """).show(10, truncate=False)

    # ---------------- A-3 计时 ----------------
    banner("A-3  PERF timing: decade aggregation query")
    n_exec = os.environ.get("EXECUTOR_INSTANCES", "?")
    t0 = time.time()
    _ = spark.sql("""
        SELECT (year - year % 10) AS decade, COUNT(*) AS cnt, AVG(rating_score) AS avg_r
        FROM movies GROUP BY (year - year % 10) ORDER BY decade
    """).collect()
    elapsed = time.time() - t0
    print(f"PERF_RESULT executor_instances={n_exec} elapsed_seconds={elapsed:.3f}")

    # 可选：保持 driver+executor 存活一段时间，方便截图（HOLD_SECONDS 环境变量控制）
    hold = int(os.environ.get("HOLD_SECONDS", "0"))
    if hold > 0:
        print(f"[hold] keeping driver+executors alive {hold}s for screenshot ...", flush=True)
        time.sleep(hold)
    spark.stop()


if __name__ == "__main__":
    main()
