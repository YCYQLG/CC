"""A-3 性能对比（Spark 端）：计时"加载 douban + 按年代聚合"整个查询。
通过 EXECUTOR_INSTANCES 环境变量标注本次用了几个 executor。"""
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType


def configure_obs(spark):
    hc = spark.sparkContext._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint", os.environ.get("OBS_ENDPOINT", "obs.cn-north-4.myhuaweicloud.com"))
    hc.set("fs.s3a.access.key", os.environ["OBS_AK"])
    hc.set("fs.s3a.secret.key", os.environ["OBS_SK"])
    hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set("fs.s3a.connection.ssl.enabled", "true")
    hc.set("fs.s3a.path.style.access", "false")


def main():
    spark = SparkSession.builder.appName("PerfQuery").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    configure_obs(spark)
    bucket = os.environ.get("OBS_BUCKET", "group17")
    n_exec = os.environ.get("EXECUTOR_INSTANCES", "?")

    t0 = time.time()
    df = (spark.read.option("header", True).option("multiLine", True)
          .option("quote", '"').option("escape", '"')
          .csv(f"s3a://{bucket}/data/douban_movies.csv")
          .withColumn("year", F.col("year").cast(DoubleType()).cast(IntegerType()))
          .withColumn("rating_score", F.col("rating_score").cast(DoubleType())))
    result = (df.filter(F.col("year").between(1920, 2025) & (F.col("rating_score") > 0))
              .groupBy(((F.col("year") / 10).cast("int") * 10).alias("decade"))
              .agg(F.count("*").alias("cnt"), F.round(F.avg("rating_score"), 2).alias("avg_r"))
              .orderBy("decade")
              .collect())
    elapsed = time.time() - t0

    print("==================== PERF (SPARK) ====================")
    for r in result:
        print(f"  decade={r['decade']}  cnt={r['cnt']}  avg_r={r['avg_r']}")
    print(f"PERF_RESULT engine=spark executor_instances={n_exec} elapsed_seconds={elapsed:.3f}")
    print("=====================================================")
    spark.stop()


if __name__ == "__main__":
    main()
