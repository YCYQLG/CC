"""A-0 入门示例：WordCount，读取 OBS 上的 sample.txt（s3a 接口），
统计词频并打印 Top 10。OBS 凭据通过环境变量注入（K8s Secret），不写死在代码里。"""
import os
from pyspark.sql import SparkSession


def configure_obs(spark):
    """配置 s3a 访问华为云 OBS。"""
    hc = spark.sparkContext._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint", os.environ.get("OBS_ENDPOINT", "obs.cn-north-4.myhuaweicloud.com"))
    hc.set("fs.s3a.access.key", os.environ["OBS_AK"])
    hc.set("fs.s3a.secret.key", os.environ["OBS_SK"])
    hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set("fs.s3a.connection.ssl.enabled", "true")
    hc.set("fs.s3a.path.style.access", "false")


def main():
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    configure_obs(spark)
    bucket = os.environ.get("OBS_BUCKET", "group17")

    lines = spark.sparkContext.textFile(f"s3a://{bucket}/data/sample.txt")
    word_counts = (
        lines.flatMap(lambda line: line.split())
             .map(lambda word: (word, 1))
             .reduceByKey(lambda a, b: a + b)
             .sortBy(lambda x: x[1], ascending=False)
    )
    print("==================== WORDCOUNT RESULT ====================")
    print("Top 10 words:", word_counts.take(10))
    print("=========================================================")
    spark.stop()


if __name__ == "__main__":
    main()
