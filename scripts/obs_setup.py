"""创建 OBS 桶 group17（cn-north-4）并上传 Spark 待分析数据集 + wordcount 示例文本。
OBS 提供 S3 兼容 API，用 boto3 访问。"""
import os
import sys
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# 凭据从环境变量读取，避免硬编码入库：运行前 set OBS_AK / OBS_SK
AK = os.environ.get("OBS_AK", "<YOUR_AK>")
SK = os.environ.get("OBS_SK", "<YOUR_SK>")
REGION = "cn-north-4"
ENDPOINT = f"https://obs.{REGION}.myhuaweicloud.com"
BUCKET = "group17"

# botocore>=1.36 默认开启 flexible checksum，OBS 的 S3 兼容层会拒绝 -> 关闭
_cfg_kwargs = dict(signature_version="s3v4", s3={"addressing_style": "virtual"})
try:
    _cfg = Config(request_checksum_calculation="when_required",
                  response_checksum_validation="when_required", **_cfg_kwargs)
except TypeError:
    _cfg = Config(**_cfg_kwargs)

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=AK,
    aws_secret_access_key=SK,
    region_name=REGION,
    config=_cfg,
)


def ensure_bucket(name):
    try:
        s3.head_bucket(Bucket=name)
        print(f"[ok] bucket '{name}' already exists and is accessible")
        return name
    except ClientError as e:
        code = e.response["Error"].get("Code")
        if code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=name,
                             CreateBucketConfiguration={"LocationConstraint": REGION})
            print(f"[ok] created bucket '{name}'")
            return name
        if code in ("403", "Forbidden"):
            print(f"[warn] bucket '{name}' exists but owned by another account")
            raise
        raise


def main():
    bucket = ensure_bucket(BUCKET)

    # 1) 上传豆瓣电影数据集（单次 PUT，避免 OBS 多段上传的 sha256 校验问题）
    print("[..] uploading douban_movies.csv (~39MB) ...")
    with open(r"E:\cloud\douban_movies.csv", "rb") as f:
        s3.put_object(Bucket=bucket, Key="data/douban_movies.csv",
                      Body=f.read(), ContentType="text/csv")
    print("[ok] uploaded -> s3a://%s/data/douban_movies.csv" % bucket)

    # 2) wordcount 示例文本
    sample = (
        "hello spark hello cloud\n"
        "spark on kubernetes is powerful\n"
        "data data data analysis with spark\n"
        "cloud computing course design group17\n"
        "spark spark spark hello hello world\n"
    ) * 50
    s3.put_object(Bucket=bucket, Key="data/sample.txt", Body=sample.encode("utf-8"))
    print("[ok] uploaded -> s3a://%s/data/sample.txt" % bucket)

    # 列出确认
    print("\n[bucket contents]")
    resp = s3.list_objects_v2(Bucket=bucket)
    for o in resp.get("Contents", []):
        print(f"  {o['Key']:30s} {o['Size']:>12,} bytes")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", type(e).__name__, e)
        sys.exit(1)
