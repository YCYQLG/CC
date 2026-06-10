import os
import socket
import logging
import math

import redis
from flask import Flask, jsonify, request

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backend")

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
APP_ENV = os.getenv("APP_ENV", "development")


def get_redis():
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
        socket_connect_timeout=2, decode_responses=True,
    )


@app.before_request
def _log_req():
    # 任务1验收：后端日志需显示收到请求
    log.info("收到请求 %s %s from %s", request.method, request.path, request.remote_addr)


@app.get("/api/ping")
def ping():
    # ELB 健康检查 / livenessProbe / HPA 压测入口
    return jsonify(status="ok")


@app.get("/api/info")
def info():
    return jsonify(hostname=socket.gethostname(), env=APP_ENV,
                   redis_host=REDIS_HOST, redis_port=REDIS_PORT)


@app.get("/api/visits")
def visits():
    # 演示前端->后端->Redis 三层通信
    try:
        r = get_redis()
        n = r.incr("visits")
        return jsonify(visits=n, served_by=socket.gethostname())
    except Exception as e:
        log.exception("redis error")
        return jsonify(error=str(e)), 500


@app.get("/api/compute")
def compute():
    # 备用 CPU 压力端点（若 /api/ping 压不出 CPU，HPA 演示改用这个）
    s = 0.0
    for i in range(1, 300000):
        s += math.sqrt(i)
    return jsonify(result=s)


if __name__ == "__main__":
    log.info("backend starting on :5000 (redis=%s:%s env=%s)",
             REDIS_HOST, REDIS_PORT, APP_ENV)
    app.run(host="0.0.0.0", port=5000)
