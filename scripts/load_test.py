"""任务6 HPA 压测脚本：多线程持续请求后端 CPU 密集端点 /api/compute，
把后端 CPU 利用率压到 60% 以上触发 HPA 扩容。
用法： python load_test.py <ELB_IP> [并发数] [持续秒数]
例：   python load_test.py 1.94.238.112 120 180
"""
import sys
import time
import threading
import urllib.request

ELB_IP = sys.argv[1] if len(sys.argv) > 1 else "1.94.238.112"
CONCURRENCY = int(sys.argv[2]) if len(sys.argv) > 2 else 120
DURATION = int(sys.argv[3]) if len(sys.argv) > 3 else 180

URL = f"http://{ELB_IP}/api/compute"
stop_at = time.time() + DURATION
counter = {"ok": 0, "err": 0}
lock = threading.Lock()


def worker():
    local_ok = local_err = 0
    while time.time() < stop_at:
        try:
            with urllib.request.urlopen(URL, timeout=10) as r:
                r.read()
            local_ok += 1
        except Exception:
            local_err += 1
    with lock:
        counter["ok"] += local_ok
        counter["err"] += local_err


def main():
    print(f"[load] target={URL} concurrency={CONCURRENCY} duration={DURATION}s")
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(CONCURRENCY)]
    t0 = time.time()
    for t in threads:
        t.start()
    # 进度打印
    while time.time() < stop_at:
        time.sleep(5)
        with lock:
            done = counter["ok"] + counter["err"]
        print(f"  t={int(time.time()-t0):3d}s  requests={done}")
    for t in threads:
        t.join(timeout=15)
    print(f"[done] ok={counter['ok']} err={counter['err']} in {int(time.time()-t0)}s")


if __name__ == "__main__":
    main()
