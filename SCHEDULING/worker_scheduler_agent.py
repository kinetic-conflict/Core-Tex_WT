import psutil
import requests
import time
import socket

SCHEDULER_URL = "http://127.0.0.1:8002"
NODE_ID = socket.gethostname()
INTERVAL = 5


def collect_metrics():
    return {
        "node_id": NODE_ID,
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory_percent": psutil.virtual_memory().percent,
        "active_tasks": 0,
        "queued_tasks": 0
    }


def send_metrics():
    payload = collect_metrics()
    try:
        res = requests.post(f"{SCHEDULER_URL}/worker-metrics", json=payload, timeout=5)
        print(
            f"[agent] sent metrics | status={res.status_code} | "
            f"cpu={payload['cpu_percent']} | mem={payload['memory_percent']}"
        )
    except Exception as e:
        print(f"[agent] failed to send metrics: {e}")


if __name__ == "__main__":
    print(f"[agent] node_id = {NODE_ID}")
    print(f"[agent] scheduler = {SCHEDULER_URL}")

    while True:
        send_metrics()
        time.sleep(INTERVAL)