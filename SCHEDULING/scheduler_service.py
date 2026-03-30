from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
import time
import requests
import uuid

app = FastAPI(title="Core-Tex Simple Scheduler")

DB_PATH = r"D:\PROGRAMMING\CoreTex_WT\MAIN\compute_network.db"
HEARTBEAT_TTL = 120


class WorkerMetrics(BaseModel):
    node_id: str
    cpu_percent: float
    memory_percent: float
    active_tasks: int = 0
    queued_tasks: int = 0


class TaskRequest(BaseModel):
    command: str


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_tables():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS worker_metrics (
        node_id TEXT PRIMARY KEY,
        cpu_percent REAL DEFAULT 0,
        memory_percent REAL DEFAULT 0,
        active_tasks INTEGER DEFAULT 0,
        queued_tasks INTEGER DEFAULT 0,
        updated_at REAL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        task_id TEXT PRIMARY KEY,
        command TEXT,
        assigned_node TEXT,
        score REAL,
        status TEXT,
        output TEXT,
        error TEXT,
        created_at REAL,
        finished_at REAL
    )
    """)

    conn.commit()
    conn.close()


@app.on_event("startup")
def startup():
    init_tables()


@app.get("/")
def root():
    return {"service": "scheduler", "status": "ok"}


@app.post("/worker-metrics")
def update_worker_metrics(data: WorkerMetrics):
    conn = get_db()
    conn.execute("""
    INSERT INTO worker_metrics (node_id, cpu_percent, memory_percent, active_tasks, queued_tasks, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(node_id) DO UPDATE SET
        cpu_percent = excluded.cpu_percent,
        memory_percent = excluded.memory_percent,
        active_tasks = excluded.active_tasks,
        queued_tasks = excluded.queued_tasks,
        updated_at = excluded.updated_at
    """, (
        data.node_id,
        data.cpu_percent,
        data.memory_percent,
        data.active_tasks,
        data.queued_tasks,
        time.time()
    ))
    conn.commit()
    conn.close()
    return {"status": "ok"}


def get_active_nodes():
    conn = get_db()
    cutoff = time.time() - HEARTBEAT_TTL

    rows = conn.execute("""
    SELECT
        n.node_id,
        n.ip_address,
        n.port,
        COALESCE(m.cpu_percent, 100) AS cpu_percent,
        COALESCE(m.memory_percent, 100) AS memory_percent,
        COALESCE(m.active_tasks, 0) AS active_tasks,
        COALESCE(m.queued_tasks, 0) AS queued_tasks,
        COALESCE(t.weight, 1000) AS network_weight
    FROM nodes n
    LEFT JOIN worker_metrics m ON n.node_id = m.node_id
    LEFT JOIN topology t ON t.source_id = 'MASTER' AND t.target_id = n.node_id
    WHERE n.last_seen > ?
      AND n.is_available = 1
      AND n.port IS NOT NULL
    """, (cutoff,)).fetchall()

    conn.close()
    return rows


def calculate_score(node):
    return (
        0.5 * node["cpu_percent"]
        + 0.3 * node["memory_percent"]
        + 10 * node["active_tasks"]
        + 5 * node["queued_tasks"]
        + 0.2 * node["network_weight"]
    )


def choose_best_node():
    nodes = get_active_nodes()
    if not nodes:
        raise HTTPException(status_code=404, detail="No active workers available")

    best_node = None
    best_score = float("inf")

    for node in nodes:
        score = calculate_score(node)
        print(f"[scheduler] {node['node_id']} -> score={round(score, 2)}")

        if score < best_score:
            best_score = score
            best_node = node

    return best_node, best_score


def increment_active_task(node_id, delta):
    conn = get_db()
    conn.execute("""
    INSERT INTO worker_metrics (node_id, active_tasks, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(node_id) DO UPDATE SET
        active_tasks = CASE
            WHEN worker_metrics.active_tasks + ? < 0 THEN 0
            ELSE worker_metrics.active_tasks + ?
        END,
        updated_at = excluded.updated_at
    """, (node_id, max(delta, 0), time.time(), delta, delta))
    conn.commit()
    conn.close()


@app.get("/nodes")
def list_nodes():
    nodes = get_active_nodes()
    result = []

    for node in nodes:
        score = calculate_score(node)
        result.append({
            "node_id": node["node_id"],
            "ip_address": node["ip_address"],
            "port": node["port"],
            "cpu_percent": node["cpu_percent"],
            "memory_percent": node["memory_percent"],
            "active_tasks": node["active_tasks"],
            "queued_tasks": node["queued_tasks"],
            "network_weight": node["network_weight"],
            "score": round(score, 2)
        })

    result.sort(key=lambda x: x["score"])
    return result


@app.post("/submit-task")
def submit_task(task: TaskRequest):
    task_id = str(uuid.uuid4())
    command = task.command

    try:
        node, score = choose_best_node()
    except HTTPException as e:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": e.detail
        }
    except Exception as e:
        return {
            "task_id": task_id,
            "status": "failed",
            "error": f"Scheduler crashed while selecting node: {str(e)}"
        }

    worker_url = f"http://{node['ip_address']}:{node['port']}/execute"

    conn = get_db()
    conn.execute("""
    INSERT INTO jobs (task_id, command, assigned_node, score, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        command,
        node["node_id"],
        score,
        "running",
        time.time()
    ))
    conn.commit()
    conn.close()

    increment_active_task(node["node_id"], 1)

    try:
        response = requests.post(
            worker_url,
            json={"command": command},
            timeout=20
        )
        response.raise_for_status()
        result = response.json()

        status = result.get("status", "unknown")
        output = result.get("output", "")
        error = None if status == "success" else output

        conn = get_db()
        conn.execute("""
        UPDATE jobs
        SET status = ?, output = ?, error = ?, finished_at = ?
        WHERE task_id = ?
        """, (
            status,
            output if status == "success" else None,
            error,
            time.time(),
            task_id
        ))
        conn.commit()
        conn.close()

        return {
            "task_id": task_id,
            "status": status,
            "assigned_node": node["node_id"],
            "score": round(score, 2),
            "result": result
        }

    except Exception as e:
        conn = get_db()
        conn.execute("""
        UPDATE jobs
        SET status = ?, error = ?, finished_at = ?
        WHERE task_id = ?
        """, (
            "failed",
            str(e),
            time.time(),
            task_id
        ))
        conn.commit()
        conn.close()

        return {
            "task_id": task_id,
            "status": "failed",
            "assigned_node": node["node_id"],
            "score": round(score, 2),
            "error": str(e)
        }

    finally:
        increment_active_task(node["node_id"], -1)

@app.get("/jobs")
def list_jobs():
    conn = get_db()
    rows = conn.execute("""
    SELECT task_id, command, assigned_node, score, status, output, error, created_at, finished_at
    FROM jobs
    ORDER BY created_at DESC
    LIMIT 50
    """).fetchall()
    conn.close()

    return [dict(row) for row in rows]