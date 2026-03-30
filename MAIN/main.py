from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import sqlite3
import time
import networkx as nx
import requests

app = FastAPI()

# --- 1. DATA MODELS ---
class Heartbeat(BaseModel):
    node_id: str
    latency_to_master: float
    bandwidth_mbps: float
    port: int  # <--- CRITICAL: Worker sends this now, so we must accept it

class TopologyUpdate(BaseModel):
    source_id: str
    target_id: str
    latency_ms: float
    bandwidth_mbps: float

# --- 2. DATABASE UTILS ---
def get_db():
    conn = sqlite3.connect('compute_network.db', timeout=5)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def calculate_weight(latency, bandwidth):
    return latency + (1000 / max(bandwidth, 1))

# --- 3. ENDPOINTS ---

@app.post("/heartbeat")
async def heartbeat(data: Heartbeat, request: Request):
    client_ip = request.client.host
    conn = get_db()
    cursor = conn.cursor()
    try:
        # Save node info including the dynamic port
        # Ensure you ran database.py to add the 'port' column!
        cursor.execute('''
            INSERT INTO nodes (node_id, ip_address, last_seen, is_available, port) 
            VALUES (?, ?, ?, 1, ?) 
            ON CONFLICT(node_id) DO UPDATE SET 
                last_seen=excluded.last_seen,
                ip_address=excluded.ip_address,
                port=excluded.port
        ''', (data.node_id, client_ip, time.time(), data.port))
        
        # Log the connection to the Master in the topology table
        weight = calculate_weight(data.latency_to_master, data.bandwidth_mbps)
        cursor.execute('''
            INSERT INTO topology (source_id, target_id, weight)
            VALUES ('MASTER', ?, ?)
            ON CONFLICT(source_id, target_id) DO UPDATE SET weight=excluded.weight
        ''', (data.node_id, weight))
        
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok"}

@app.get("/get-peers")
async def get_peers():
    conn = get_db()
    cursor = conn.cursor()
    # Fetch port so workers know which port to use when pinging peers
    cursor.execute("SELECT node_id, ip_address, port FROM nodes WHERE last_seen > ?", (time.time() - 120,))
    peers = cursor.fetchall()
    conn.close()
    return [{"node_id": p[0], "ip": p[1], "port": p[2]} for p in peers]

@app.post("/run-task/{target_node_id}")
async def run_remote_task(target_node_id: str, command: str):
    conn = get_db()
    cursor = conn.cursor()
    # Fetch both IP AND Port to find the worker's API
    cursor.execute("SELECT ip_address, port FROM nodes WHERE node_id = ?", (target_node_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Node not found")

    target_ip, target_port = row[0], row[1]
    worker_url = f"http://{target_ip}:{target_port}/execute"

    try:
        response = requests.post(worker_url, json={"command": command}, timeout=10)
        return response.json()
    except Exception as e:
        return {"status": "failed", "error": f"Relay failed: {str(e)}"}

@app.post("/update-topology")
async def update_topology(data: TopologyUpdate):
    conn = get_db()
    cursor = conn