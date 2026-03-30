import requests
import time
import socket
import threading
import subprocess
from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel
import nest_asyncio
import json
import os
import hashlib

# --- SPYDER FIX ---
nest_asyncio.apply()

# --- CONFIGURATION ---
MASTER_URL = "http://172.18.236.240:8000"
NODE_ID = socket.gethostname()
LEDGER_FILE = "ledger.json"

# --- LEDGER CORE LOGIC ---
def get_hash(data):
    """Generates a SHA-256 hash for a block."""
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

def add_to_ledger(task_command, result, worker_id):
    """Saves a new task execution to the local chain."""
    ledger = []
    if os.path.exists(LEDGER_FILE):
        try:
            with open(LEDGER_FILE, 'r') as f:
                ledger = json.load(f)
        except: ledger = []

    prev_hash = ledger[-1]['hash'] if ledger else "0" * 64
    
    new_block = {
        "index": len(ledger),
        "timestamp": time.time(),
        "worker": worker_id,
        "command": task_command,
        "result_summary": result[:50].strip() + "..." if len(result) > 50 else result,
        "prev_hash": prev_hash
    }
    new_block['hash'] = get_hash(new_block)
    
    ledger.append(new_block)
    with open(LEDGER_FILE, 'w') as f:
        json.dump(ledger, f, indent=4)
    return new_block

# --- THE MINI-SERVER ---
peer_app = FastAPI()

class TaskRequest(BaseModel):
    command: str

@peer_app.post("/execute")
async def execute_task(task: TaskRequest):
    print(f"🛠️ Executing task: {task.command}")
    try:
        # 1. Run Task
        output = subprocess.check_output(task.command, shell=True, stderr=subprocess.STDOUT).decode('utf-8')
        
        # 2. Record in Local Ledger
        new_block = add_to_ledger(task.command, output, NODE_ID)
        print(f"📝 Recorded Block #{new_block['index']}")
        
        # 3. GOSSIP: Tell other peers (Background)
        threading.Thread(target=broadcast_block, args=(new_block,)).start()
        
        return {"status": "success", "output": output, "block": new_block['index']}
    except subprocess.CalledProcessError as e:
        return {"status": "error", "output": e.output.decode('utf-8')}

@peer_app.post("/receive-block")
async def receive_block(block: dict):
    """Endpoint for other peers to sync their ledger with you."""
    ledger = []
    if os.path.exists(LEDGER_FILE):
        with open(LEDGER_FILE, 'r') as f:
            ledger = json.load(f)
    
    # Simple check to avoid duplicates
    if not any(b['hash'] == block['hash'] for b in ledger):
        ledger.append(block)
        with open(LEDGER_FILE, 'w') as f:
            json.dump(ledger, f, indent=4)
        print(f"📦 Network Sync: Received Block {block['index']} from {block['worker']}")
        return {"status": "synced"}
    return {"status": "already_exists"}

@peer_app.get("/ping")
async def ping():
    return {"status": "online", "from": NODE_ID}

# --- CLIENT LOGIC ---
def broadcast_block(block):
    """Sends a new block to all active peers."""
    try:
        response = requests.get(f"{MASTER_URL}/get-peers", timeout=5)
        if response.status_code == 200:
            peers = response.json()
            for peer in peers:
                if peer['node_id'] != NODE_ID:
                    try:
                        # Use the port reported by the Brain
                        url = f"http://{peer['ip']}:{peer['port']}/receive-block"
                        requests.post(url, json=block, timeout=2)
                    except: continue
    except: pass

def send_heartbeat(current_port):
    data = {"node_id": NODE_ID, "latency_to_master": 10.5, "bandwidth_mbps": 100.0, "port": current_port}
    try:
        requests.post(f"{MASTER_URL}/heartbeat", json=data, timeout=5)
        print(f" ✅ Heartbeat (Port {current_port})")
    except: print(" ❌ Brain unreachable")

def update_mesh():
    try:
        response = requests.get(f"{MASTER_URL}/get-peers", timeout=5)
        if response.status_code == 200:
            peers = response.json()
            for peer in peers:
                if peer['node_id'] == NODE_ID: continue
                start = time.time()
                try:
                    url = f"http://{peer['ip']}:{peer['port']}/ping"
                    requests.get(url, timeout=1)
                    latency = (time.time() - start) * 1000
                    edge_data = {"source_id": NODE_ID, "target_id": peer['node_id'], "latency_ms": latency, "bandwidth_mbps": 80.0}
                    requests.post(f"{MASTER_URL}/update-topology", json=edge_data, timeout=2)
                except: pass
    except: pass

# --- EXECUTION ---
if __name__ == "__main__":
    import asyncio

    def find_free_port(start_port):
        p = start_port
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(('localhost', p)) != 0: return p
                p += 1

    MY_PORT = find_free_port(8001)
    print(f"🚀 Starting Decentralized Worker: {NODE_ID} on Port: {MY_PORT}")

    def background_loop():
        while True:
            send_heartbeat(MY_PORT)
            update_mesh()
            time.sleep(15)

    threading.Thread(target=background_loop, daemon=True).start()

    config = uvicorn.Config(app=peer_app, host="0.0.0.0", port=MY_PORT, loop="asyncio", log_level="error")
    server = uvicorn.Server(config)
    
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(server.serve())
    else:
        server.run()