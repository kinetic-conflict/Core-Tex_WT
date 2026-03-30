import requests
import time
import socket
import threading
import subprocess
from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel
import nest_asyncio

# --- SPYDER FIX ---
nest_asyncio.apply()

# --- CONFIGURATION ---
# IMPORTANT: Ensure this IP matches your Master's actual IP address
MASTER_URL = "http://172.18.236.240:8000"
NODE_ID = socket.gethostname()

# --- THE MINI-SERVER (Listening for Peers) ---
peer_app = FastAPI()

class TaskRequest(BaseModel):
    command: str

@peer_app.post("/execute")
async def execute_task(task: TaskRequest):
    print(f"🛠️ Executing task: {task.command}")
    try:
        result = subprocess.check_output(task.command, shell=True, stderr=subprocess.STDOUT)
        return {
            "status": "success",
            "node": NODE_ID,
            "output": result.decode('utf-8')
        }
    except subprocess.CalledProcessError as e:
        return {
            "status": "error",
            "node": NODE_ID,
            "output": e.output.decode('utf-8')
        }
    
@peer_app.get("/ping")
async def ping():
    return {"status": "online", "from": NODE_ID}

# --- THE CLIENT LOGIC ---
def send_heartbeat(current_port):
    data = {
        "node_id": NODE_ID,
        "latency_to_master": 10.5,
        "bandwidth_mbps": 100.0,
        "port": current_port  # Sending our dynamic port to the brain
    }
    try:
        r = requests.post(f"{MASTER_URL}/heartbeat", json=data, timeout=5)
        if r.status_code == 200:
            print(f" ✅ Heartbeat (Port {current_port}) sent to Brain")
        else:
            print(f" ⚠️ Brain rejected heartbeat: {r.status_code}")
    except Exception as e:
        print(f" ❌ Brain unreachable: {e}")

def update_mesh():
    try:
        response = requests.get(f"{MASTER_URL}/get-peers", timeout=5)
        
        # Check if response is actually JSON
        if response.status_code != 200:
            print(f" ⚠️ Could not get peers: {response.status_code}")
            return

        peers = response.json()

        for peer in peers:
            if peer['node_id'] == NODE_ID:
                continue 
            
            start = time.time()
            try:
                # Note: This logic assumes peers are on 8001. 
                # For a full demo, you'd fetch the peer's port from the Brain too.
                peer_url = f"http://{peer['ip']}:8001/ping"
                requests.get(peer_url, timeout=1)
                latency = (time.time() - start) * 1000
                
                edge_data = {
                    "source_id": NODE_ID,
                    "target_id": peer['node_id'],
                    "latency_ms": latency,
                    "bandwidth_mbps": 80.0 
                }
                requests.post(f"{MASTER_URL}/update-topology", json=edge_data, timeout=2)
                print(f" 🔗 Linked to {peer['node_id']} ({round(latency, 1)}ms)")
            except:
                pass # Silent fail if peer is offline
    except Exception as e:
        print(f" ❌ Mesh update failed (likely Brain response error): {e}")

# --- EXECUTION ---

if __name__ == "__main__":
    import socket
    import asyncio

    # 1. Define the function FIRST
    def find_free_port(start_port):
        p = start_port
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(('localhost', p)) != 0:
                    return p
                p += 1

    # 2. Now call it
    MY_PORT = find_free_port(8001)
    print(f"🚀 Starting Worker: {NODE_ID} on Port: {MY_PORT}")

    # 3. Background Loop
    def background_loop():
        while True:
            send_heartbeat(MY_PORT)
            update_mesh()
            time.sleep(15)

    threading.Thread(target=background_loop, daemon=True).start()

    # 4. Spyder-safe Server Start
    config = uvicorn.Config(app=peer_app, host="0.0.0.0", port=MY_PORT, loop="asyncio")
    server = uvicorn.Server(config)
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(server.serve())
        else:
            server.run()
    except Exception as e:
        print(f"Server error: {e}")
    