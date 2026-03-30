import streamlit as st
import sqlite3
import pandas as pd
import time
import requests

# --- 1. PAGE SETUP ---
st.set_page_config(page_title="Core-Tex Dashboard", layout="wide")
st.title("🌐 Core-Tex Dashboard")

# --- 2. DATA FETCHING ---
def get_data():
    conn = sqlite3.connect('compute_network.db', timeout=10, check_same_thread=False)
    try:
        nodes_df = pd.read_sql_query("SELECT node_id, ip_address, last_seen FROM nodes", conn)
        edges_df = pd.read_sql_query("SELECT source_id, target_id, weight FROM topology", conn)
        return nodes_df, edges_df
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()

nodes, edges = get_data()

# --- 3. SIDEBAR: FLEET STATUS ---
with st.sidebar:
    st.header("📡 Active Fleet")
    if not nodes.empty:
        current_time = time.time()
        nodes['status'] = nodes['last_seen'].apply(
            lambda x: "🟢 Online" if (current_time - x) < 30 else "🔴 Offline"
        )
        st.dataframe(nodes[['node_id', 'status']], hide_index=True)
    else:
        st.info("No nodes connected.")
    
    st.markdown("---")
    spread = st.slider("Graph Spread", 1.0, 3.0, 1.5)
    # 💡 ADDED: Auto-refresh toggle so it doesn't interrupt you while typing commands
    auto_refresh = st.checkbox("Auto-refresh Graph", value=True)

# --- 4. MAIN LAYOUT: NETWORK VISUALIZER ---
col1, col2 = st.columns([3, 1])

with col1:
    st.subheader("Live Network Topology")
    if not edges.empty:
        dot = "graph Mesh {\n"
        dot += '  layout=neato;\n  overlap=false;\n  splines=true;\n' 
        dot += '  node [style=filled, fontname="Helvetica", shape=circle, width=0.8, color="#4CAF50", fontcolor=white];\n'
        dot += '  edge [fontname="Helvetica", fontsize=10, color="gray"];\n'
        dot += '  "MASTER" [shape=doublecircle, fillcolor="#FF5722", fontcolor=white];\n'
        
        for _, row in edges.iterrows():
            src, tgt, w = row['source_id'], row['target_id'], row['weight']
            dot += f'  "{src}" -- "{tgt}" [label="{round(w, 1)}", len={spread}];\n'
            
        dot += "}\n"
        st.graphviz_chart(dot, use_container_width=True)
    else:
        st.info("⏳ Waiting for nodes to report topology...")

with col2:
    st.subheader("Routing Weights")
    st.dataframe(edges, hide_index=True)

st.markdown("---")

# --- 5. TASK ORCHESTRATOR (Moved Up) ---
st.subheader("🚀 Task Orchestrator")
t_col1, t_col2 = st.columns([1, 2])

with t_col1:
    target_node = st.selectbox("Select Target Node", nodes['node_id'].tolist() if not nodes.empty else ["None"])
    command_to_run = st.text_input("Command", "hostname")
    run_btn = st.button("Run Remote Task")

with t_col2:
    if run_btn and target_node != "None":
        with st.spinner(f"Requesting {target_node}..."):
            try:
                # 💡 NOTE: Use 127.0.0.1 if running Dashboard & Brain on same PC
                brain_url = f"http://127.0.0.1:8000/run-task/{target_node}"
                res = requests.post(brain_url, params={"command": command_to_run}, timeout=15)
                
                if res.status_code == 200:
                    result = res.json()
                    st.success(f"Execution Complete on {target_node}")
                    st.code(result.get("output", "No output returned."))
                else:
                    st.error(f"Brain Error: {res.status_code}")
            except Exception as e:
                st.error(f"Connection Failed: {e}")

# --- 6. REFRESH LOGIC (Must be at the very bottom) ---
if auto_refresh:
    time.sleep(3)
    st.rerun()