import streamlit as st
import sqlite3
import pandas as pd
import time
import requests
import json
import os

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

def get_ledger():
    """Reads the decentralized ledger file."""
    if os.path.exists("ledger.json"):
        try:
            with open("ledger.json", "r") as f:
                return json.load(f)
        except:
            return []
    return []

nodes, edges = get_data()

# --- 3. SIDEBAR ---
with st.sidebar:
    st.header("📡 Active Fleet")
    if not nodes.empty:
        current_time = time.time()
        nodes['status'] = nodes['last_seen'].apply(
            lambda x: "🟢 Online" if (current_time - x) < 30 else "🔴 Offline"
        )
        st.dataframe(nodes[['node_id', 'status']], hide_index=True)
    
    st.markdown("---")
    spread = st.slider("Graph Spread", 1.0, 3.0, 1.5)
    auto_refresh = st.checkbox("Auto-refresh Graph", value=True)

# --- 4. TABS: TOPOLOGY vs LEDGER ---
tab1, tab2 = st.tabs(["🗺️ Network Topology", "📑 Decentralized Ledger"])

with tab1:
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

with tab2:
    st.subheader("📑 Global Event Ledger")
    ledger_data = get_ledger()
    
    if ledger_data:
        # Convert to DataFrame for a clean table
        df_ledger = pd.DataFrame(ledger_data)
        
        # Format the timestamp for readability
        df_ledger['timestamp'] = pd.to_datetime(df_ledger['timestamp'], unit='s').dt.strftime('%H:%M:%S')
        
        # Display the "Chain" metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Blocks", len(ledger_data))
        m2.metric("Last Worker", ledger_data[-1]['worker'])
        m3.metric("Chain Integrity", "☑ Verified" if len(ledger_data) > 0 else "N/A")

        # Show the ledger table
        st.dataframe(
            df_ledger[['index', 'timestamp', 'worker', 'command', 'result_summary', 'hash']], 
            use_container_width=True, 
            hide_index=True
        )
        
        # Detail View (expand any block to see hashes)
        with st.expander("🔍 Inspect Block Hashes"):
            st.json(ledger_data)
    else:
        st.info("No events recorded in the ledger yet.")

st.markdown("---")

# --- 5. TASK ORCHESTRATOR ---
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
                brain_url = f"http://127.0.0.1:8000/run-task/{target_node}"
                res = requests.post(brain_url, params={"command": command_to_run}, timeout=15)
                if res.status_code == 200:
                    result = res.json()
                    st.success(f"Execution Complete! New block added to ledger.")
                    st.code(result.get("output", "No output returned."))
                else:
                    st.error(f"Brain Error: {res.status_code}")
            except Exception as e:
                st.error(f"Connection Failed: {e}")

# --- 6. REFRESH LOGIC ---
if auto_refresh:
    time.sleep(3)
    st.rerun()