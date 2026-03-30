import sqlite3

def init_db():
    conn = sqlite3.connect('compute_network.db')
    cursor = conn.cursor()

    # Drop existing tables to clear the "Generated Column" error
    cursor.execute("DROP TABLE IF EXISTS nodes")
    cursor.execute("DROP TABLE IF EXISTS topology")

    # Create Nodes Table
    cursor.execute('''
        CREATE TABLE nodes (
            node_id TEXT PRIMARY KEY,
            ip_address TEXT,
            last_seen REAL,
            is_available INTEGER,
            port INTEGER
        )
    ''')

    # Create Topology Table (Weight is a normal REAL now, handled by Python)
    cursor.execute('''
        CREATE TABLE topology (
            source_id TEXT,
            target_id TEXT,
            weight REAL,
            PRIMARY KEY (source_id, target_id)
        )
    ''')

    conn.commit()
    conn.close()
    print("✅ Database re-initialized! No more generated column errors.")

if __name__ == "__main__":
    init_db()