import sqlite3
import ijson

JSON_FILE = r"D:\users_data\users_data.json"
DB_FILE   = "users4.db"

BATCH_SIZE = 5_000_000   # 50 lakh
PRINT_AFTER_INSERTS = 10_000_000   # 1 crore

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute("PRAGMA journal_mode=WAL;")
cur.execute("PRAGMA synchronous=OFF;")
cur.execute("PRAGMA temp_store=MEMORY;")

# --- Table ---
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    mobile TEXT PRIMARY KEY,
    name TEXT,
    fname TEXT,
    address TEXT,
    alt TEXT,
    circle TEXT,
    id TEXT,
    email TEXT
)
""")

# --- Resume state ---
cur.execute("""
CREATE TABLE IF NOT EXISTS import_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_index INTEGER NOT NULL
)
""")
cur.execute("INSERT OR IGNORE INTO import_state (id, last_index) VALUES (1, 0)")
conn.commit()

(last_index,) = cur.execute(
    "SELECT last_index FROM import_state WHERE id=1"
).fetchone()

print(f"▶ Resuming after index {last_index:,}")

processed = 0
inserted = 0
since_commit = 0

try:
    with open(JSON_FILE, "rb") as f:
        for idx, obj in enumerate(ijson.items(f, "item"), start=1):

            if idx <= last_index:
                continue

            processed += 1
            since_commit += 1

            mobile = obj.get("mobile")
            if not mobile:
                continue

            cur.execute("""
                INSERT OR IGNORE INTO users
                (mobile, name, fname, address, alt, circle, id, email)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                mobile,
                obj.get("name"),
                obj.get("fname"),
                obj.get("address"),
                obj.get("alt"),
                obj.get("circle"),
                obj.get("id"),
                obj.get("email") or None
            ))

            if cur.rowcount == 1:
                inserted += 1
                if inserted % PRINT_AFTER_INSERTS == 0:
                    print(f"🎉 Inserted {inserted:,} records successfully")

            if since_commit >= BATCH_SIZE:
                conn.commit()
                cur.execute(
                    "UPDATE import_state SET last_index=? WHERE id=1",
                    (idx,)
                )
                conn.commit()
                print(f"✅ Checkpoint saved at index={idx:,}")
                since_commit = 0

    conn.commit()
    cur.execute(
        "UPDATE import_state SET last_index=? WHERE id=1",
        (idx,)
    )
    conn.commit()

    print("\n✅ Conversion completed")
    print(f"Processed this run : {processed:,}")
    print(f"Inserted (real)    : {inserted:,}")

except KeyboardInterrupt:
    conn.commit()
    cur.execute(
        "UPDATE import_state SET last_index=? WHERE id=1",
        (idx,)
    )
    conn.commit()
    print(f"\n⏸ Interrupted. Saved checkpoint at index {idx:,}")

finally:
    conn.close()
