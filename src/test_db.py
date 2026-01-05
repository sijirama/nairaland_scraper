import psycopg2
import config

def test_conn():
    url = config.DATABASE_URL
    print("Connecting...")
    conn = psycopg2.connect(url, connect_timeout=15)
    print("Connected. Executing SELECT 1...")
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(f"Result: {cur.fetchone()}")
    conn.close()
    print("Done.")

if __name__ == "__main__":
    test_conn()
