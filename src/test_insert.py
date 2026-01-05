import psycopg2
import config

def test_insert():
    url = config.DATABASE_URL
    print("Connecting...")
    conn = psycopg2.connect(url, connect_timeout=15)
    conn.autocommit = True
    print("Connected. Inserting dummy post...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO posts (post_id, author, post_time, content, source_url)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (post_id) DO UPDATE SET content = EXCLUDED.content
        """, ("test_id_123", "test_author", "now", "hello world", "http://test.com"))
        print("Insert successful.")
    conn.close()
    print("Done.")

if __name__ == "__main__":
    test_insert()
