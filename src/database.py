import psycopg2
import time
import random

# //INFO: Database management module for distributed scraping
class DatabaseManager:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.conn = None
        self._init_db()

    def _get_conn(self):
        attempts = 0
        while attempts < 3:
            try:
                if not self.conn or self.conn.closed:
                    self.conn = psycopg2.connect(self.db_url, connect_timeout=15)
                    self.conn.autocommit = True
                return self.conn
            except Exception as e:
                attempts += 1
                time.sleep(2)
        raise Exception("# //WARN: DB Connection failed")

    def _init_db(self):
        try:
            with self._get_conn().cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS posts (
                        post_id TEXT PRIMARY KEY,
                        author TEXT,
                        post_time TEXT,
                        content TEXT,
                        source_url TEXT,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE IF NOT EXISTS visited_urls (
                        url TEXT PRIMARY KEY,
                        status TEXT DEFAULT 'pending',
                        last_visited TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
        except: pass

    def is_url_visited(self, url):
        try:
            with self._get_conn().cursor() as cur:
                cur.execute("SELECT status FROM visited_urls WHERE url = %s", (url,))
                res = cur.fetchone()
                return res is not None and res[0] == 'completed'
        except: return False

    def mark_url_processing(self, url):
        try:
            with self._get_conn().cursor() as cur:
                cur.execute("INSERT INTO visited_urls (url, status) VALUES (%s, 'processing') ON CONFLICT (url) DO UPDATE SET status = 'processing'", (url,))
        except: pass

    def mark_url_completed(self, url):
        try:
            with self._get_conn().cursor() as cur:
                cur.execute("UPDATE visited_urls SET status = 'completed', last_visited = CURRENT_TIMESTAMP WHERE url = %s", (url,))
        except: pass

    def mark_url_failed(self, url):
        try:
            with self._get_conn().cursor() as cur:
                cur.execute("UPDATE visited_urls SET status = 'failed' WHERE url = %s", (url,))
        except: pass

    def save_posts(self, posts):
        # //INFO: Simple loop for insertion - highly stable over patchy connections
        conn = self._get_conn()
        with conn.cursor() as cur:
            for p in posts:
                try:
                    cur.execute("""
                        INSERT INTO posts (post_id, author, post_time, content, source_url)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (post_id) DO NOTHING
                    """, (str(p['post_id']), str(p['author']), str(p['time']), str(p['content']), str(p.get('source_url', ''))))
                except Exception as e:
                    print(f"      # //NOTE: Failed to save post {p.get('post_id')}: {e}")
                    conn.rollback() # Ensure transaction is clean if autocommit is off
