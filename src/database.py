import psycopg2
import time
import random

#INFO: Database management module for distributed scraping
class DatabaseManager:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.conn = None
        self._init_db()

    def _get_conn(self, force_reconnect=False):
        if force_reconnect and self.conn:
            try: self.conn.close()
            except: pass
            self.conn = None

        attempts = 0
        while attempts < 5:
            try:
                if not self.conn or self.conn.closed:
                    self.conn = psycopg2.connect(
                        self.db_url, 
                        connect_timeout=20,
                        options="-c statement_timeout=30000"
                    )
                    self.conn.autocommit = True
                
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return self.conn
            except:
                attempts += 1
                if self.conn:
                    try: self.conn.close()
                    except: pass
                    self.conn = None
                time.sleep(random.uniform(2, 5))
        
        raise Exception("#WARN: Persistent DB connection failure")

    def _init_db(self):
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
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
                        url_type TEXT DEFAULT 'topic',
                        last_visited TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                try:
                    cur.execute("ALTER TABLE visited_urls ADD COLUMN IF NOT EXISTS url_type TEXT DEFAULT 'topic'")
                except: pass
        except Exception as e:
            print(f"#WARN: Init DB failed: {e}")

    def _execute_with_retry(self, query, params=None, is_select=False, fetch_all=False):
        for attempt in range(4):
            try:
                conn = self._get_conn(force_reconnect=(attempt > 0))
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if is_select:
                        if fetch_all: return cur.fetchall()
                        return cur.fetchone()
                    return True
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if attempt == 3: raise
                if self.conn:
                    try: self.conn.close()
                    except: pass
                    self.conn = None
                time.sleep(2 ** attempt)
            except Exception as e:
                if "duplicate key" not in str(e).lower():
                    print(f"      #NOTE: DB operation error: {str(e)[:100]}")
                return None

    def is_url_visited(self, url):
        # Check if URL is already done OR currently being handled by someone else
        res = self._execute_with_retry("SELECT status FROM visited_urls WHERE url = %s", (url,), is_select=True)
        if not res: return False
        status = res[0]
        return status in ['completed', 'processing']

    def mark_url_processing(self, url):
        self._execute_with_retry("""
            UPDATE visited_urls SET status = 'processing', last_visited = CURRENT_TIMESTAMP WHERE url = %s
        """, (url,))

    def mark_url_completed(self, url):
        self._execute_with_retry("UPDATE visited_urls SET status = 'completed', last_visited = CURRENT_TIMESTAMP WHERE url = %s", (url,))

    def mark_url_failed(self, url):
        self._execute_with_retry("UPDATE visited_urls SET status = 'failed', last_visited = CURRENT_TIMESTAMP WHERE url = %s", (url,))

    def add_urls(self, urls, url_type='topic'):
        if not urls: return
        for url in urls:
            self._execute_with_retry("""
                INSERT INTO visited_urls (url, status, url_type) 
                VALUES (%s, 'pending', %s) 
                ON CONFLICT (url) DO NOTHING
            """, (url, url_type))

    def get_pending_urls(self, url_type=None, limit=50):
        if url_type:
            query = "SELECT url, url_type FROM visited_urls WHERE status = 'pending' AND url_type = %s ORDER BY last_visited ASC LIMIT %s"
            params = (url_type, limit)
        else:
            query = "SELECT url, url_type FROM visited_urls WHERE status = 'pending' ORDER BY last_visited ASC LIMIT %s"
            params = (limit,)
            
        rows = self._execute_with_retry(query, params, is_select=True, fetch_all=True)
        return rows if rows else []

    def save_posts(self, posts):
        if not posts: return
        for p in posts:
            query = """
                INSERT INTO posts (post_id, author, post_time, content, source_url)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (post_id) DO NOTHING
            """
            params = (str(p['post_id']), str(p['author']), str(p['time']), str(p['content']), str(p.get('source_url', '')))
            self._execute_with_retry(query, params)
