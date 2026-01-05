import time
import random
import os
from collections import deque
from typing import List, Dict

from browser import BrowserManager, safe_goto
from parser import extract_topic_links, extract_pagination_links, parse_topic_content, get_url_type
import config
from database import DatabaseManager

class NairalandCrawler:
    def __init__(self):
        self.topic_queue = deque()
        self.processed_count = 0
        self.cf_failures = 0
        
        #INFO: Initialize shared database manager
        self.db = DatabaseManager(config.DATABASE_URL)
        
        #INFO: Random startup offset to prevent race conditions in distributed mode
        time.sleep(random.uniform(0, 5))

    def is_cloudflare_page(self, html_content: str) -> bool:
        cf_indicators = ["Just a moment...", "Checking your browser", "challenges.cloudflare.com"]
        return any(indicator in html_content for indicator in cf_indicators)

    def handle_cf_block(self, url: str):
        self.cf_failures += 1
        backoff = min(config.CF_BACKOFF_BASE * (2 ** (self.cf_failures - 1)), 600)
        print(f"\n    #WARN: Cloudflare block detection. Backing off {int(backoff)}s...")
        time.sleep(backoff)
        if self.cf_failures >= 5:
            print("#WARN: Critical block level. Cooling down 10 mins.")
            time.sleep(600)
            self.cf_failures = 0

    def process_url(self, page, url: str, url_type: str):
        #INFO: Core processing logic for any Nairaland URL
        if self.db.is_url_visited(url):
            return True

        self.db.mark_url_processing(url)
        
        try:
            content = safe_goto(page, url)
            
            if self.is_cloudflare_page(content):
                print(f"    #WARN: Blocked on {url}")
                self.db.mark_url_failed(url)
                return False

            # 1. Topic Discovery (on all pages)
            discovered_topics = extract_topic_links(content)
            if discovered_topics:
                self.db.add_urls(discovered_topics, url_type='topic')

            # 2. Pagination Discovery
            discovered_pgn = extract_pagination_links(content)
            if discovered_pgn:
                # Pagination links inherit the type of the parent page
                self.db.add_urls(discovered_pgn, url_type=url_type)

            # 3. Post Extraction (only for topics)
            if url_type == 'topic':
                posts = parse_topic_content(content)
                if posts:
                    for post in posts: post['source_url'] = url
                    self.db.save_posts(posts)
                    print(f"    #INFO: Saved {len(posts)} posts from {url}")
                else:
                    print(f"    #NOTE: No posts on topic page: {url}")
            else:
                print(f"    #INFO: Processed listing page: {url}")

            self.db.mark_url_completed(url)
            return True
                    
        except Exception as e:
            print(f"    #WARN: Error processing {url}: {e}")
            self.db.mark_url_failed(url)
            return False

    def start(self):
        print("="*60)
        print(f"#INFO: NAIRALAND INDEFINITE CRAWLER STARTED")
        print(f"#INFO: Goal: {config.MAX_TOPICS} | Remote DB: {config.DATABASE_URL[:25]}...")
        print("="*60)

        with BrowserManager(headless=config.HEADLESS) as page:
            while self.processed_count < config.MAX_TOPICS:
                
                # Check Local Queue
                if not self.topic_queue:
                    # Priority 1: Pick pending listings (to find more topics)
                    pending_listings = self.db.get_pending_urls(url_type='listing', limit=20)
                    # Priority 2: Pick pending topics
                    pending_topics = self.db.get_pending_urls(url_type='topic', limit=50)
                    
                    batch = pending_listings + pending_topics
                    if not batch:
                        # Bootstrap if absolutely nothing is left
                        print("#INFO: DB empty. Bootstrapping from homepage...")
                        self.db.add_urls([config.BASE_URL], url_type='listing')
                        batch = self.db.get_pending_urls(limit=10)
                    
                    random.shuffle(batch)
                    self.topic_queue.extend(batch)

                if not self.topic_queue:
                    print("#NOTE: No URLs found. Sleeping...")
                    time.sleep(30)
                    continue

                url, url_type = self.topic_queue.popleft()
                
                # Dedupe check
                if self.db.is_url_visited(url):
                    continue
                
                print(f"\n#INFO: [{self.processed_count}/{config.MAX_TOPICS}] [{url_type.upper()}] {url}")
                
                success = self.process_url(page, url, url_type)
                
                if success:
                    if url_type == 'topic': self.processed_count += 1
                    self.cf_failures = 0
                    time.sleep(config.CRAWL_DELAY + random.uniform(-1, 2))
                else:
                    self.handle_cf_block(url)

if __name__ == "__main__":
    crawler = NairalandCrawler()
    crawler.start()
