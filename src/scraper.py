import time
import random
import os
from collections import deque
from typing import List, Dict

from browser import BrowserManager, safe_goto
from parser import parse_homepage_posts, parse_topic_content
import config
from database import DatabaseManager

class NairalandCrawler:
    def __init__(self):
        self.topic_queue = deque()
        self.visited_urls = set()
        self.topics_scraped = 0
        self.cf_failures = 0
        
        #INFO: Initialize shared database manager
        self.db = DatabaseManager(config.DATABASE_URL)
        
        #INFO: Random startup offset to prevent race conditions in distributed mode
        time.sleep(random.uniform(0, 10))

    def is_cloudflare_page(self, html_content: str) -> bool:
        cf_indicators = [
            "Just a moment...",
            "Checking your browser",
            "challenges.cloudflare.com",
            "cf-chl-bypass",
            "Verifying you are human"
        ]
        return any(indicator in html_content for indicator in cf_indicators)

    def handle_cf_block(self, topic_url: str):
        self.cf_failures += 1
        
        backoff = config.CF_BACKOFF_BASE * (2 ** (self.cf_failures - 1))
        backoff += random.uniform(0, 10)
        
        print(f"\n    #WARN: Cloudflare block #{self.cf_failures}. Backing off for {int(backoff)}s...")
        time.sleep(backoff)
        
        #INFO: Return failed topic to front of queue
        self.topic_queue.appendleft(topic_url)
        
        if self.cf_failures >= 5:
            print("#WARN: Critical block level reached. Cooling down.")
            time.sleep(300)
            self.cf_failures = 0

    def crawl_topic(self, page, start_url: str):
        current_url = start_url
        while current_url:
            #INFO: Distributed check - check if any instance has completed this URL
            if self.db.is_url_visited(current_url):
                print(f"    #NOTE: URL already processed by another instance: {current_url}")
                break

            self.db.mark_url_processing(current_url)
            
            try:
                content = safe_goto(page, current_url)
                
                if self.is_cloudflare_page(content):
                    print(f"    #WARN: Block detected on {current_url}")
                    self.db.mark_url_failed(current_url)
                    return False

                data = parse_topic_content(content)
                
                if not data['posts']:
                    debug_path = config.OUTPUT_DIR / f"debug_topic_{random.randint(0,1000)}.html"
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    print(f"    #NOTE: No posts found. Debug HTML saved.")

                #INFO: Save posts to shared DB (handles de-duplication)
                for post in data['posts']:
                    post['source_url'] = current_url
                
                self.db.save_posts(data['posts'])
                self.db.mark_url_completed(current_url)
                
                print(f"    #INFO: Extracted {len(data['posts'])} posts from {current_url}")

                #INFO: Handle topic pagination
                next_url = None
                for pgn_url in data['pagination']:
                    if not self.db.is_url_visited(pgn_url):
                        next_url = pgn_url
                        break
                
                current_url = next_url
                if current_url:
                    time.sleep(random.uniform(2, 5))
                    
            except Exception as e:
                print(f"    #WARN: Failed to crawl {start_url}: {e}")
                self.db.mark_url_failed(start_url)
                return False
        
        return True

    def start(self):
        print("="*60)
        print(f"#INFO: NAIRALAND DISTRIBUTED CRAWLER STARTED")
        print(f"#INFO: Limit: {config.MAX_TOPICS} topics | Shared DB: {config.DATABASE_URL[:20]}...")
        print("="*60)

        with BrowserManager(headless=config.HEADLESS) as page:
            print(f"\n#INFO: [Step 1] Bootstrapping from homepage")
            homepage_html = safe_goto(page, config.BASE_URL, is_first_request=True)
            
            if self.is_cloudflare_page(homepage_html):
                print("#WARN: Blocked during bootstrap. Manual intervention may be needed.")
                return

            initial_topics = parse_homepage_posts(homepage_html)
            
            temp_list = [t['url'] for t in initial_topics]
            random.shuffle(temp_list)
            self.topic_queue = deque(temp_list)
            
            print(f"#INFO: Found {len(self.topic_queue)} initial topics.")

            while self.topic_queue and self.topics_scraped < config.MAX_TOPICS:
                topic_url = self.topic_queue.popleft()
                
                if self.db.is_url_visited(topic_url):
                    continue
                
                self.topics_scraped += 1
                print(f"\n#INFO: [{self.topics_scraped}/{config.MAX_TOPICS}] Processing Topic: {topic_url}")
                
                success = self.crawl_topic(page, topic_url)
                
                if success:
                    self.cf_failures = 0
                    time.sleep(config.CRAWL_DELAY + random.uniform(-2, 3))
                else:
                    self.handle_cf_block(topic_url)

if __name__ == "__main__":
    crawler = NairalandCrawler()
    crawler.start()
