from playwright.sync_api import sync_playwright, Page, BrowserContext
from playwright_stealth import stealth_sync
import time
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

class BrowserManager:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.user_data_dir = "/tmp/playwright_user_data"

    def __enter__(self) -> Page:
        self.playwright = sync_playwright().start()
        
        #INFO: Launching persistent context to maintain challenge state
        self.context = self.playwright.chromium.launch_persistent_context(
            user_data_dir=self.user_data_dir,
            headless=self.headless,
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1920, "height": 1080},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ]
        )
        
        page = self.context.pages[0] if self.context.pages else self.context.new_page()
        stealth_sync(page)
        
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)
        
        return page

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.context: self.context.close()
        if self.playwright: self.playwright.stop()


def wait_for_cloudflare(page: Page, max_wait: int = 180):
    #INFO: Handles Turnstile challenges via human simulation
    start_time = time.time()
    check_count = 0
    
    try:
        page.mouse.move(random.randint(200, 600), random.randint(200, 600))
        time.sleep(1)
        page.keyboard.press("PageDown")
        time.sleep(0.5)
        page.keyboard.press("PageUp")
    except:
        pass

    while time.time() - start_time < max_wait:
        check_count += 1
        
        try:
            title = page.title()
        except:
            title = ""
        
        if "Nairaland" in title and "Just a moment" not in title:
            print(f"    #INFO: Challenge passed in {int(time.time() - start_time)}s")
            return True
        
        content = ""
        try:
            content = page.content()[:8000]
        except:
            pass

        is_cf = any(ind in title for ind in ["Just a moment", "Checking your browser"]) or \
                any(ind in content for ind in ["Verifying you are human", "cf-challenge", "turnstile"])
        
        if is_cf:
            if check_count % 3 == 1:
                print(f"    #INFO: Still waiting for Cloudflare... ({check_count})")
            
            if check_count % 6 == 1:
                try:
                    page.screenshot(path="/app/data/cf_progress.png")
                except:
                    pass

            try:
                if check_count % 2 == 0:
                    page.mouse.move(random.randint(0, 1000), random.randint(0, 800), steps=10)
                if check_count % 5 == 0:
                    page.mouse.wheel(0, 100)
            except:
                pass

            try:
                iframe = None
                selectors = ['iframe[src*="challenges.cloudflare.com"]', 'iframe[title*="Cloudflare"]', 'iframe[title*="widget"]']
                for selector in selectors:
                    iframe = page.query_selector(selector)
                    if iframe: break
                
                if iframe:
                    box = iframe.bounding_box()
                    if box and box['width'] > 0:
                        if check_count >= 3 and check_count % 3 == 0:
                            tx = box["x"] + 30 + random.randint(0, 10)
                            ty = box["y"] + (box["height"] / 2)
                            page.mouse.move(tx, ty, steps=15)
                            time.sleep(0.3)
                            page.mouse.click(tx, ty)
                            print(f"    #INFO: Interacted with Turnstile widget")
                else:
                    if check_count > 6 and check_count % 5 == 0:
                        v = page.viewport_size or {"width": 1280, "height": 720}
                        page.mouse.click(v["width"]/2, v["height"]/2 + 100)
            except:
                pass
            
            time.sleep(random.uniform(5, 8))
        else:
            if check_count > 5:
                print(f"    #INFO: Page state unclear ('{title}'). Waiting...")
            time.sleep(4)
    
    print(f"    #WARN: Challenge timeout reached")
    return False


def human_delay():
    time.sleep(random.uniform(2.0, 5.0))


def safe_goto(page: Page, url: str, timeout: int = 60000, is_first_request: bool = False):
    print(f"    #INFO: Navigating to {url}")
    
    if not is_first_request:
        human_delay()
    
    try:
        page.goto(url, wait_until="commit", timeout=timeout)
        time.sleep(2)
    except Exception as e:
        print(f"    #WARN: Navigation issue: {str(e)[:50]}")
    
    title = page.title()
    if "Just a moment" in title or "Checking your browser" in title:
        success = wait_for_cloudflare(page, max_wait=180)
        if not success:
            return page.content()
    
    try:
        page.wait_for_selector("body", timeout=10000)
        page.wait_for_selector("#up, .topictitle, table[summary='posts']", timeout=10000)
    except:
        pass
    
    return page.content()
