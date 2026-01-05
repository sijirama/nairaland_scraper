import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# //INFO: Configuration constants for the scraper
DATABASE_URL: str = os.getenv("DATABASE_URL", "")
OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", "data"))
BASE_URL: str = os.getenv("BASE_URL", "https://www.nairaland.com")
HEADLESS: bool = os.getenv("HEADLESS", "false").lower() == "true"
CRAWL_DELAY: float = float(os.getenv("CRAWL_DELAY", "12.0"))
MAX_TOPICS: int = int(os.getenv("MAX_TOPICS", "50000"))

# //INFO: Cloudflare retry settings
MAX_CF_RETRIES: int = 5
CF_BACKOFF_BASE: float = 30.0

if not OUTPUT_DIR.exists():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
