import json
import os
import sys
import time
from pathlib import Path
from database import DatabaseManager
import config

def sanitize(text):
    if not isinstance(text, str):
        return str(text)
    # Remove null bytes which Postgres hates in TEXT fields
    return text.replace('\u0000', '').replace('\x00', '')

def run_migration():
    print("# //INFO: Starting migration script...")
    legacy_file = config.OUTPUT_DIR / "scraped_posts.jsonl"
    
    if not legacy_file.exists():
        print(f"# //NOTE: No legacy file found at {legacy_file}")
        return

    try:
        db = DatabaseManager(config.DATABASE_URL)
        print(f"# //INFO: Database connection established.")
    except Exception as e:
        print(f"# //WARN: Database connection failed: {e}")
        return
    
    total_count = 0
    print(f"# //INFO: Migrating in batches of 50 with sanitization...")
    
    batch = []
    try:
        with open(legacy_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    # Support both 'time' and 'post_time' keys
                    p_time = data.get('time') or data.get('post_time') or ""
                    
                    cleaned_data = {
                        "post_id": sanitize(data['post_id']),
                        "author": sanitize(data['author']),
                        "time": sanitize(p_time),
                        "content": sanitize(data['content']),
                        "source_url": sanitize(data.get('source_url', ''))
                    }
                    
                    batch.append(cleaned_data)
                    
                    if len(batch) >= 50:
                        db.save_posts(batch)
                        total_count += len(batch)
                        print(f"  # //INFO: Migrated {total_count} posts...")
                        batch = []
                        time.sleep(0.5)
                except Exception as e:
                    print(f"  # //NOTE: Skipped line: {e}")
                    continue
        
        if batch:
            db.save_posts(batch)
            total_count += len(batch)
            
        print(f"# //INFO: Migration complete. Total posts moved: {total_count}")
        
    except Exception as e:
        print(f"# //WARN: Migration interrupted: {e}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_dir)
    run_migration()
