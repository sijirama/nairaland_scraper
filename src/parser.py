from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import datetime

def parse_homepage_posts(html_content: str) -> List[Dict[str, str]]:
    #INFO: Parses homepage to extract thread links
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    links = soup.find_all('a')
    seen_ids = set()

    for link in links:
        href = link.get('href', '')
        text = link.get_text(strip=True)
        if not href or not text: continue

        path = href
        if href.startswith('http'):
            if 'nairaland.com' in href:
                path = '/' + href.split('nairaland.com/')[-1]
            else: continue
        
        if path.startswith('/') and len(path) > 1:
            parts = path.strip('/').split('/')
            if parts and parts[0].isdigit():
                topic_id = parts[0]
                if topic_id in seen_ids: continue
                seen_ids.add(topic_id)
                if text.isdigit(): continue

                results.append({
                    "id": topic_id,
                    "title": text,
                    "url": f"https://www.nairaland.com{path}",
                    "scraped_at": datetime.datetime.now().isoformat()
                })
    return results

def parse_topic_content(html_content: str) -> Dict:
    #INFO: Parses topic pages to extract posts and pagination
    soup = BeautifulSoup(html_content, 'html.parser')
    post_tables = soup.find_all('table', summary='posts')
    posts = []
    
    for table in post_tables:
        body_td = table.find('td', class_='l w pd') or table.find('td', class_='body')
        if body_td:
            author_a = table.find('a', class_='user')
            author = author_a.get_text(strip=True) if author_a else "Unknown"
            
            time_span = table.find('span', class_='s')
            post_time = time_span.get_text(strip=True) if time_span else ""
            content = body_td.get_text('\n', strip=True)
            
            post_id = ""
            if body_td.has_attr('id') and body_td['id'].startswith('pb'):
                post_id = body_td['id'][2:]

            posts.append({
                "post_id": post_id,
                "author": author,
                "time": post_time,
                "content": content
            })

    next_pages = []
    pgn_links = soup.find_all('a', class_='pgn')
    for link in pgn_links:
        href = link.get('href', '')
        if href.startswith('/'):
            full_url = f"https://www.nairaland.com{href}"
            if full_url not in next_pages:
                next_pages.append(full_url)

    return {"posts": posts, "pagination": next_pages}
