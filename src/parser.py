from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import datetime

def get_topic_id(url: str) -> str:
    #INFO: Extract numeric topic ID from URL
    # Format: nairaland.com/12345/topic-title
    path = url.split('nairaland.com/')[-1].strip('/')
    parts = path.split('/')
    if parts and parts[0].isdigit():
        return parts[0]
    return ""

def get_url_type(url: str) -> str:
    path = url.split('nairaland.com/')[-1].strip('/')
    if not path: return 'listing'
    parts = path.split('/')
    if parts[0].isdigit():
        return 'topic'
    if parts[0] in ['news', 'recent', 'politics', 'romance', 'jobs', 'investment', 'business', 'family']:
        return 'listing'
    return 'listing'

def extract_topic_links(html_content: str) -> List[str]:
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    links = soup.find_all('a')
    for link in links:
        href = link.get('href', '')
        if not href: continue
        path = href
        if href.startswith('http'):
            if 'nairaland.com' in href:
                path = '/' + href.split('nairaland.com/')[-1]
            else: continue
        path = path.strip('/')
        if not path: continue
        parts = path.split('/')
        if parts[0].isdigit() and not any(p.isalpha() for p in parts[0]):
            if len(parts) > 1 and not parts[1].isdigit():
                results.append(f"https://www.nairaland.com/{path}")
    return list(set(results))

def extract_pagination_links(html_content: str) -> List[str]:
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    pgn_links = soup.find_all('a', class_='pgn')
    for link in pgn_links:
        href = link.get('href', '')
        if not href: continue
        if href.startswith('/'):
            results.append(f"https://www.nairaland.com{href}")
        elif 'nairaland.com' in href:
            results.append(href)
    return list(set(results))

def parse_topic_content(html_content: str) -> List[Dict]:
    soup = BeautifulSoup(html_content, 'html.parser')
    posts = []
    body_cells = soup.find_all('td', class_='l w pd')
    for cell in body_cells:
        try:
            post_id = cell.get('id', '')
            if post_id.startswith('pb'):
                post_id = post_id[2:]
            content_div = cell.find('div', class_='narrow')
            if not content_div: continue
            content_text = content_div.get_text('\n', strip=True)
            parent_tr = cell.find_parent('tr')
            header_tr = parent_tr.find_previous_sibling('tr')
            author = "Unknown"
            post_time = ""
            if header_tr:
                author_a = header_tr.find('a', class_='user')
                if author_a: author = author_a.get_text(strip=True)
                time_span = header_tr.find('span', class_='s')
                if time_span:
                    post_time = time_span.get_text(' ', strip=True).split('Modified:')[0].strip()
            if post_id and content_text:
                posts.append({
                    "post_id": post_id,
                    "author": author,
                    "time": post_time,
                    "content": content_text
                })
        except Exception: continue
    return posts
