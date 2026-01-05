from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import datetime

def get_url_type(url: str) -> str:
    #INFO: Determine if a URL is a discussion topic or a listing page
    path = url.split('nairaland.com/')[-1].strip('/')
    if not path: return 'listing' # Base URL is a listing
    
    parts = path.split('/')
    if parts[0].isdigit():
        return 'topic'
    
    # Check for known listing paths
    if parts[0] in ['news', 'recent', 'politics', 'romance', 'jobs', 'investment', 'business']:
        return 'listing'
        
    return 'listing' # Default to listing for discovery

def extract_topic_links(html_content: str) -> List[str]:
    #INFO: Extract topic links from any page
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    links = soup.find_all('a')
    
    for link in links:
        href = link.get('href', '')
        text = link.get_text(strip=True)
        if not href or not text: continue

        path = href
        if href.startswith('http'):
            if 'nairaland.com' in href:
                path = '/' + href.split('nairaland.com/')[-1]
            else: continue
        
        path = path.strip('/')
        if not path: continue
        
        parts = path.split('/')
        if parts[0].isdigit():
            # It's a topic
            results.append(f"https://www.nairaland.com/{path}")
            
    return list(set(results))

def extract_pagination_links(html_content: str) -> List[str]:
    #INFO: Extract pagination links (could be topic pages or listing pages)
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    pgn_links = soup.find_all('a', class_='pgn')
    
    for link in pgn_links:
        href = link.get('href', '')
        if not href: continue
        
        if href.startswith('/'):
            results.append(f"https://www.nairaland.com{href}")
        elif href.startswith('https://www.nairaland.com'):
            results.append(href)
            
    return list(set(results))

def parse_topic_content(html_content: str) -> List[Dict]:
    #INFO: Parses topic pages to extract posts
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

            if post_id:
                posts.append({
                    "post_id": post_id,
                    "author": author,
                    "time": post_time,
                    "content": content
                })

    return posts
