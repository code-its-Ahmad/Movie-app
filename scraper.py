# scraper.py
"""
Web scraper for movies, series, and anime from multiple sources:
- hindilinks4u.host: Movies and TV series
- toonstream.one: Anime content (redirected from toonstream.love)

This module provides async functions to scrape:
- Movies by search, year, genre, director
- TV series by page and search
- Episodes with streaming links
- Anime by search and category

All functions use proper URL construction, error handling, and logging.
"""
from httpx import AsyncClient, AsyncHTTPTransport, HTTPStatusError, RequestError
from bs4 import BeautifulSoup
from urllib.parse import quote
import logging
import asyncio
import re
from fastapi import HTTPException
from typing import List, Optional, Tuple
from models import Anime, Episode, Movie, Series, AnimeSeriesDetail, AnimeEpisode, ToonstreamMovieDetail, SeriesDetail, SeriesEpisode, MovieDetail, StreamingServer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base URLs for scraping
# Note: hindilinks4u.host redirects to hindilinks4u.delivery
HINDILINKS_BASE_URL = "https://hindilinks4u.delivery"
HINDILINKS_OLD_BASE_URL = "https://hindilinks4u.host"  # For fallback
TOONSTREAM_BASE_URL = "https://toonstream.one"

# Helper function to extract series slug from URL
def extract_series_slug_from_url(url: str) -> Optional[str]:
    """
    Extract series slug from a toonstream.one series URL.
    Examples:
        https://toonstream.one/series/a-wild-last-boss-appeared/ -> a-wild-last-boss-appeared
        /series/spy-x-family/ -> spy-x-family
    """
    if not url:
        return None
    # Remove query parameters and fragments
    url = url.split('?')[0].split('#')[0]
    # Match /series/{slug}/ pattern
    match = re.search(r'/series/([^/]+)', url)
    if match:
        return match.group(1).rstrip('/')
    return None

# Helper function to extract movie slug from URL
def extract_movie_slug_from_url(url: str) -> Optional[str]:
    """
    Extract movie slug from a toonstream.one movie URL.
    Examples:
        https://toonstream.one/movies/crayon-shin-chan-the-movie-super-hot-the-spicy-kasukabe-dancers/ -> crayon-shin-chan-the-movie-super-hot-the-spicy-kasukabe-dancers
        /movies/your-name/ -> your-name
    """
    if not url:
        return None
    # Remove query parameters and fragments
    url = url.split('?')[0].split('#')[0]
    # Match /movies/{slug}/ pattern
    match = re.search(r'/movies/([^/]+)', url)
    if match:
        return match.group(1).rstrip('/')
    return None

# Helper function to normalize movie slug
def normalize_movie_slug(title: str) -> str:
    """
    Convert a movie title to a slug format matching toonstream.one's URL structure.
    Examples:
        'Crayon Shin-chan the Movie: Super Hot!' -> 'crayon-shin-chan-the-movie-super-hot'
        'Your Name' -> 'your-name'
    """
    if not title:
        return ""
    # If already a slug (contains hyphens and no spaces), just clean it up
    if '-' in title and ' ' not in title:
        slug = title.strip().lower()
        slug = re.sub(r'[^\w-]', '', slug)
        slug = re.sub(r'-+', '-', slug)
        slug = slug.strip('-')
        return slug
    
    # Convert to lowercase
    slug = title.strip().lower()
    # Convert spaces to hyphens
    slug = re.sub(r'\s+', '-', slug)
    # Handle special characters: keep alphanumeric and hyphens
    slug = re.sub(r'[^\w-]', '', slug)
    # Remove multiple consecutive hyphens
    slug = re.sub(r'-+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    return slug

# Helper function to normalize series slug
def normalize_series_slug(title: str) -> str:
    """
    Convert a series title to a slug format matching toonstream.one's URL structure.
    Examples:
        'A Wild Last Boss Appeared' -> 'a-wild-last-boss-appeared'
        'SPY x FAMILY' -> 'spy-x-family'
        'Sentenced to Be a Hero' -> 'sentenced-to-be-a-hero'
        'a-wild-last-boss-appeared' -> 'a-wild-last-boss-appeared' (no change)
    """
    if not title:
        return ""
    # If already a slug (contains hyphens and no spaces), just clean it up
    if '-' in title and ' ' not in title:
        slug = title.strip().lower()
        slug = re.sub(r'[^\w-]', '', slug)
        slug = re.sub(r'-+', '-', slug)
        slug = slug.strip('-')
        return slug
    
    # Convert to lowercase
    slug = title.strip().lower()
    # Convert spaces to hyphens
    slug = re.sub(r'\s+', '-', slug)
    # Handle special characters: keep alphanumeric and hyphens
    slug = re.sub(r'[^\w-]', '', slug)
    # Remove multiple consecutive hyphens
    slug = re.sub(r'-+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    return slug

# Helper function to extract server name from link/button text
def extract_server_name(link_element, default_name: str = None) -> str:
    """Extract server name from link text, button text, or parent element"""
    if not link_element:
        return default_name or "Server"
    
    # Try to get text from the link itself
    text = link_element.text.strip() if hasattr(link_element, 'text') else ""
    
    # Common server name patterns
    server_patterns = [
        r'(Server\s*\d+)',
        r'(HD\s*Server)',
        r'(SD\s*Server)',
        r'(720p|1080p|480p|360p)',
        r'(DoodStream|Streamtape|Mixdrop|Vidstream|Gounlimited)',
        r'(Watch|Stream|Play|Download)',
    ]
    
    for pattern in server_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    # Try parent element
    if hasattr(link_element, 'parent') and link_element.parent:
        parent_text = link_element.parent.get_text() if hasattr(link_element.parent, 'get_text') else ""
        for pattern in server_patterns:
            match = re.search(pattern, parent_text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
    
    # Try class names
    if hasattr(link_element, 'get'):
        class_name = link_element.get('class', [])
        if class_name:
            class_str = ' '.join(class_name).lower()
            if 'server' in class_str:
                # Extract server number or name
                server_match = re.search(r'server[-\s]*(\d+|[a-z]+)', class_str)
                if server_match:
                    return f"Server {server_match.group(1).title()}"
    
    return default_name or f"Server {text[:20]}" if text else "Server"

# Helper function to extract quality from text or URL
def extract_quality(text: str, url: str = "") -> Optional[str]:
    """Extract video quality from text or URL"""
    quality_patterns = [
        r'\b(720p|1080p|480p|360p|240p|4K|2160p)\b',
        r'\b(HD|SD|FHD|UHD)\b',
        r'\b(High|Medium|Low)\s*Quality\b',
    ]
    
    search_text = (text + " " + url).lower()
    for pattern in quality_patterns:
        match = re.search(pattern, search_text, re.IGNORECASE)
        if match:
            return match.group(1).upper()
    
    return None

# Helper function to determine server type from URL
def get_server_type(url: str) -> str:
    """Determine server type from URL"""
    url_lower = url.lower()
    if any(x in url_lower for x in ['embed', 'iframe']):
        return 'embed'
    elif any(x in url_lower for x in ['.mp4', '.m3u8', 'direct', 'cdn']):
        return 'direct'
    elif 'drive.google' in url_lower:
        return 'gdrive'
    elif any(x in url_lower for x in ['dood', 'streamtape', 'mixdrop']):
        return 'filehost'
    else:
        return 'iframe'

# Helper function to resolve embed URLs to direct playable links
async def resolve_embed_to_direct(client: AsyncClient, embed_url: str, max_depth: int = 3) -> Optional[str]:
    """Recursively resolve embed URLs to find direct playable video links with comprehensive support for various hosting services"""
    if max_depth <= 0:
        return None
    
    try:
        # Ensure embed_url is a string (handle HttpUrl objects)
        embed_url = str(embed_url)
        
        # Filter out non-video URLs early to avoid unnecessary requests
        embed_url_lower = embed_url.lower()
        
        # Skip non-video file types
        non_video_extensions = ['.css', '.js', '.json', '.xml', '.txt', '.pdf', '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico']
        if any(ext in embed_url_lower for ext in non_video_extensions):
            return None
        
        # Skip non-video domains/paths
        non_video_keywords = ['/wp-content/', '/wp-includes/', '/themes/', '/plugins/', '/assets/', '/static/', '/css/', '/js/', '/fonts/', '/images/']
        if any(keyword in embed_url_lower for keyword in non_video_keywords):
            return None
        
        logger.debug(f"Resolving embed URL (depth {max_depth}): {embed_url}")
        
        # Special handling for known hosting services
        # DoodStream pattern: extract direct link
        if 'doodstream.com' in embed_url_lower or 'dood' in embed_url_lower or 'dood.to' in embed_url_lower:
            try:
                response = await client.get(embed_url, follow_redirects=True, timeout=20.0)
                if response.status_code == 200:
                    text = response.text
                    # Pattern: var pass_md5 = "..."; var video_url = "...";
                    md5_match = re.search(r'var\s+pass_md5\s*=\s*["\']([^"\']+)["\']', text)
                    video_url_match = re.search(r'var\s+video_url\s*=\s*["\']([^"\']+)["\']', text)
                    if md5_match and video_url_match:
                        video_url = video_url_match.group(1)
                        if video_url.startswith('//'):
                            video_url = 'https:' + video_url
                        if video_url.startswith('http') and any(ext in video_url.lower() for ext in ['.mp4', '.m3u8', 'stream', 'video']):
                            return video_url
                    
                    # Alternative patterns for DoodStream
                    patterns = [
                        r'["\'](https?://[^"\']*dood[^"\']*\.(?:mp4|m3u8)[^"\']*)["\']',
                        r'file["\']?\s*[:=]\s*["\'](https?://[^"\']+\.mp4[^"\']*)["\']',
                        r'sources["\']?\s*[:=]\s*\[["\'](https?://[^"\']+\.mp4[^"\']*)["\']',
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            url = match.group(1)
                            if url.startswith('//'):
                                url = 'https:' + url
                            if url.startswith('http'):
                                return url
            except Exception as e:
                logger.debug(f"DoodStream resolution attempt failed: {e}")
        
        # Streamtape pattern
        if 'streamtape.com' in embed_url_lower or 'streamtape.to' in embed_url_lower:
            try:
                response = await client.get(embed_url, follow_redirects=True, timeout=20.0)
                if response.status_code == 200:
                    text = response.text
                    # Look for get_video URL
                    video_match = re.search(r'get_video["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']', text)
                    if video_match:
                        return video_match.group(1)
                    # Alternative pattern
                    video_match = re.search(r'robotlink["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']', text)
                    if video_match:
                        return video_match.group(1)
            except Exception as e:
                logger.debug(f"Streamtape resolution attempt failed: {e}")
        
        # Mixdrop pattern
        if 'mixdrop.co' in embed_url_lower or 'mixdrop.to' in embed_url_lower or 'mixdrop' in embed_url_lower:
            try:
                response = await client.get(embed_url, follow_redirects=True, timeout=20.0)
                if response.status_code == 200:
                    text = response.text
                    # Look for video source
                    video_match = re.search(r'MDCore\.wurl\s*=\s*["\'](https?://[^"\']+)["\']', text)
                    if video_match:
                        return video_match.group(1)
                    # Alternative patterns
                    patterns = [
                        r'MDCore\.wurl\s*=\s*["\'](https?://[^"\']+)["\']',
                        r'sources["\']?\s*[:=]\s*\[["\'](https?://[^"\']+\.mp4[^"\']*)["\']',
                        r'file["\']?\s*[:=]\s*["\'](https?://[^"\']+\.mp4[^"\']*)["\']',
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            url = match.group(1)
                            if url.startswith('//'):
                                url = 'https:' + url
                            if url.startswith('http'):
                                return url
            except Exception as e:
                logger.debug(f"Mixdrop resolution attempt failed: {e}")
        
        # Vidstream/Vidcloud pattern
        if 'vidstream' in embed_url_lower or 'vidcloud' in embed_url_lower:
            try:
                response = await client.get(embed_url, follow_redirects=True, timeout=20.0)
                if response.status_code == 200:
                    text = response.text
                    soup_temp = BeautifulSoup(text, 'html.parser')
                    # Look for iframe or video tag
                    iframe = soup_temp.find('iframe')
                    if iframe:
                        iframe_src = iframe.get('src')
                        if iframe_src:
                            if iframe_src.startswith('//'):
                                iframe_src = 'https:' + iframe_src
                            if iframe_src.startswith('http'):
                                resolved = await resolve_embed_to_direct(client, iframe_src, max_depth - 1)
                                if resolved:
                                    return resolved
                    # Look for video tag
                    video = soup_temp.find('video')
                    if video:
                        src = video.get('src')
                        if src:
                            if src.startswith('//'):
                                src = 'https:' + src
                            if src.startswith('http'):
                                return src
            except Exception as e:
                logger.debug(f"Vidstream/Vidcloud resolution attempt failed: {e}")
        
        # Trembed pattern (common in hindilinks4u)
        if 'trembed' in embed_url_lower or '/embed/' in embed_url_lower:
            try:
                response = await client.get(embed_url, follow_redirects=True, timeout=20.0)
                if response.status_code == 200:
                    text = response.text
                    soup_temp = BeautifulSoup(text, 'html.parser')
                    # Look for iframe
                    iframe = soup_temp.find('iframe')
                    if iframe:
                        iframe_src = iframe.get('src') or iframe.get('data-src')
                        if iframe_src:
                            if iframe_src.startswith('//'):
                                iframe_src = 'https:' + iframe_src
                            if iframe_src.startswith('http'):
                                resolved = await resolve_embed_to_direct(client, iframe_src, max_depth - 1)
                                if resolved:
                                    return resolved
            except Exception as e:
                logger.debug(f"Trembed resolution attempt failed: {e}")
        
        # Standard resolution
        response = await client.get(embed_url, follow_redirects=True, timeout=20.0)
        
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Priority 1: Direct video sources
        video_tag = soup.find('video')
        if video_tag:
            src = video_tag.get('src') or video_tag.get('data-src') or video_tag.get('data-url')
            if src:
                if src.startswith('//'):
                    src = 'https:' + src
                elif src.startswith('/'):
                    base_url = '/'.join(embed_url.split('/')[:3])
                    src = base_url + src
                if src.startswith('http'):
                    return src
            
            # Check source tags
            source_tags = video_tag.find_all('source')
            for source_tag in source_tags:
                src = source_tag.get('src') or source_tag.get('data-src')
                if src:
                    if src.startswith('//'):
                        src = 'https:' + src
                    elif src.startswith('/'):
                        base_url = '/'.join(embed_url.split('/')[:3])
                        src = base_url + src
                    if src.startswith('http') and any(ext in src.lower() for ext in ['.mp4', '.m3u8', '.webm', '.mkv', '.flv']):
                        return src
        
        # Priority 2: Iframe sources (recursive)
        iframes = soup.find_all('iframe')
        for iframe in iframes:
            iframe_src = iframe.get('src') or iframe.get('data-src') or iframe.get('data-url') or iframe.get('data-frame')
            if iframe_src:
                if iframe_src.startswith('//'):
                    iframe_src = 'https:' + iframe_src
                elif iframe_src.startswith('/'):
                    base_url = '/'.join(embed_url.split('/')[:3])
                    iframe_src = base_url + iframe_src
                
                if iframe_src.startswith('http'):
                    # Filter out social media and ads
                    exclude_domains = ['facebook', 'twitter', 'instagram', 'youtube.com/channel', 'google', 'ads', 'advertisement']
                    if not any(exclude in iframe_src.lower() for exclude in exclude_domains):
                        # Recursively resolve
                        resolved = await resolve_embed_to_direct(client, iframe_src, max_depth - 1)
                        if resolved:
                            return resolved
        
        # Priority 3: Script-based redirects or video URLs (comprehensive patterns)
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string or ""
            if not script_text:
                continue
            
            # Comprehensive patterns for video URLs
            patterns = [
                r'["\'](https?://[^"\']+\.(?:mp4|m3u8|webm|mkv|flv)[^"\']*)["\']',
                r'src["\']?\s*[:=]\s*["\'](https?://[^"\']+\.(?:mp4|m3u8|webm|mkv)[^"\']*)["\']',
                r'file["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'video["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'stream["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'source["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'fileurl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'video_url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'videoUrl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'getElementById\(["\']([^"\']+)["\']\)\.src\s*=\s*["\'](https?://[^"\']+)["\']',
                r'\.setAttribute\(["\']src["\'],\s*["\'](https?://[^"\']+)["\']',
                r'iframe\.src\s*=\s*["\'](https?://[^"\']+)["\']',
                r'player\.src\s*=\s*["\'](https?://[^"\']+)["\']',
                r'sources["\']?\s*[:=]\s*\[["\'](https?://[^"\']+\.mp4[^"\']*)["\']',
                r'jwplayer\(["\']([^"\']+)["\']\)\.setup\([^}]+file["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, script_text, re.IGNORECASE)
                for match in matches:
                    # Handle tuple matches
                    url = match[1] if isinstance(match, tuple) and len(match) > 1 else match
                    if isinstance(match, tuple):
                        url = match[1] if len(match) > 1 else match[0]
                    else:
                        url = match
                    
                    if url and url.startswith('http'):
                        # Check if it looks like a video URL
                        video_indicators = ['.mp4', '.m3u8', '.webm', '.mkv', '.flv', 'video', 'stream', 'cdn', 'media', 'dood', 'streamtape', 'mixdrop']
                        if any(indicator in url.lower() for indicator in video_indicators):
                            # Exclude common non-video URLs
                            exclude_keywords = ['analytics', 'tracking', 'advert', 'ads', 'facebook', 'twitter', 'instagram', 'google-analytics', 'doubleclick']
                            if not any(exclude in url.lower() for exclude in exclude_keywords):
                                return url
        
        # Priority 4: Look for data attributes with video URLs
        elements_with_data = soup.find_all(attrs=lambda x: x and any(k.startswith('data-') for k in x.keys()))
        for elem in elements_with_data:
            for attr, value in elem.attrs.items():
                if attr.startswith('data-') and isinstance(value, str) and value.startswith('http'):
                    if any(ext in value.lower() for ext in ['.mp4', '.m3u8', '.webm', 'video', 'stream', 'cdn']):
                        exclude_keywords = ['analytics', 'tracking', 'advert', 'ads']
                        if not any(exclude in value.lower() for exclude in exclude_keywords):
                            return value
        
        # Priority 5: Check for HLS/M3U8 playlists
        m3u8_links = soup.find_all('a', href=re.compile(r'\.m3u8', re.I))
        for link in m3u8_links:
            href = link.get('href')
            if href:
                if href.startswith('//'):
                    href = 'https:' + href
                elif href.startswith('/'):
                    base_url = '/'.join(embed_url.split('/')[:3])
                    href = base_url + href
                if href.startswith('http'):
                    return href
        
        # Priority 6: Check for window.location redirects
        for script in scripts:
            script_text = script.string or ""
            if not script_text:
                continue
            redirect_match = re.search(r'window\.location\.(?:href|replace)\s*=\s*["\'](https?://[^"\']+)["\']', script_text, re.IGNORECASE)
            if redirect_match:
                redirect_url = redirect_match.group(1)
                if redirect_url.startswith('http'):
                    resolved = await resolve_embed_to_direct(client, redirect_url, max_depth - 1)
                    if resolved:
                        return resolved
        
        return None
    except Exception as e:
        logger.warning(f"Failed to resolve embed {embed_url}: {e}")
        return None

# Dependency to provide HTTP client
async def get_http_client():
    transport = AsyncHTTPTransport(retries=3)
    client = AsyncClient(
        transport=transport,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        },
        timeout=10.0,
        follow_redirects=True  # Explicitly enable redirect following
    )
    try:
        yield client
    finally:
        await client.aclose()

# Helper function to parse movies or series from a BeautifulSoup object
def parse_items(soup: BeautifulSoup, logger, model_class: type) -> List[Movie | Series]:
    items_list = soup.find('div', class_='movies-list movies-list-full')
    if not items_list:
        logger.warning("No items list found on the page")
        return []
    
    results = []
    base_url = HINDILINKS_BASE_URL
    for item in items_list.find_all('div', class_='ml-item'):
        item_data = {}
        link_tag = item.find('a', class_='ml-mask')
        
        # Try multiple ways to get title
        title = ''
        if link_tag:
            title = link_tag.get('oldtitle', '') or link_tag.get('title', '')
            if not title:
                # Try to get from img alt or text content
                img_tag = item.find('img')
                if img_tag:
                    title = img_tag.get('alt', '') or img_tag.get('title', '')
                if not title and link_tag.text:
                    title = link_tag.text.strip()
        
        item_data['title'] = title
        url = link_tag.get('href', '') if link_tag else ''
        
        # Ensure URL is absolute
        if url:
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = base_url + url
            elif not url.startswith(('http://', 'https://')):
                url = base_url + '/' + url.lstrip('/')
        
        item_data['url'] = url
        
        if not item_data['title'] or not item_data['url']:
            logger.debug(f"Skipping item due to missing title or URL: {item_data}")
            continue
        
        # Determine if item is a series
        is_series = 'series' in item_data['url'].lower() or item.find('span', class_='mli-eps')
        if model_class == Series and not is_series:
            logger.debug(f"Skipping non-series item: {item_data['title']}")
            continue
        if model_class == Movie and is_series:
            logger.debug(f"Skipping series item for movie parsing: {item_data['title']}")
            continue

        img_tag = item.find('img', class_='lazy thumb mli-thumb')
        image_url = None
        if img_tag:
            image_url = img_tag.get('data-original') or img_tag.get('src')
            if image_url:
                # Ensure image URL is absolute
                if image_url.startswith('//'):
                    image_url = 'https:' + image_url
                elif image_url.startswith('/'):
                    image_url = base_url + image_url
                elif not image_url.startswith(('http://', 'https://')):
                    image_url = base_url + '/' + image_url.lstrip('/')
        item_data['image'] = image_url
        quality_tag = item.find('span', class_='mli-quality') or item.find('span', class_='mli-eps')
        item_data['quality'] = quality_tag.text.strip() if quality_tag else None
        hidden_tip = item.find('div', id='hidden_tip')
        if hidden_tip:
            # Extract IMDb rating
            imdb_tag = hidden_tip.find('div', class_='jt-imdb')
            item_data['imdb'] = imdb_tag.text.strip() if imdb_tag else None
            
            # Extract year - try multiple approaches
            year_tag = hidden_tip.find('div', class_='jt-info')
            if year_tag:
                year_link = year_tag.find('a')
                if year_link:
                    year_text = year_link.text.strip()
                    # Extract year (4 digits)
                    year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                    item_data['year'] = year_match.group(0) if year_match else year_text
                else:
                    # Try to extract year from text
                    year_text = year_tag.text.strip()
                    year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                    item_data['year'] = year_match.group(0) if year_match else None
            else:
                item_data['year'] = None
            
            # Extract duration
            jt_info_tags = hidden_tip.find_all('div', class_='jt-info')
            item_data['duration'] = jt_info_tags[-1].text.strip() if jt_info_tags else None
            
            # Extract description
            desc_tag = hidden_tip.find('p', class_='f-desc')
            item_data['description'] = desc_tag.text.strip() if desc_tag else None
            
            # Extract genres
            block_tag = hidden_tip.find('div', class_='block')
            if block_tag:
                item_data['genres'] = [
                    a.text.strip() for a in block_tag.find_all('a', href=True) 
                    if 'genre' in a.get('href', '').lower()
                ]
            else:
                item_data['genres'] = []
            
            # Extract series-specific data
            if model_class == Series:
                seasons_episodes = hidden_tip.find('div', class_='jt-info')
                if seasons_episodes:
                    seasons_episodes_text = seasons_episodes.text.strip()
                    if 'Season' in seasons_episodes_text:
                        try:
                            item_data['seasons'] = seasons_episodes_text.split('Season')[1].strip().split()[0]
                        except (IndexError, AttributeError):
                            item_data['seasons'] = None
                    else:
                        item_data['seasons'] = None
                else:
                    item_data['seasons'] = None
                
                eps_tag = item.find('span', class_='mli-eps')
                item_data['episodes'] = eps_tag.text.strip() if eps_tag else None
        
        # Extract language
        language = None
        if hidden_tip:
            language_tag = hidden_tip.find('div', class_='jt-info', string=lambda x: x and 'language' in x.lower())
            if language_tag:
                language = language_tag.text.strip().replace('Language:', '').strip()
            elif item_data['description']:
                for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam']:
                    if lang in item_data['description'].lower():
                        language = lang
                        break
            elif item_data['title']:
                for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam']:
                    if f'({lang})' in item_data['title'].lower() or f'[{lang}]' in item_data['title'].lower():
                        language = lang
                        break
        item_data['language'] = language

        try:
            results.append(model_class(**item_data))
        except Exception as e:
            logger.error(f"Failed to create {model_class.__name__} object for {item_data['title']}: {e}")
            continue
    
    return results

# Helper function to filter items by language
def filter_by_language(items: List[Movie | Series], language: Optional[str]) -> List[Movie | Series]:
    if not language:
        return items
    language = language.lower()
    filtered = [
        item for item in items
        if (item.language and language in item.language.lower()) or
           (item.description and language in item.description.lower()) or
           (item.title and language in item.title.lower())
    ]
    logger.info(f"Filtered {len(filtered)} items for language: {language}")
    return filtered

# Helper function to scrape a single page
async def scrape_page(url: str, client: AsyncClient, logger, semaphore: asyncio.Semaphore, model_class: type) -> Tuple[List[Movie | Series], Optional[str]]:
    async with semaphore:
        logger.info(f"Scraping URL: {url}")
        await asyncio.sleep(1)
        try:
            response = await client.get(url)
            response.raise_for_status()
            
            if not response.text:
                logger.warning(f"Empty response from {url}")
                return [], None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            items = parse_items(soup, logger, model_class)
            
            # Find next page link
            next_url = None
            pagination = soup.find('div', class_='pagination')
            if pagination:
                next_link = pagination.find('a', class_='next page-numbers')
                if next_link:
                    next_url = next_link.get('href')
                    # Ensure next_url is absolute
                    if next_url:
                        if next_url.startswith('//'):
                            next_url = 'https:' + next_url
                        elif next_url.startswith('/'):
                            next_url = HINDILINKS_BASE_URL + next_url
                        elif not next_url.startswith(('http://', 'https://')):
                            next_url = HINDILINKS_BASE_URL + '/' + next_url.lstrip('/')
            
            logger.debug(f"Scraped {len(items)} items from {url}, next_url: {next_url}")
            return items, next_url
            
        except HTTPStatusError as e:
            status_code = e.response.status_code if e.response else 502
            logger.error(f"HTTP error {status_code} while scraping {url}: {e}")
            if status_code == 404:
                raise HTTPException(status_code=404, detail=f"Page not found: {url}")
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except RequestError as e:
            logger.error(f"Network error while scraping {url}: {e}")
            raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
        except HTTPException:
            raise  # Re-raise HTTPException as-is
        except Exception as e:
            logger.exception(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

# Function to scrape movies by search term
async def scrape_movie_data(search_term: str, client: AsyncClient, language: Optional[str] = None, max_pages: int = 3) -> List[Movie]:
    formatted_search_term = quote(search_term)
    url = f"{HINDILINKS_BASE_URL}/?s={formatted_search_term}"
    semaphore = asyncio.Semaphore(5)
    
    logger.info(f"Scraping movie search URL: {url}")
    
    movies_list = []
    current_url = url
    page_count = 0
    
    try:
        while current_url and page_count < max_pages:
            movies, next_url = await scrape_page(current_url, client, logger, semaphore, Movie)
            filtered_movies = filter_by_language(movies, language)
            movies_list.extend(filtered_movies)
            current_url = next_url
            page_count += 1
            logger.info(f"Scraped page {page_count} for search term '{search_term}': {len(filtered_movies)} movies found")
            if not next_url:
                break
            await asyncio.sleep(2)
        
        if not movies_list:
            logger.warning(f"No movies found for search term: {search_term}")
            raise HTTPException(status_code=404, detail=f"No movies found for search term: {search_term}")
        
        logger.info(f"Total {len(movies_list)} movies scraped for search term: {search_term}")
        return movies_list
    
    except HTTPStatusError as e:
        logger.error(f"HTTP error while scraping: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
    except RequestError as e:
        logger.error(f"Network error while scraping: {e}")
        raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error while scraping: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")
    
# Function to scrape a specific page for a year
async def scrape_movies_by_year_page(year: int, page: int, client: AsyncClient) -> List[Movie]:
    if not (1900 <= year <= 2025):
        raise HTTPException(status_code=400, detail="Year must be between 1900 and 2025")
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be a positive integer")
    
    url = f"{HINDILINKS_BASE_URL}/release-year/{year}/" if page == 1 else f"{HINDILINKS_BASE_URL}/release-year/{year}/page/{page}/"
    semaphore = asyncio.Semaphore(5)
    
    movies, _ = await scrape_page(url, client, logger, semaphore, Movie)
    if not movies:
        raise HTTPException(status_code=404, detail=f"No movies found for year {year} on page {page}")
    logger.info(f"Scraped {len(movies)} movies for year {year}, page {page}")
    return movies

# Function to scrape a specific page for a genre
async def scrape_movies_by_genre_page(genre: str, page: int, client: AsyncClient) -> List[Movie]:
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be a positive integer")
    
    url = f"{HINDILINKS_BASE_URL}/genre/{genre}/" if page == 1 else f"{HINDILINKS_BASE_URL}/genre/{genre}/page/{page}/"
    semaphore = asyncio.Semaphore(5)
    
    movies, _ = await scrape_page(url, client, logger, semaphore, Movie)
    if not movies:
        raise HTTPException(status_code=404, detail=f"No movies found for genre {genre} on page {page}")
    logger.info(f"Scraped {len(movies)} movies for genre {genre}, page {page}")
    return movies

# Function to scrape a specific page for a director
async def scrape_movies_by_director_page(director: str, page: int, client: AsyncClient) -> List[Movie]:
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be a positive integer")
    
    url = f"{HINDILINKS_BASE_URL}/director/{director}/" if page == 1 else f"{HINDILINKS_BASE_URL}/director/{director}/page/{page}/"
    semaphore = asyncio.Semaphore(5)
    
    movies, _ = await scrape_page(url, client, logger, semaphore, Movie)
    if not movies:
        raise HTTPException(status_code=404, detail=f"No movies found for director {director} on page {page}")
    logger.info(f"Scraped {len(movies)} movies for director {director}, page {page}")
    return movies

# Function to scrape a specific page for series
async def scrape_series_page(page: int, client: AsyncClient) -> List[Series]:
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be a positive integer")
    
    url = f"{HINDILINKS_BASE_URL}/series/" if page == 1 else f"{HINDILINKS_BASE_URL}/series/page/{page}/"
    semaphore = asyncio.Semaphore(5)
    
    series, _ = await scrape_page(url, client, logger, semaphore, Series)
    if not series:
        raise HTTPException(status_code=404, detail=f"No series found on page {page}")
    logger.info(f"Scraped {len(series)} series for page {page}")
    return series

async def scrape_series_search(search_series: str, client: AsyncClient) -> List[Series]:
    if not search_series.strip():
        raise HTTPException(status_code=400, detail="Search query cannot be empty")
    
    formatted_search_term = quote(search_series)
    url = f"{HINDILINKS_BASE_URL}/series/?s={formatted_search_term}"
    semaphore = asyncio.Semaphore(5)
    
    series, _ = await scrape_page(url, client, logger, semaphore, Series)
    if not series:
        raise HTTPException(status_code=404, detail=f"No series found for search query: {search_series}")
    logger.info(f"Scraped {len(series)} series for search query: {search_series}")
    return series

# Helper function to parse episode data
def parse_episode(soup: BeautifulSoup, logger, series_slug: str, season: int, episode: int) -> Episode:
    episode_data = {}

    title_tag = soup.find('h1', class_='entry-title') or soup.find('h1')
    episode_data['title'] = title_tag.text.strip() if title_tag else f"{series_slug.replace('-', ' ').title()} S{season:02d}E{episode:02d}"

    # Use the actual URL from the page if available, otherwise construct it
    current_url_tag = soup.find('link', rel='canonical') or soup.find('meta', property='og:url')
    if current_url_tag:
        episode_data['url'] = current_url_tag.get('href') or current_url_tag.get('content')
    else:
        episode_data['url'] = f"{HINDILINKS_BASE_URL}/episode/{series_slug}-season-{season}-episode-{episode}/"

    episode_data['series_title'] = series_slug.replace('-', ' ').title()
    series_title_tag = soup.find('a', href=lambda x: x and '/series/' in x)
    if series_title_tag:
        episode_data['series_title'] = series_title_tag.text.strip()

    episode_data['season'] = str(season)
    episode_data['episode_number'] = str(episode)

    desc_tag = soup.find('div', class_='entry-content') or soup.find('p', class_='f-desc')
    episode_data['description'] = desc_tag.text.strip() if desc_tag else None

    img_tag = soup.find('img', class_='lazy thumb') or soup.find('img', class_='poster')
    # Handle episode image URL
    image_url = None
    if img_tag:
        image_url = img_tag.get('data-original') or img_tag.get('src') or img_tag.get('data-src')
        if image_url:
            if image_url.startswith('//'):
                image_url = 'https:' + image_url
            elif image_url.startswith('/'):
                image_url = HINDILINKS_BASE_URL + image_url
            elif not image_url.startswith(('http://', 'https://')):
                image_url = HINDILINKS_BASE_URL + '/' + image_url.lstrip('/')
    episode_data['image'] = image_url

    streaming_links = []  # Legacy support
    servers = []  # New structured server list
    
    # Comprehensive video server extraction with metadata
    # Enhanced for new hindilinks4u.delivery structure with "Server 1", "Server 2" format
    # 1. Look for server selection buttons/links (most reliable for server names)
    server_selectors = [
        # New format: Look for elements with "Server 1", "Server 2" text
        ('div', {'class': lambda x: x and ('server' in " ".join(x).lower() if x else False)}),
        ('a', {'class': lambda x: x and ('server' in " ".join(x).lower() or 'link' in " ".join(x).lower() or 'watch' in " ".join(x).lower() or 'download' in " ".join(x).lower()) if x else False}),
        ('button', {'class': lambda x: x and ('server' in " ".join(x).lower() or 'play' in " ".join(x).lower()) if x else False}),
        ('div', {'class': lambda x: x and ('player-option' in " ".join(x).lower()) if x else False}),
        ('a', {'class': 'link'}),
        ('a', {'class': 'watch-link'}),
        ('a', {'class': 'stream-link'}),
        ('div', {'class': 'player-options'}),
    ]
    
    server_counter = 1
    processed_urls = set()
    
    # Enhanced: Look for server structure in new format
    # Pattern: "Server 1" / "Server 2" with quality info (HD 1080p, HD 720p)
    server_sections = soup.find_all(['div', 'section', 'article'], class_=lambda x: x and ('server' in " ".join(x).lower() if x else False))
    for section in server_sections:
        # Look for server number/name
        section_text = section.get_text()
        server_match = re.search(r'Server\s*(\d+)', section_text, re.IGNORECASE)
        server_name = f"Server {server_match.group(1)}" if server_match else f"Server {server_counter}"
        
        # Look for quality info
        quality_match = re.search(r'(HD|SD|FHD|UHD|4K)?\s*(\d+p|720p|1080p|480p|360p)', section_text, re.IGNORECASE)
        quality = quality_match.group(0).strip() if quality_match else None
        
        # Find download/watch links in this section
        section_links = section.find_all('a', href=True)
        for link in section_links:
            href = link.get('href')
            link_text = link.get_text().strip().lower()
            
            # Check if it's a download/watch link
            if href and any(keyword in link_text for keyword in ['download', 'watch', 'stream', 'play', 'hd', '720p', '1080p']):
                if href.startswith('//'):
                    href = 'https:' + href
                elif href.startswith('/'):
                    href = HINDILINKS_BASE_URL + href
                elif not href.startswith(('http://', 'https://')):
                    continue
                
                # Filter out unwanted links
                exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram']
                if any(exclude in href.lower() for exclude in exclude_keywords):
                    continue
                
                if href not in processed_urls:
                    processed_urls.add(href)
                    streaming_links.append(href)
                    servers.append(StreamingServer(
                        name=server_name,
                        url=href,
                        quality=quality or extract_quality(link_text, href),
                        type=get_server_type(href)
                    ))
                    server_counter += 1
    
    # Original method: Look through selectors
    for tag_name, attrs in server_selectors:
        link_tags = soup.find_all(tag_name, attrs)
        if link_tags:
            for link in link_tags:
                href = link.get('href') or link.get('data-url') or link.get('data-link')
                if not href and link.find('a'):
                    href = link.find('a').get('href') or link.find('a').get('data-url')
                if href:
                    # Handle relative URLs
                    if href.startswith('//'):
                        href = 'https:' + href
                    elif href.startswith('/'):
                        href = HINDILINKS_BASE_URL + href
                    elif not href.startswith(('http://', 'https://')):
                        continue
                    
                    # Filter out unwanted links and non-video files
                    exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram', 'youtube.com/channel']
                    if any(exclude in href.lower() for exclude in exclude_keywords):
                        continue
                    
                    # Filter out non-video file extensions
                    non_video_extensions = ['.css', '.js', '.json', '.xml', '.txt', '.pdf', '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.woff', '.ttf']
                    if any(ext in href.lower() for ext in non_video_extensions):
                        continue
                    
                    # Filter out non-video paths
                    non_video_paths = ['/wp-content/', '/wp-includes/', '/themes/', '/plugins/', '/assets/', '/static/', '/css/', '/js/', '/fonts/', '/images/']
                    if any(path in href.lower() for path in non_video_paths):
                        continue
                    
                    # Check if it's a valid streaming link
                    streaming_domains = ['stream', 'embed', 'player', 'watch', 'video', 'play', 'server', 'cdn', 'mp4', 'm3u8']
                    is_streaming = any(domain in href.lower() for domain in streaming_domains) or 'episode' in href.lower() or 'movie' in href.lower()
                    
                    if is_streaming and href not in processed_urls:
                        processed_urls.add(href)
                        streaming_links.append(href)  # Legacy
                        
                        # Extract server metadata
                        server_name = extract_server_name(link, f"Server {server_counter}")
                        quality = extract_quality(link.text if hasattr(link, 'text') else "", href)
                        server_type = get_server_type(href)
                        
                        servers.append(StreamingServer(
                            name=server_name,
                            url=href,
                            quality=quality,
                            type=server_type
                        ))
                        server_counter += 1
    
    # 2. Look for player containers and iframes
    player_containers = soup.find_all(["div", "section"], class_=lambda x: x and ("player" in " ".join(x).lower() or "video" in " ".join(x).lower() or "embed" in " ".join(x).lower()) if x else False)
    for container in player_containers:
        # Check for iframe sources
        iframes = container.find_all("iframe")
        for iframe in iframes:
            src = iframe.get("src") or iframe.get("data-src")
            if src and src.startswith(('http://', 'https://')):
                if src.startswith('//'):
                    src = 'https:' + src
                if src not in processed_urls:
                    processed_urls.add(src)
                    streaming_links.append(src)  # Legacy
                    servers.append(StreamingServer(
                        name=f"Embed {server_counter}",
                        url=src,
                        quality=extract_quality("", src),
                        type=get_server_type(src)
                    ))
                    server_counter += 1
        
        # Check for video sources
        videos = container.find_all("video")
        for video in videos:
            src = video.get("src")
            if src and src.startswith(('http://', 'https://')):
                if src not in processed_urls:
                    processed_urls.add(src)
                    streaming_links.append(src)  # Legacy
                    servers.append(StreamingServer(
                        name=f"Direct {server_counter}",
                        url=src,
                        quality=extract_quality("", src),
                        type="direct"
                    ))
                    server_counter += 1
            
            source_tags = video.find_all("source")
            for source in source_tags:
                src = source.get("src")
                if src and src.startswith(('http://', 'https://')):
                    if src not in processed_urls:
                        processed_urls.add(src)
                        streaming_links.append(src)  # Legacy
                        quality = source.get('label') or extract_quality("", src)
                        servers.append(StreamingServer(
                            name=f"Direct {server_counter}",
                            url=src,
                            quality=quality,
                            type="direct"
                        ))
                        server_counter += 1
    
    # 3. Look for data attributes with video URLs
    data_attrs = ['data-url', 'data-src', 'data-link', 'data-video', 'data-embed']
    for attr in data_attrs:
        elements = soup.find_all(attrs={attr: True})
        for elem in elements:
            data_url = elem.get(attr)
            if data_url and data_url.startswith(('http://', 'https://')):
                data_url_lower = str(data_url).lower()
                # Filter out non-video files
                non_video_extensions = ['.css', '.js', '.json', '.xml', '.txt', '.pdf', '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico']
                if any(ext in data_url_lower for ext in non_video_extensions):
                    continue
                # Filter out non-video paths
                non_video_paths = ['/wp-content/', '/wp-includes/', '/themes/', '/plugins/', '/assets/', '/static/', '/css/', '/js/', '/fonts/', '/images/']
                if any(path in data_url_lower for path in non_video_paths):
                    continue
                # Filter out unwanted domains
                exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram', 'analytics', 'tracking']
                if any(exclude in data_url_lower for exclude in exclude_keywords):
                    continue
                if data_url not in processed_urls:
                    processed_urls.add(data_url)
                    streaming_links.append(data_url)  # Legacy
                    servers.append(StreamingServer(
                        name=f"Data {server_counter}",
                        url=data_url,
                        quality=extract_quality("", data_url),
                        type=get_server_type(data_url)
                    ))
                    server_counter += 1
    
    # 4. Extract from script tags (JSON data, embedded URLs) - Enhanced
    script_tags = soup.find_all("script")
    for script in script_tags:
        script_text = script.string or ""
        if not script_text:
            continue
        
        # Enhanced URL patterns for video links
        url_patterns = [
            r'["\'](https?://[^"\']*(?:stream|embed|player|watch|video|play|server|cdn|mp4|m3u8|webm|mkv|flv)[^"\']*)["\']',
            r'url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'src["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'file["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'video["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'source["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'fileurl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'video_url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'videoUrl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            r'getElementById\(["\']([^"\']+)["\']\)\.src\s*=\s*["\'](https?://[^"\']+)["\']',
            r'\.setAttribute\(["\']src["\'],\s*["\'](https?://[^"\']+)["\']',
            r'iframe\.src\s*=\s*["\'](https?://[^"\']+)["\']',
        ]
        
        for pattern in url_patterns:
            matches = re.findall(pattern, script_text, re.IGNORECASE)
            for match in matches:
                # Handle tuple matches
                url = match[1] if isinstance(match, tuple) and len(match) > 1 else match
                if isinstance(match, tuple):
                    url = match[1] if len(match) > 1 else match[0]
                else:
                    url = match
                
                if url and url.startswith(('http://', 'https://')):
                    exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram', 'analytics', 'tracking', 'google-analytics']
                    if not any(exclude in url.lower() for exclude in exclude_keywords):
                        # Check if it looks like a video URL
                        video_indicators = ['stream', 'embed', 'player', 'watch', 'video', 'play', 'server', 'cdn', 'mp4', 'm3u8', 'webm', 'mkv', 'dood', 'streamtape', 'mixdrop']
                        if any(indicator in url.lower() for indicator in video_indicators):
                            if url not in processed_urls:
                                processed_urls.add(url)
                                streaming_links.append(url)  # Legacy
                                servers.append(StreamingServer(
                                    name=f"Script {server_counter}",
                                    url=url,
                                    quality=extract_quality("", url),
                                    type=get_server_type(url)
                                ))
                                server_counter += 1
    
    # 5. Look for server tabs/buttons in common player structures
    server_tabs = soup.find_all(['div', 'li', 'button', 'a'], class_=lambda x: x and any(keyword in " ".join(x).lower() for keyword in ['server', 'tab', 'option', 'link', 'quality']) if x else False)
    for tab in server_tabs:
        # Check for data attributes
        for attr in ['data-url', 'data-src', 'data-link', 'data-video', 'data-embed', 'data-server']:
            data_url = tab.get(attr)
            if data_url:
                if data_url.startswith('//'):
                    data_url = 'https:' + data_url
                elif data_url.startswith('/'):
                    data_url = HINDILINKS_BASE_URL + data_url
                elif data_url.startswith(('http://', 'https://')):
                    if data_url not in processed_urls:
                        processed_urls.add(data_url)
                        streaming_links.append(data_url)  # Legacy
                        servers.append(StreamingServer(
                            name=extract_server_name(tab, f"Server {server_counter}"),
                            url=data_url,
                            quality=extract_quality(tab.text if hasattr(tab, 'text') else "", data_url),
                            type=get_server_type(data_url)
                        ))
                        server_counter += 1
        
        # Check for nested links
        nested_link = tab.find('a', href=True)
        if nested_link:
            href = nested_link.get('href')
            if href:
                if href.startswith('//'):
                    href = 'https:' + href
                elif href.startswith('/'):
                    href = HINDILINKS_BASE_URL + href
                elif href.startswith(('http://', 'https://')):
                    if href not in processed_urls:
                        processed_urls.add(href)
                        streaming_links.append(href)  # Legacy
                        servers.append(StreamingServer(
                            name=extract_server_name(nested_link, f"Server {server_counter}"),
                            url=href,
                            quality=extract_quality(nested_link.text if nested_link.text else "", href),
                            type=get_server_type(href)
                        ))
                        server_counter += 1
    
    # 6. Fallback: search for any links with streaming keywords (enhanced)
    if len(servers) < 3:  # Only if we haven't found many servers yet
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href')
            text = link.text.lower() if link.text else ''
            if href:
                if href.startswith('//'):
                    href = 'https:' + href
                elif href.startswith('/'):
                    href = HINDILINKS_BASE_URL + href
                elif not href.startswith(('http://', 'https://')):
                    continue
                    
                # Enhanced keyword matching
                video_keywords = ['watch', 'stream', 'play', 'download', 'server', 'embed', 'player', 'video', 'mp4', 'm3u8', 'dood', 'streamtape', 'mixdrop', 'vidstream', 'gounlimited']
                if any(keyword in text or keyword in href.lower() for keyword in video_keywords):
                    exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram', 'analytics', 'tracking']
                    if not any(exclude in href.lower() for exclude in exclude_keywords):
                        if href not in processed_urls:
                            processed_urls.add(href)
                            streaming_links.append(href)  # Legacy
                            servers.append(StreamingServer(
                                name=extract_server_name(link, f"Server {server_counter}"),
                                url=href,
                                quality=extract_quality(text, href),
                                type=get_server_type(href)
                            ))
                            server_counter += 1
    
    episode_data['streaming_links'] = streaming_links  # Legacy support
    episode_data['servers'] = servers  # New structured format

    duration_tag = soup.find('div', class_='jt-info', string=lambda x: x and 'min' in x.lower() and x.strip().replace('min', '').strip().isdigit())
    episode_data['duration'] = duration_tag.text.strip() if duration_tag else None

    # Extract language with improved detection
    language = None
    sources = [
        (desc_tag, desc_tag.text if desc_tag else None),
        (title_tag, title_tag.text if title_tag else None),
        (soup.find('meta', property='og:description'), lambda tag: tag.get('content') if tag else None),
        (soup.find('div', class_='category'), lambda tag: tag.text if tag else None),
    ]
    for tag, text_or_func in sources:
        text = text_or_func(tag) if callable(text_or_func) else text_or_func
        if text:
            for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam']:
                if lang in text.lower():
                    language = lang.lower()
                    break
        if language:
            break

    # Fallback to 'english' if no language is detected
    if not language:
        logger.info(f"No language detected for {series_slug} S{season:02d}E{episode:02d}, defaulting to 'english'")
        language = 'english'

    episode_data['language'] = language

    if not episode_data['title'] or not episode_data['url']:
        logger.warning(f"Failed to parse episode data for {series_slug} S{season:02d}E{episode:02d}")
        raise HTTPException(status_code=404, detail=f"Episode not found for {series_slug} season {season} episode {episode}")

    return Episode(**episode_data)

# Function to scrape a specific episode page
async def scrape_episode_data(
    series_slug: str, season: int, episode: int, client: AsyncClient, language: Optional[str] = None
) -> Episode:
    if season < 1 or episode < 1:
        raise HTTPException(status_code=400, detail="Season and episode must be positive integers")

    # Try multiple URL patterns for the new domain structure
    # Pattern 1: Standard format: /episode/{series}-season-{s}-episode-{e}/
    # Pattern 2: New format with part: /{series}-{year}-season-{s}-part-{e}-{lang}-dubbed-Watch-online-full-movie/
    # Pattern 3: New format episode: /{series}-{year}-season-{s}-episode-{e}-{lang}-dubbed-Watch-online-full-movie/
    
    url_patterns = [
        f"{HINDILINKS_BASE_URL}/episode/{series_slug}-season-{season}-episode-{episode}/",
        f"{HINDILINKS_BASE_URL}/{series_slug}-season-{season}-episode-{episode}/",
        f"{HINDILINKS_BASE_URL}/{series_slug}-season-{season}-part-{episode}-hindi-dubbed-Watch-online-full-movie/",
        f"{HINDILINKS_BASE_URL}/{series_slug}-season-{season}-episode-{episode}-hindi-dubbed-Watch-online-full-movie/",
    ]
    
    # Add language-specific patterns if language is provided
    if language:
        lang_slug = language.lower()
        url_patterns.extend([
            f"{HINDILINKS_BASE_URL}/{series_slug}-season-{season}-part-{episode}-{lang_slug}-dubbed-Watch-online-full-movie/",
            f"{HINDILINKS_BASE_URL}/{series_slug}-season-{season}-episode-{episode}-{lang_slug}-dubbed-Watch-online-full-movie/",
        ])
    
    semaphore = asyncio.Semaphore(5)
    last_error = None

    async with semaphore:
        for url in url_patterns:
            try:
                logger.info(f"Trying episode URL: {url}")
                await asyncio.sleep(1)
                response = await client.get(url, follow_redirects=True)
                
                # Check if we got redirected to a different domain
                final_url = str(response.url)
                if 'hindilinks4u.delivery' in final_url and 'hindilinks4u.host' in url:
                    # Update base URL if we got redirected
                    logger.info(f"Detected redirect to new domain: {final_url}")
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # Check if page actually has content (not a 404 page)
                    title_tag = soup.find('h1') or soup.find('title')
                    if title_tag and '404' not in title_tag.text.lower() and 'not found' not in title_tag.text.lower():
                        episode_data = parse_episode(soup, logger, series_slug, season, episode)
                        break
                elif response.status_code == 404:
                    continue  # Try next pattern
                else:
                    response.raise_for_status()
            except HTTPStatusError as e:
                if e.response.status_code == 404:
                    last_error = e
                    continue  # Try next pattern
                last_error = e
            except Exception as e:
                last_error = e
                continue
        
        # If we got here without breaking, all patterns failed
        if 'episode_data' not in locals():
            if last_error:
                if isinstance(last_error, HTTPStatusError) and last_error.response.status_code == 404:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Episode not found for {series_slug} season {season} episode {episode}"
                    )
                raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(last_error)}")
            raise HTTPException(
                status_code=404,
                detail=f"Episode not found for {series_slug} season {season} episode {episode}"
            )
        
        try:

            # Deep resolve embed URLs to direct playable links
            resolved_servers = []
            for server in episode_data.servers:
                server_url_str = str(server.url)
                if server.type in ['embed', 'iframe'] or '/embed/' in server_url_str.lower() or 'embed' in server_url_str.lower():
                    # Try to resolve embed URL to direct link
                    resolved_url = await resolve_embed_to_direct(client, server_url_str, max_depth=2)
                    if resolved_url and resolved_url != server_url_str:
                        # Use resolved URL if different
                        resolved_servers.append(StreamingServer(
                            name=server.name,
                            url=resolved_url,
                            quality=server.quality,
                            type="direct" if resolved_url else server.type
                        ))
                    else:
                        # Keep original if resolution failed or same
                        resolved_servers.append(server)
                else:
                    # Keep non-embed links as-is
                    resolved_servers.append(server)
            
            # Update episode data with resolved servers
            episode_data.servers = resolved_servers
            # Also update legacy streaming_links with resolved URLs
            episode_data.streaming_links = [str(server.url) for server in resolved_servers]

            # Apply language filter
            if language:
                if not episode_data.language or language.lower() not in episode_data.language.lower():
                    logger.warning(
                        f"Episode {series_slug} S{season:02d}E{episode:02d} language ({episode_data.language}) "
                        f"does not match requested language ({language})"
                    )
                    raise HTTPException(
                        status_code=404,
                        detail=f"Episode not available in {language} for {series_slug} season {season} episode {episode}"
                    )
                
            logger.info(f"Scraped episode data for {series_slug} S{season:02d}E{episode:02d}, language: {episode_data.language}")
            return episode_data
        except HTTPStatusError as e:
            logger.error(f"HTTP error while scraping {url}: {e}")
            if e.response.status_code == 404:
                raise HTTPException(
                    status_code=404,
                    detail=f"Episode not found for {series_slug} season {season} episode {episode}"
                )
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except RequestError as e:
            logger.error(f"Network error while scraping {url}: {e}")
            raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

# Helper function to parse series detail
def parse_series_detail(soup: BeautifulSoup, logger, series_slug: str, original_title: Optional[str] = None) -> SeriesDetail:
    # 1. Metadata extraction
    title_tag = soup.find('h1', class_='entry-title') or soup.find('h1')
    title = title_tag.text.strip() if title_tag else (original_title or series_slug.replace('-', ' ').title())
    
    # Extract ID from body class if available (for data-id usage generally, but mainly url here)
    url = f"{HINDILINKS_BASE_URL}/series/{series_slug}/"
    
    desc_tag = soup.find('div', class_='entry-content') or soup.find('div', class_='description') or soup.find('p', class_='f-desc')
    description = desc_tag.text.strip() if desc_tag else None
    
    img_tag = soup.find('img', class_='lazy thumb') or soup.find('img', class_='poster')
    image_url = None
    if img_tag:
        image_url = img_tag.get('data-original') or img_tag.get('src') or img_tag.get('data-src')
        if image_url:
            if image_url.startswith('//'):
                image_url = 'https:' + image_url
            elif image_url.startswith('/'):
                image_url = HINDILINKS_BASE_URL + image_url
            elif not image_url.startswith(('http://', 'https://')):
                image_url = HINDILINKS_BASE_URL + '/' + image_url.lstrip('/')
    
    # Extra info
    extra_info = soup.find('div', class_='sheader') or soup.find('div', class_='extra')
    year = None
    duration = None
    imdb = None
    
    if extra_info:
        # Try to find year
        year_tag = extra_info.find('span', class_='date') or extra_info.find('a', href=lambda x: x and 'release-year' in x)
        if year_tag:
            year = year_tag.text.strip()
            
        # Try to find runtime/duration
        duration_tag = extra_info.find('span', class_='runtime')
        if duration_tag:
            duration = duration_tag.text.strip()
            
        # Try to find country (language proxy sometimes)
        # country_tag = extra_info.find('span', class_='country')
    
    # Genres
    genres = []
    genre_tags = soup.find_all('a', href=lambda x: x and 'genre' in x)
    for tag in genre_tags:
        genres.append(tag.text.strip())
        
    # Cast (simple extraction)
    cast = []
    cast_tags = soup.find_all('a', href=lambda x: x and 'cast' in x) # hypothetical
    for tag in cast_tags:
        cast.append(tag.text.strip())

    # 2. Episodes Extraction - Enhanced to automatically detect all episodes
    episodes = []
    seasons_count = 0
    total_episodes_count = 0
    season_episode_map = {}  # Track episodes per season
    
    # Method 1: Look for season containers (primary method)
    seasons_divs = soup.find_all('div', class_='se-c')
    if not seasons_divs:
        # Fallback for some layouts without 'se-c' container
        seasons_divs = soup.find_all('div', id=lambda x: x and x.startswith('season-'))
    if not seasons_divs:
        # Additional fallback: look for divs with season-related classes
        seasons_divs = soup.find_all('div', class_=lambda x: x and ('season' in " ".join(x).lower() or 'episode' in " ".join(x).lower()) if x else False)
    
    for season_div in seasons_divs:
        seasons_count += 1
        
        # Extract season number from various sources
        season_num = "1"  # Default
        season_num_tag = season_div.find('span', class_='se-t') or season_div.find('div', class_='se-q') or season_div.find('h3') or season_div.find('h4')
        if season_num_tag:
            txt = season_num_tag.text.strip()
            # Extract digits - look for "Season 1", "S1", "1", etc.
            season_match = re.search(r'(?:season|s)[\s:]*(\d+)', txt, re.IGNORECASE)
            if season_match:
                season_num = season_match.group(1)
            else:
                digits = re.findall(r'\d+', txt)
                if digits:
                    season_num = digits[0]
        
        # Also try to extract from div ID or class
        div_id = season_div.get('id', '')
        if div_id:
            season_match = re.search(r'season[_-]?(\d+)', div_id, re.IGNORECASE)
            if season_match:
                season_num = season_match.group(1)
        
        # Initialize season episode counter
        if season_num not in season_episode_map:
            season_episode_map[season_num] = 0
        
        # Find episode items - try multiple selectors
        episode_list = season_div.find_all('li')
        if not episode_list:
            # Try finding episode links directly
            episode_list = season_div.find_all('a', href=lambda x: x and 'episode' in x.lower() if x else False)
            # Convert links to list items for processing
            episode_list = [{'tag': 'a', 'link': link, 'parent': link.find_parent('li') or link.find_parent('div')} for link in episode_list]
        
        for ep_item in episode_list:
            # Handle both li elements and direct links
            if isinstance(ep_item, dict):
                ep_link = ep_item.get('link')
                ep = ep_item.get('parent') or ep_link
            else:
                ep = ep_item
                ep_link = ep.find('a')
            
            if not ep_link:
                continue
            
            # Get episode URL
            ep_url = ep_link.get('href') if hasattr(ep_link, 'get') else str(ep_link)
            if not ep_url:
                continue
            
            # Normalize URL
            if ep_url.startswith('//'):
                ep_url = 'https:' + ep_url
            elif ep_url.startswith('/'):
                ep_url = HINDILINKS_BASE_URL + ep_url
            elif not ep_url.startswith(('http://', 'https://')):
                ep_url = HINDILINKS_BASE_URL + '/' + ep_url.lstrip('/')
            
            # Extract episode title
            ep_title = ep_link.text.strip() if hasattr(ep_link, 'text') else (ep.text.strip() if hasattr(ep, 'text') else f"Episode {season_episode_map[season_num] + 1}")
            
            # Extract episode number from URL (most reliable)
            ep_num = None
            try:
                # URL format: .../episode/{series}-season-{s}-episode-{e}/
                if 'season-' in ep_url and 'episode-' in ep_url:
                    # Extract season and episode from URL
                    url_parts = ep_url.split('/')
                    for part in url_parts:
                        if 'episode-' in part:
                            ep_match = re.search(r'episode-(\d+)', part, re.IGNORECASE)
                            if ep_match:
                                ep_num = ep_match.group(1)
                                break
                
                # Fallback: extract from title
                if not ep_num:
                    ep_match = re.search(r'(?:episode|ep)[\s:]*(\d+)', ep_title, re.IGNORECASE)
                    if ep_match:
                        ep_num = ep_match.group(1)
                
                # Final fallback: use counter
                if not ep_num:
                    season_episode_map[season_num] += 1
                    ep_num = str(season_episode_map[season_num])
                else:
                    # Update counter to match found episode number
                    ep_num_int = int(ep_num)
                    if ep_num_int > season_episode_map[season_num]:
                        season_episode_map[season_num] = ep_num_int
            except Exception as e:
                logger.debug(f"Error extracting episode number: {e}")
                season_episode_map[season_num] += 1
                ep_num = str(season_episode_map[season_num])
            
            # Extract episode image
            ep_image_url = None
            if isinstance(ep, dict):
                ep_img_tag = (ep.get('parent') or ep.get('link')).find('img') if hasattr(ep.get('parent') or ep.get('link'), 'find') else None
            else:
                ep_img_tag = ep.find('img')
            
            if ep_img_tag:
                ep_image_url = ep_img_tag.get('src') or ep_img_tag.get('data-src') or ep_img_tag.get('data-original')
                if ep_image_url:
                    if ep_image_url.startswith('//'):
                        ep_image_url = 'https:' + ep_image_url
                    elif ep_image_url.startswith('/'):
                        ep_image_url = HINDILINKS_BASE_URL + ep_image_url
                    elif not ep_image_url.startswith(('http://', 'https://')):
                        ep_image_url = HINDILINKS_BASE_URL + '/' + ep_image_url.lstrip('/')
            
            total_episodes_count += 1
            episodes.append(SeriesEpisode(
                title=ep_title,
                url=ep_url,
                season=season_num,
                episode_number=ep_num,
                image=ep_image_url,
                duration=None,  # Usually not visible in list
                language=None 
            ))
    
    # Method 2: If no episodes found via season containers, try finding all episode links on page
    if not episodes:
        logger.info(f"No episodes found via season containers, trying alternative method for {series_slug}")
        all_episode_links = soup.find_all('a', href=lambda x: x and 'episode' in x.lower() and series_slug in x.lower() if x else False)
        
        # Group by season based on URL pattern
        for ep_link in all_episode_links:
            ep_url = ep_link.get('href')
            if not ep_url:
                continue
            
            # Normalize URL
            if ep_url.startswith('//'):
                ep_url = 'https:' + ep_url
            elif ep_url.startswith('/'):
                ep_url = HINDILINKS_BASE_URL + ep_url
            elif not ep_url.startswith(('http://', 'https://')):
                ep_url = HINDILINKS_BASE_URL + '/' + ep_url.lstrip('/')
            
            # Extract season and episode from URL
            season_match = re.search(r'season-(\d+)', ep_url, re.IGNORECASE)
            episode_match = re.search(r'episode-(\d+)', ep_url, re.IGNORECASE)
            
            if season_match and episode_match:
                season_num = season_match.group(1)
                ep_num = episode_match.group(1)
                ep_title = ep_link.text.strip() or f"Episode {ep_num}"
                
                # Extract image
                ep_image_url = None
                parent = ep_link.find_parent(['li', 'div', 'article'])
                if parent:
                    img_tag = parent.find('img')
                    if img_tag:
                        ep_image_url = img_tag.get('src') or img_tag.get('data-src')
                        if ep_image_url:
                            if ep_image_url.startswith('//'):
                                ep_image_url = 'https:' + ep_image_url
                            elif ep_image_url.startswith('/'):
                                ep_image_url = HINDILINKS_BASE_URL + ep_image_url
                
                episodes.append(SeriesEpisode(
                    title=ep_title,
                    url=ep_url,
                    season=season_num,
                    episode_number=ep_num,
                    image=ep_image_url,
                    duration=None,
                    language=None
                ))
        
        # Update seasons count
        if episodes:
            unique_seasons = set(ep.season for ep in episodes)
            seasons_count = len(unique_seasons)
    
    # Sort episodes by season and episode number
    episodes.sort(key=lambda x: (int(x.season) if x.season.isdigit() else 0, int(x.episode_number) if x.episode_number.isdigit() else 0))
            
    # Language detection
    language = None
    for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam']:
        if lang in title.lower() or (description and lang in description.lower()):
            language = lang
            break
            
    languages = [language] if language else []

    return SeriesDetail(
        title=title,
        url=url,
        image=image_url,
        description=description,
        genres=list(set(genres)),
        languages=languages,
        year=year,
        imdb=imdb,
        quality=None,
        duration=duration,
        seasons=str(seasons_count),
        episodes_count=str(total_episodes_count),
        cast=cast,
        episodes=episodes
    )

# Function to scrape series detail
async def scrape_series_detail(series_slug: str, client: AsyncClient, include_servers: bool = False) -> SeriesDetail:
    """
    Scrape series detail page with optional server links for episodes.
    
    Args:
        series_slug: Series slug identifier
        client: Async HTTP client
        include_servers: If True, fetch server links for each episode (slower but complete)
    
    Returns:
        SeriesDetail object with episode information
    """
    url = f"{HINDILINKS_BASE_URL}/series/{series_slug}/"
    semaphore = asyncio.Semaphore(5)
    
    async with semaphore:
        logger.info(f"Scraping series detail URL: {url}")
        try:
            response = await client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            series_detail = parse_series_detail(soup, logger, series_slug)
            
            # Optionally populate server links for each episode
            if include_servers and series_detail.episodes:
                logger.info(f"Fetching server links for {len(series_detail.episodes)} episodes...")
                
                async def fetch_episode_servers(ep: SeriesEpisode) -> SeriesEpisode:
                    """Fetch server links for a single episode"""
                    try:
                        # Extract season and episode numbers from URL
                        # URL format: .../episode/{series}-season-{s}-episode-{e}/
                        url_str = str(ep.url)
                        season_match = re.search(r'season-(\d+)', url_str)
                        episode_match = re.search(r'episode-(\d+)', url_str)
                        
                        if season_match and episode_match:
                            season_num = int(season_match.group(1))
                            episode_num = int(episode_match.group(1))
                            
                            # Fetch episode data to get server links
                            episode_data = await scrape_episode_data(
                                series_slug, 
                                season_num, 
                                episode_num, 
                                client, 
                                language=None
                            )
                            
                            # Update episode with server links
                            return SeriesEpisode(
                                title=ep.title,
                                url=ep.url,
                                season=ep.season,
                                episode_number=ep.episode_number,
                                image=ep.image,
                                duration=ep.duration,
                                language=ep.language,
                                servers=episode_data.servers
                            )
                    except Exception as e:
                        logger.warning(f"Failed to fetch servers for episode {ep.episode_number}: {e}")
                        # Return original episode without servers
                    
                    return ep
                
                # Fetch all episodes concurrently (limit concurrency)
                semaphore_episodes = asyncio.Semaphore(10)  # Limit concurrent episode fetches
                
                async def fetch_with_limit(ep: SeriesEpisode) -> SeriesEpisode:
                    async with semaphore_episodes:
                        return await fetch_episode_servers(ep)
                
                # Fetch all episodes
                updated_episodes = await asyncio.gather(*[fetch_with_limit(ep) for ep in series_detail.episodes])
                series_detail.episodes = updated_episodes
                
                logger.info(f"Successfully fetched server links for episodes")
            
            return series_detail
            
        except HTTPStatusError as e:
            logger.error(f"HTTP error while scraping {url}: {e}")
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail="Series not found")
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

# Async version for consistency with existing codebase
async def scrape_anime_data(search_term: str, client: AsyncClient) -> List[Anime]:
    base_url = TOONSTREAM_BASE_URL.rstrip('/')
    formatted_search_term = quote(search_term)
    url = f"{base_url}/home/?s={formatted_search_term}"
    
    logger.info(f"Scraping anime search URL: {url}")
    
    try:
        response = await client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Try multiple selectors to find anime items
        articles = []
        
        # Try the original selector first
        articles = soup.find_all("article", class_="post dfx fcl movies")
        
        # If not found, try alternative selectors
        if not articles:
            # Try finding articles with class containing "post" or "movies"
            articles = soup.find_all("article", class_=lambda x: x and ("post" in " ".join(x) or "movies" in " ".join(x)))
        
        if not articles:
            # Try finding by div with similar classes
            articles = soup.find_all("div", class_=lambda x: x and ("post" in " ".join(x) or "movie" in " ".join(x) or "anime" in " ".join(x)))
        
        if not articles:
            # Try finding items in a list container
            list_container = soup.find("div", class_=lambda x: x and ("list" in " ".join(x).lower() or "grid" in " ".join(x).lower()))
            if list_container:
                articles = list_container.find_all(["article", "div"], recursive=True)
        
        if not articles:
            # Last resort: try to find any article or div with title and link
            all_items = soup.find_all(["article", "div"], class_=True)
            articles = [item for item in all_items if item.find("h2") and item.find("a")]
        
        if not articles:
            logger.warning(f"No anime found for search term: {search_term}")
            # Log the page structure for debugging
            logger.debug(f"Page content preview: {soup.prettify()[:1000]}")
            raise HTTPException(status_code=404, detail=f"No anime found for search term: {search_term}")
        
        anime_list = []
        for article in articles:
            try:
                # Try multiple ways to find title
                title = None
                title_tag = article.find("h2", class_="entry-title") or article.find("h2") or article.find("h3")
                if title_tag:
                    title = title_tag.text.strip()
                else:
                    # Try finding title in link text
                    link_tag = article.find("a")
                    if link_tag:
                        title = link_tag.get("title") or link_tag.get("alt") or link_tag.text.strip()
                
                if not title:
                    logger.debug("Skipping item: no title found")
                    continue
                
                # Try multiple ways to find TMDB rating
                tmdb_rating = None
                rating_tag = article.find("span", class_="vote") or article.find("span", class_=lambda x: x and "vote" in " ".join(x))
                if rating_tag:
                    rating_text = rating_tag.text.replace("TMDB", "").strip()
                    try:
                        tmdb_rating = float(rating_text)
                    except ValueError:
                        # Try to extract number from text
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        if rating_match:
                            tmdb_rating = float(rating_match.group(1))
                
                # Handle image URL
                image_url = None
                img_tag = article.find("img")
                if img_tag:
                    image_url = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original") or img_tag.get("data-lazy-src")
                    if image_url:
                        if image_url.startswith("//"):
                            image_url = "https:" + image_url
                        elif image_url.startswith("/"):
                            image_url = base_url + image_url
                        elif not image_url.startswith(('http://', 'https://')):
                            image_url = base_url + '/' + image_url.lstrip('/')
                
                # Handle series URL - try multiple selectors
                series_url = None
                link_tag = article.find("a", class_="lnk-blk") or article.find("a", href=True)
                if link_tag:
                    series_url = link_tag.get("href")
                    if series_url:
                        if series_url.startswith("//"):
                            series_url = "https:" + series_url
                        elif series_url.startswith("/"):
                            series_url = base_url + series_url
                        elif not series_url.startswith(('http://', 'https://')):
                            series_url = base_url + '/' + series_url.lstrip('/')
                
                # If no URL found, skip this item
                if not series_url:
                    logger.debug(f"Skipping item '{title}': no URL found")
                    continue
                
                # Basic language detection
                language = None
                for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam']:
                    if lang in title.lower():
                        language = lang
                        break
                
                # Try to extract description if available
                description = None
                desc_tag = article.find("p", class_=lambda x: x and "desc" in " ".join(x).lower()) or article.find("div", class_=lambda x: x and "excerpt" in " ".join(x).lower())
                if desc_tag:
                    description = desc_tag.text.strip()
                
                # Try to extract genres
                genres = []
                genre_tags = article.find_all("a", href=lambda x: x and "genre" in x.lower() if x else False)
                if genre_tags:
                    genres = [tag.text.strip() for tag in genre_tags]
                
                anime_data = {
                    "title": title,
                    "url": series_url,
                    "image": image_url,
                    "rating": str(tmdb_rating) if tmdb_rating else None,
                    "language": language,
                    "description": description,
                    "genres": genres,
                    "year": None
                }
                
                anime_list.append(Anime(**anime_data))
            except Exception as e:
                logger.error(f"Failed to parse anime item: {e}", exc_info=True)
                continue
        
        if not anime_list:
            logger.warning(f"Could not parse any anime items from the page for search term: {search_term}")
            raise HTTPException(status_code=404, detail=f"No anime found for search term: {search_term}")
        
        logger.info(f"Scraped {len(anime_list)} anime for search term: {search_term}")
        return anime_list
    
    except HTTPStatusError as e:
        status_code = e.response.status_code if e.response else 502
        logger.error(f"HTTP error {status_code} while scraping {url}: {e}")
        if status_code == 404:
            raise HTTPException(status_code=404, detail=f"Anime not found for search term: {search_term}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
    except RequestError as e:
        logger.error(f"Network error while scraping {url}: {e}")
        raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
    except HTTPException:
        raise  # Re-raise HTTPException as-is
    except Exception as e:
        logger.exception(f"Unexpected error while scraping {url}: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")
async def scrape_anime_by_category(anime_category: str, page: int, client: AsyncClient, type: Optional[str] = None) -> List[Anime]:
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be a positive integer")
    
    base_url = TOONSTREAM_BASE_URL.rstrip('/')
    # Construct URL: e.g., https://toonstream.one/category/action/ or https://toonstream.one/category/action/page/2/
    # Support query parameter ?type=movies or ?type=series
    if page == 1:
        url = f"{base_url}/category/{anime_category}/"
    else:
        url = f"{base_url}/category/{anime_category}/page/{page}/"
    
    # Add query parameter if type is specified
    if type:
        separator = '&' if '?' in url else '?'
        url = f"{url}{separator}type={type}"
    
    semaphore = asyncio.Semaphore(5)
    
    logger.info(f"Scraping anime category URL: {url}")
    
    async with semaphore:
        await asyncio.sleep(1)  # Respectful delay
        try:
            response = await client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Try multiple selectors to find anime items
            articles = []
            articles = soup.find_all("article", class_="post dfx fcl movies")
            if not articles:
                articles = soup.find_all("article", class_=lambda x: x and ("post" in " ".join(x) or "movies" in " ".join(x)))
            if not articles:
                articles = soup.find_all("div", class_=lambda x: x and ("post" in " ".join(x) or "movie" in " ".join(x) or "anime" in " ".join(x)))
            if not articles:
                list_container = soup.find("div", class_=lambda x: x and ("list" in " ".join(x).lower() or "grid" in " ".join(x).lower()))
                if list_container:
                    articles = list_container.find_all(["article", "div"], recursive=True)
            if not articles:
                all_items = soup.find_all(["article", "div"], class_=True)
                articles = [item for item in all_items if item.find("h2") and item.find("a")]
            # Fallback: find h2 headings and get their parent containers
            if not articles:
                h2_headings = soup.find_all("h2")
                for h2 in h2_headings:
                    # Find parent container (article, div, or section)
                    parent = h2.find_parent(["article", "div", "section", "li"])
                    if parent and parent not in articles and parent.find("a"):
                        articles.append(parent)
            
            if not articles:
                logger.warning(f"No anime found for category '{anime_category}' on page {page}")
                raise HTTPException(status_code=404, detail=f"No anime found for category '{anime_category}' on page {page}")
            
            anime_list = []
            for article in articles:
                try:
                    # Try multiple ways to find title
                    title = None
                    title_tag = article.find("h2", class_="entry-title") or article.find("h2") or article.find("h3")
                    if title_tag:
                        title = title_tag.text.strip()
                    else:
                        link_tag = article.find("a")
                        if link_tag:
                            title = link_tag.get("title") or link_tag.get("alt") or link_tag.text.strip()
                    
                    if not title:
                        logger.debug("Skipping item: no title found")
                        continue
                    
                    # Try multiple ways to find TMDB rating
                    tmdb_rating = None
                    rating_tag = article.find("span", class_="vote") or article.find("span", class_=lambda x: x and "vote" in " ".join(x))
                    if rating_tag:
                        rating_text = rating_tag.text.replace("TMDB", "").strip()
                        try:
                            tmdb_rating = float(rating_text)
                        except ValueError:
                            rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                            if rating_match:
                                tmdb_rating = float(rating_match.group(1))
                    # Alternative: look for TMDB text in the article text
                    if not tmdb_rating:
                        article_text = article.get_text()
                        tmdb_match = re.search(r'TMDB\s*(\d+\.?\d*)', article_text, re.IGNORECASE)
                        if tmdb_match:
                            try:
                                tmdb_rating = float(tmdb_match.group(1))
                            except ValueError:
                                pass
                    
                    # Extract image URL
                    image_url = None
                    image_tag = article.find("img")
                    if image_tag:
                        image_url = image_tag.get("src") or image_tag.get("data-src") or image_tag.get("data-original") or image_tag.get("data-lazy-src")
                        if image_url:
                            if image_url.startswith("//"):
                                image_url = "https:" + image_url
                            elif image_url.startswith("/"):
                                image_url = base_url + image_url
                            elif not image_url.startswith(('http://', 'https://')):
                                image_url = base_url + '/' + image_url.lstrip('/')
                    
                    # Extract series URL - try multiple selectors
                    series_url = None
                    # Try to find "View Movie" or "View Serie" links first
                    view_link = article.find("a", string=re.compile(r'View\s+(Movie|Serie)', re.IGNORECASE))
                    if view_link:
                        link_tag = view_link
                    else:
                        link_tag = article.find("a", class_="lnk-blk") or article.find("a", href=True)
                    
                    if link_tag:
                        series_url = link_tag.get("href")
                        if series_url:
                            if series_url.startswith("//"):
                                series_url = "https:" + series_url
                            elif series_url.startswith("/"):
                                series_url = base_url + series_url
                            elif not series_url.startswith(('http://', 'https://')):
                                series_url = base_url + '/' + series_url.lstrip('/')
                            
                            # Ensure the URL has proper slug format (extract and reconstruct if needed)
                            extracted_slug = extract_series_slug_from_url(series_url)
                            if extracted_slug:
                                # Reconstruct URL with proper slug format
                                if '/series/' in series_url:
                                    series_url = f"{base_url}/series/{extracted_slug}/"
                                elif '/movie/' in series_url:
                                    series_url = f"{base_url}/movie/{extracted_slug}/"
                    
                    if not series_url:
                        logger.debug(f"Skipping item '{title}': no URL found")
                        continue
                    
                    # Basic language detection
                    language = None
                    for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam']:
                        if lang in title.lower():
                            language = lang
                            break
                    
                    # Attempt to extract additional fields (if available)
                    description = None
                    desc_tag = article.find("p", class_=lambda x: x and "desc" in " ".join(x).lower()) or article.find("div", class_=lambda x: x and "excerpt" in " ".join(x).lower())
                    if desc_tag:
                        description = desc_tag.text.strip()
                    
                    genres = []
                    genre_tags = article.find_all("a", href=lambda x: x and "genre" in x.lower() if x else False)
                    if genre_tags:
                        genres = [tag.text.strip() for tag in genre_tags]
                    
                    year = None
                    year_tag = article.find("span", class_="release-year") or article.find("div", class_="year")
                    if year_tag:
                        year = year_tag.text.strip()
                    
                    anime_data = {
                        "title": title,
                        "url": series_url,
                        "image": image_url,
                        "description": description,
                        "genres": genres,
                        "language": language,
                        "year": year,
                        "rating": str(tmdb_rating) if tmdb_rating else None
                    }
                    
                    anime_list.append(Anime(**anime_data))
                except Exception as e:
                    logger.error(f"Failed to parse anime item in category '{anime_category}' on page {page}: {e}", exc_info=True)
                    continue
            
            if not anime_list:
                logger.warning(f"Could not parse any anime items from the page for category '{anime_category}' on page {page}")
                raise HTTPException(status_code=404, detail=f"No anime found for category '{anime_category}' on page {page}")
            
            logger.info(f"Scraped {len(anime_list)} anime for category '{anime_category}' on page {page}")
            return anime_list
        
        except HTTPStatusError as e:
            logger.error(f"HTTP error while scraping {url}: {e}")
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Category '{anime_category}' not found or page {page} does not exist")
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except RequestError as e:
            logger.error(f"Network error while scraping {url}: {e}")
            raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

# Function to scrape anime series detail page from toonstream.one
async def scrape_anime_series_detail(series_slug: str, client: AsyncClient, original_title: Optional[str] = None, _retry_from_search: bool = False, include_servers: bool = False) -> AnimeSeriesDetail:
    """
    Scrape detailed information about an anime series from toonstream.one/series/{series-slug}/
    
    Args:
        series_slug: The slug identifier for the series (e.g., 'a-wild-last-boss-appeared')
                     Can also be a full URL - will extract slug automatically
        client: Async HTTP client
        original_title: Original title for fallback search if slug fails
        
    Returns:
        AnimeSeriesDetail object with series information and episodes list
    """
    # Store original input for potential fallback search
    original_input = series_slug
    
    # If input is a URL, extract the slug
    if series_slug.startswith('http') or series_slug.startswith('/'):
        extracted_slug = extract_series_slug_from_url(series_slug)
        if extracted_slug:
            series_slug = extracted_slug
        else:
            # If URL extraction failed, try to normalize it
            series_slug = normalize_series_slug(series_slug)
    else:
        # Normalize the slug if it's a title
        # Save original for fallback search
        if not original_title:
            original_title = series_slug
        series_slug = normalize_series_slug(series_slug)
    
    base_url = TOONSTREAM_BASE_URL.rstrip('/')
    # Try series path first
    url = f"{base_url}/series/{series_slug}/"
    
    logger.info(f"Scraping anime series detail URL: {url}")
    
    try:
        response = await client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extract title
        title_tag = soup.find("h1")
        if not title_tag:
            raise HTTPException(status_code=404, detail=f"Series not found: {series_slug}")
        title = title_tag.text.strip()
        
        # Extract image
        image_url = None
        img_tag = soup.find("img") or soup.find("img", class_=lambda x: x and ("poster" in " ".join(x).lower() or "thumb" in " ".join(x).lower()))
        if img_tag:
            image_url = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original") or img_tag.get("data-lazy-src")
            if image_url:
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
                elif image_url.startswith("/"):
                    image_url = base_url + image_url
                elif not image_url.startswith(('http://', 'https://')):
                    image_url = base_url + '/' + image_url.lstrip('/')
        
        # Extract description - look for paragraph after h1 or in specific containers
        description = None
        # Try to find description in paragraph tags
        paragraphs = soup.find_all("p")
        for p in paragraphs:
            desc_text = p.get_text(strip=True)
            # Look for a paragraph that's reasonably long and contains descriptive content
            if len(desc_text) > 100 and "menu" not in desc_text.lower() and "navigation" not in desc_text.lower():
                # Check if it looks like a description (contains common description words)
                if any(word in desc_text.lower() for word in ["is", "are", "the", "a", "an", "with", "story", "follows", "about"]):
                    description = desc_text
                    break
        
        # Fallback: look in divs with description-related classes
        if not description:
            desc_tag = soup.find("div", class_=lambda x: x and ("desc" in " ".join(x).lower() or "content" in " ".join(x).lower() or "synopsis" in " ".join(x).lower()))
            if desc_tag:
                desc_text = desc_tag.get_text(strip=True)
                if len(desc_text) > 100 and "menu" not in desc_text.lower():
                    description = desc_text
        
        # Extract metadata from page text
        page_text = soup.get_text()
        
        # Extract genres - look for genre tags or links
        genres = []
        genre_tags = soup.find_all("a", href=lambda x: x and "genre" in x.lower() if x else False)
        if genre_tags:
            genres = [tag.text.strip() for tag in genre_tags if tag.text.strip()]
        else:
            # Try to extract from text like "Action & Adventure, Animation, Anime Series"
            genres_match = re.search(r'(Action|Adventure|Animation|Anime|Series|Comedy|Drama|Fantasy|Horror|Romance|Sci-Fi|Thriller|Crime|Family|Kids|Martial|Mystery|Superhero|War)[\w\s&,]*', page_text)
            if genres_match:
                genres_text = genres_match.group(0)
                genres = [g.strip() for g in re.split(r'[,&]', genres_text) if g.strip()]
        
        # Extract languages
        languages = []
        lang_match = re.search(r'Language[:\s]*(.+?)(?:\n|Quality|Running|Cast|TMDB|$)', page_text, re.IGNORECASE)
        if lang_match:
            lang_text = lang_match.group(1).strip()
            languages = [l.strip() for l in re.split(r'[–-]', lang_text) if l.strip()]
        else:
            # Look for language links
            lang_tags = soup.find_all("a", href=lambda x: x and "language" in x.lower() if x else False)
            languages = [tag.text.strip() for tag in lang_tags if tag.text.strip()]
        
        # Extract quality
        quality = None
        quality_match = re.search(r'Quality[:\s]*(.+?)(?:\n|Running|Cast|TMDB|$)', page_text, re.IGNORECASE)
        if quality_match:
            quality = quality_match.group(1).strip()
        
        # Extract duration/running time
        duration = None
        duration_match = re.search(r'(?:Running time|Duration)[:\s]*(\d+\s*min)', page_text, re.IGNORECASE)
        if duration_match:
            duration = duration_match.group(1).strip()
        
        # Extract rating
        rating = None
        rating_match = re.search(r'TMDB[:\s]*(\d+\.?\d*)', page_text, re.IGNORECASE)
        if rating_match:
            rating = rating_match.group(1).strip()
        
        # Extract year
        year = None
        year_match = re.search(r'\b(19|20)\d{2}\b', page_text)
        if year_match:
            year = year_match.group(0)
        
        # Extract seasons and episodes count
        seasons = None
        episodes_count = None
        seasons_match = re.search(r'(\d+)\s*Seasons?', page_text, re.IGNORECASE)
        if seasons_match:
            seasons = seasons_match.group(1)
        episodes_match = re.search(r'(\d+)\s*Episodes?', page_text, re.IGNORECASE)
        if episodes_match:
            episodes_count = episodes_match.group(1)
        
        # Extract cast
        cast = []
        cast_match = re.search(r'Cast[:\s]*(.+?)(?:\n\n|\nTMDB|$)', page_text, re.IGNORECASE | re.DOTALL)
        if cast_match:
            cast_text = cast_match.group(1).strip()
            cast = [c.strip() for c in re.split(r'[,]', cast_text) if c.strip() and len(c.strip()) > 2]
        
        # Extract episodes - Enhanced automatic detection
        episodes = []
        episode_urls_found = set()  # Track found URLs to avoid duplicates
        
        # Method 1: Look for episode items with class containing "episode"
        episode_items = soup.find_all(["div", "li", "article"], class_=lambda x: x and "episode" in " ".join(x).lower() if x else False)
        
        # Method 2: Look for links with episode pattern in href
        if not episode_items:
            episode_links = soup.find_all("a", href=lambda x: x and ("episode" in x.lower() or re.search(r'\d+x\d+', x.lower())) if x else False)
            for link in episode_links:
                parent = link.find_parent(["div", "li", "article", "section"])
                if parent and parent not in episode_items:
                    episode_items.append(parent)
        
        # Method 3: Look for h2/h3/h4 with episode format (e.g., "1x1", "1x2", "S1E1")
        if not episode_items:
            all_headings = soup.find_all(["h2", "h3", "h4"])
            for heading in all_headings:
                heading_text = heading.text.strip()
                # Check for various episode formats: "1x1", "S1E1", "Season 1 Episode 1", etc.
                episode_pattern = re.search(r'(?:(\d+)[xX](\d+)|[Ss](\d+)[Ee](\d+)|Season\s*(\d+)\s*Episode\s*(\d+))', heading_text)
                if episode_pattern:
                    # Find parent container
                    parent = heading.find_parent(["div", "li", "article", "section"])
                    if parent and parent not in episode_items:
                        episode_items.append(parent)
        
        # Method 4: Find items with "View" link and episode pattern
        if not episode_items:
            view_links = soup.find_all("a", string=re.compile(r'(View|Watch|Play)', re.IGNORECASE))
            for link in view_links:
                # Check parent or nearby elements for episode pattern
                parent = link.find_parent(["div", "li", "article", "section"])
                if parent:
                    parent_text = parent.get_text()
                    if re.search(r'\d+x\d+', parent_text) or re.search(r'[Ss]\d+[Ee]\d+', parent_text):
                        if parent not in episode_items:
                            episode_items.append(parent)
        
        # Method 5: Direct episode link search by URL pattern
        if not episode_items:
            all_links = soup.find_all("a", href=True)
            for link in all_links:
                href = link.get("href", "")
                # Check if URL matches episode pattern: /episode/{series-slug}-{season}x{episode}/
                if href and ("/episode/" in href.lower() or re.search(r'-\d+x\d+', href.lower())):
                    parent = link.find_parent(["div", "li", "article", "section"])
                    if parent and parent not in episode_items:
                        episode_items.append(parent)
        
        # Parse episodes from found items - Enhanced parsing
        for item in episode_items:
            try:
                item_text = item.get_text()
                
                # Try multiple episode format patterns
                episode_match = None
                patterns = [
                    r'(\d+)[xX](\d+)',  # 1x1, 1x2 format
                    r'[Ss](\d+)[Ee](\d+)',  # S1E1 format
                    r'Season\s*(\d+)\s*Episode\s*(\d+)',  # Season 1 Episode 1
                    r'Ep\.?\s*(\d+)',  # Ep 1 (assume season 1)
                ]
                
                for pattern in patterns:
                    episode_match = re.search(pattern, item_text, re.IGNORECASE)
                    if episode_match:
                        break
                
                if not episode_match:
                    # Try extracting from URL
                    ep_link = item.find("a", href=True)
                    if ep_link:
                        href = ep_link.get("href", "")
                        # Check URL pattern: /episode/{series-slug}-{season}x{episode}/
                        url_match = re.search(r'-(\d+)x(\d+)', href)
                        if url_match:
                            episode_match = url_match
                
                if not episode_match:
                    continue
                
                # Extract season and episode numbers
                if len(episode_match.groups()) >= 2:
                    season_num = episode_match.group(1)
                    episode_num = episode_match.group(2)
                else:
                    # Single group match (Ep 1 format)
                    season_num = "1"
                    episode_num = episode_match.group(1)
                
                # Extract episode URL
                ep_url = None
                ep_link = item.find("a", href=True)
                if ep_link:
                    ep_url = ep_link.get("href")
                    if ep_url:
                        if ep_url.startswith("//"):
                            ep_url = "https:" + ep_url
                        elif ep_url.startswith("/"):
                            ep_url = base_url + ep_url
                        elif not ep_url.startswith(('http://', 'https://')):
                            ep_url = base_url + '/' + ep_url.lstrip('/')
                
                # Skip if URL already processed
                if ep_url and ep_url in episode_urls_found:
                    continue
                if ep_url:
                    episode_urls_found.add(ep_url)
                
                # Extract episode title
                ep_title_tag = item.find(["h2", "h3", "h4"]) or item.find("a")
                ep_title = ep_title_tag.text.strip() if ep_title_tag and ep_title_tag.text.strip() else f"Episode {episode_num}"
                
                # Remove episode number from title if present
                ep_title = re.sub(r'\d+x\d+\s*', '', ep_title, flags=re.IGNORECASE).strip()
                ep_title = re.sub(r'[Ss]\d+[Ee]\d+\s*', '', ep_title, flags=re.IGNORECASE).strip()
                ep_title = re.sub(r'Season\s*\d+\s*Episode\s*\d+\s*', '', ep_title, flags=re.IGNORECASE).strip()
                
                # If title is empty or just repeats series name, use default
                if not ep_title or ep_title.lower() == title.lower() or len(ep_title) < 3:
                    ep_title = f"Episode {episode_num}"
                
                # Clean up title
                ep_title = re.sub(r'\s+', ' ', ep_title).strip()
                
                # Extract episode image
                ep_image_url = None
                ep_img_tag = item.find("img")
                if ep_img_tag:
                    ep_image_url = ep_img_tag.get("src") or ep_img_tag.get("data-src") or ep_img_tag.get("data-original") or ep_img_tag.get("data-lazy-src")
                    if ep_image_url:
                        if ep_image_url.startswith("//"):
                            ep_image_url = "https:" + ep_image_url
                        elif ep_image_url.startswith("/"):
                            ep_image_url = base_url + ep_image_url
                        elif not ep_image_url.startswith(('http://', 'https://')):
                            ep_image_url = base_url + '/' + ep_image_url.lstrip('/')
                
                # Extract aired date if available
                aired_date = None
                date_patterns = [
                    r'(\d+\s*(?:months?|days?|weeks?|years?)\s*ago)',
                    r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                    r'(\d{4}-\d{2}-\d{2})',
                ]
                for date_pattern in date_patterns:
                    date_match = re.search(date_pattern, item_text, re.IGNORECASE)
                    if date_match:
                        aired_date = date_match.group(1).strip()
                        break
                
                if ep_url:  # Only add if we have a URL
                    episodes.append(AnimeEpisode(
                        title=ep_title,
                        url=ep_url,
                        season=season_num,
                        episode_number=episode_num,
                        image=ep_image_url,
                        aired_date=aired_date
                    ))
            except Exception as e:
                logger.error(f"Failed to parse episode: {e}")
                continue
        
        # Sort episodes by season and episode number
        episodes.sort(key=lambda x: (int(x.season), int(x.episode_number)))
        
        # Build and return the detail object
        series_detail = AnimeSeriesDetail(
            title=title,
            url=url,
            image=image_url,
            description=description,
            genres=genres[:10] if genres else [],  # Limit to 10 genres
            languages=languages[:5] if languages else [],  # Limit to 5 languages
            year=year,
            rating=rating,
            quality=quality,
            duration=duration,
            seasons=seasons,
            episodes_count=episodes_count,
            cast=cast[:10] if cast else [],  # Limit to 10 cast members
            episodes=episodes
        )
        
        # Optionally populate server links for each episode
        if include_servers and series_detail.episodes:
            logger.info(f"Fetching server links for {len(series_detail.episodes)} anime episodes...")
            
            async def fetch_anime_episode_servers(ep: AnimeEpisode) -> AnimeEpisode:
                """Fetch server links for a single anime episode"""
                try:
                    # Extract season and episode numbers
                    season_num = int(ep.season)
                    episode_num = int(ep.episode_number)
                    
                    # Fetch episode data to get server links
                    episode_data = await scrape_anime_episode_detail(
                        series_slug, 
                        season_num, 
                        episode_num, 
                        client
                    )
                    
                    # Update episode with server links
                    return AnimeEpisode(
                        title=ep.title,
                        url=ep.url,
                        season=ep.season,
                        episode_number=ep.episode_number,
                        image=ep.image,
                        aired_date=ep.aired_date,
                        servers=episode_data.servers
                    )
                except Exception as e:
                    logger.warning(f"Failed to fetch servers for anime episode {ep.episode_number}: {e}")
                    # Return original episode without servers
                    return ep
            
            # Fetch all episodes concurrently (limit concurrency)
            semaphore_episodes = asyncio.Semaphore(10)  # Limit concurrent episode fetches
            
            async def fetch_with_limit(ep: AnimeEpisode) -> AnimeEpisode:
                async with semaphore_episodes:
                    return await fetch_anime_episode_servers(ep)
            
            # Fetch all episodes
            updated_episodes = await asyncio.gather(*[fetch_with_limit(ep) for ep in series_detail.episodes])
            series_detail.episodes = updated_episodes
            
            logger.info(f"Successfully fetched server links for anime episodes")
        
        logger.info(f"Scraped series detail for '{title}': {len(episodes)} episodes found")
        return series_detail
    
    except HTTPStatusError as e:
        status_code = e.response.status_code if e.response else 502
        if status_code == 404:
            # Try fallback: search for the series and use the correct slug
            logger.info(f"Series not found with slug '{series_slug}', trying fallback search...")
            
            # Use original title if available, otherwise use the slug
            search_term = original_title if original_title else series_slug.replace('-', ' ')
            
            try:
                # Search for the series
                search_results = await scrape_anime_data(search_term, client)
                
                if search_results:
                    # Find the best match (exact title match or closest match)
                    best_match = None
                    search_term_lower = search_term.lower()
                    
                    for result in search_results:
                        result_title_lower = result.title.lower()
                        # Check for exact match or high similarity
                        if (search_term_lower in result_title_lower or 
                            result_title_lower in search_term_lower or
                            search_term_lower.replace(':', '').replace('-', ' ') in result_title_lower.replace(':', '').replace('-', ' ')):
                            best_match = result
                            break
                    
                    # If no exact match, use the first result
                    if not best_match and search_results:
                        best_match = search_results[0]
                    
                    if best_match:
                        # Extract slug from the URL - check both /series/ and /movie/ paths
                        match_url = str(best_match.url)
                        correct_slug = extract_series_slug_from_url(match_url)
                        
                        # Also check if it's a movie URL
                        if not correct_slug:
                            movie_match = re.search(r'/movie/([^/]+)', match_url)
                            if movie_match:
                                correct_slug = movie_match.group(1).rstrip('/')
                        
                        if correct_slug and not _retry_from_search:
                            logger.info(f"Found series via search, retrying with correct slug: {correct_slug}")
                            # Check if it's a movie or series by URL path
                            if '/movie/' in match_url:
                                logger.warning(f"Found item is a movie, not a series: {match_url}. Will try anyway.")
                            
                            # Recursively call with the correct slug (set flag to prevent infinite recursion)
                            return await scrape_anime_series_detail(correct_slug, client, original_title=original_title if original_title else search_term, _retry_from_search=True)
            except Exception as search_error:
                logger.warning(f"Fallback search failed: {search_error}")
            
            # If fallback search didn't work, raise 404
            raise HTTPException(
                status_code=404, 
                detail=f"Series not found: {series_slug}. Please try searching for the series first using /search-anime endpoint."
            )
        
        logger.error(f"HTTP error {status_code} while scraping {url}: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
    except RequestError as e:
        logger.error(f"Network error while scraping {url}: {e}")
        raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
    except HTTPException:
        raise  # Re-raise HTTPException as-is
    except Exception as e:
        logger.exception(f"Unexpected error while scraping {url}: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

# Function to scrape anime episode detail page from toonstream.one
async def scrape_anime_episode_detail(series_slug: str, season: int, episode: int, client: AsyncClient) -> Episode:
    """
    Scrape detailed information about an anime episode from toonstream.one/episode/{series-slug}-{season}x{episode}/
    
    Args:
        series_slug: The slug identifier for the series (e.g., 'tojima-wants-to-be-a-kamen-rider')
        season: Season number (e.g., 1)
        episode: Episode number (e.g., 2)
        client: Async HTTP client
        
    Returns:
        Episode object with episode information including streaming links
    """
    # Normalize the slug
    series_slug = normalize_series_slug(series_slug)
    
    if season < 1 or episode < 1:
        raise HTTPException(status_code=400, detail="Season and episode must be positive integers")
    
    base_url = TOONSTREAM_BASE_URL.rstrip('/')
    # Construct URL: https://toonstream.one/episode/{series-slug}-{season}x{episode}/
    url = f"{base_url}/episode/{series_slug}-{season}x{episode}/"
    
    logger.info(f"Scraping anime episode detail URL: {url}")
    
    semaphore = asyncio.Semaphore(5)
    
    async with semaphore:
        await asyncio.sleep(1)  # Respectful delay
        try:
            response = await client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            episode_data = {}
            
            # Extract title - try multiple selectors
            title = None
            title_tag = soup.find("h1", class_="entry-title") or soup.find("h1") or soup.find("h2", class_="entry-title")
            if title_tag:
                title = title_tag.text.strip()
            else:
                # Fallback: construct title from series slug and episode info
                title = f"{series_slug.replace('-', ' ').title()} S{season:02d}E{episode:02d}"
            
            episode_data['title'] = title
            
            # Set episode URL
            episode_data['url'] = url
            
            # Extract series title - look for links to series page or breadcrumbs
            series_title = series_slug.replace('-', ' ').title()
            series_link = soup.find("a", href=lambda x: x and "/series/" in x if x else False)
            if series_link:
                series_title = series_link.text.strip()
            else:
                # Try to find in breadcrumbs
                breadcrumb = soup.find("nav", class_=lambda x: x and "breadcrumb" in " ".join(x).lower() if x else False)
                if breadcrumb:
                    series_link = breadcrumb.find("a", href=lambda x: x and "/series/" in x if x else False)
                    if series_link:
                        series_title = series_link.text.strip()
                else:
                    # Try to extract from page title or meta tags
                    page_title = soup.find("title")
                    if page_title:
                        page_title_text = page_title.text.strip()
                        # Try to extract series name from title (usually before episode info)
                        title_match = re.search(r'(.+?)\s*[-–]\s*(?:Episode|S\d+E\d+|\d+x\d+)', page_title_text, re.IGNORECASE)
                        if title_match:
                            series_title = title_match.group(1).strip()
            
            episode_data['series_title'] = series_title
            
            # Set season and episode numbers
            episode_data['season'] = str(season)
            episode_data['episode_number'] = str(episode)
            
            # Extract description - try multiple locations
            description = None
            # Try entry-content div
            desc_tag = soup.find("div", class_="entry-content") or soup.find("div", class_=lambda x: x and "content" in " ".join(x).lower() if x else False)
            if desc_tag:
                # Get all paragraphs
                paragraphs = desc_tag.find_all("p")
                desc_texts = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                if desc_texts:
                    # Filter out navigation/menu text
                    desc_texts = [t for t in desc_texts if len(t) > 50 and "menu" not in t.lower() and "navigation" not in t.lower()]
                    if desc_texts:
                        description = " ".join(desc_texts[:3])  # Take first 3 paragraphs
            
            # Fallback: look for meta description
            if not description:
                meta_desc = soup.find("meta", property="og:description") or soup.find("meta", attrs={"name": "description"})
                if meta_desc:
                    description = meta_desc.get("content", "").strip()
            
            episode_data['description'] = description
            
            # Extract image - try multiple selectors
            image_url = None
            img_tag = soup.find("img", class_=lambda x: x and ("poster" in " ".join(x).lower() or "thumb" in " ".join(x).lower() or "episode" in " ".join(x).lower()) if x else False)
            if not img_tag:
                # Try to find any img in the main content area
                content_area = soup.find("div", class_="entry-content") or soup.find("article") or soup.find("main")
                if content_area:
                    img_tag = content_area.find("img")
            
            if img_tag:
                image_url = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original") or img_tag.get("data-lazy-src")
                if image_url:
                    if image_url.startswith("//"):
                        image_url = "https:" + image_url
                    elif image_url.startswith("/"):
                        image_url = base_url + image_url
                    elif not image_url.startswith(('http://', 'https://')):
                        image_url = base_url + '/' + image_url.lstrip('/')
            
            episode_data['image'] = image_url
            
            # Extract streaming links - this is critical for deep scraping
            streaming_links = []  # Legacy support
            servers = []  # New structured server list
            
            server_counter = 1
            processed_urls = set()
            
            # Method 1: Look for server/player option links (most reliable for server names)
            server_links = soup.find_all("a", class_=lambda x: x and ("server" in " ".join(x).lower() or "player" in " ".join(x).lower() or "option" in " ".join(x).lower()) if x else False)
            for link in server_links:
                href = link.get("href")
                if href:
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = base_url + href
                    elif not href.startswith(('http://', 'https://')):
                        continue
                    exclude_keywords = ['login', 'signup', 'advert', 'advertisement']
                    if not any(exclude in href.lower() for exclude in exclude_keywords):
                        if href not in processed_urls:
                            processed_urls.add(href)
                            streaming_links.append(href)  # Legacy
                            servers.append(StreamingServer(
                                name=extract_server_name(link, f"Server {server_counter}"),
                                url=href,
                                quality=extract_quality(link.text if hasattr(link, 'text') else "", href),
                                type=get_server_type(href)
                            ))
                            server_counter += 1
            
            # Method 2: Look for iframe embeds (common for video players)
            iframes = soup.find_all("iframe")
            for iframe in iframes:
                src = iframe.get("src") or iframe.get("data-src")
                if src:
                    if src.startswith("//"):
                        src = "https:" + src
                    elif src.startswith("/"):
                        src = base_url + src
                    elif not src.startswith(('http://', 'https://')):
                        continue
                    # Filter out unwanted iframes
                    exclude_domains = ['facebook', 'twitter', 'instagram', 'google', 'ads', 'advertisement']
                    if not any(exclude in src.lower() for exclude in exclude_domains):
                        if src not in processed_urls:
                            processed_urls.add(src)
                            streaming_links.append(src)  # Legacy
                            servers.append(StreamingServer(
                                name=f"Embed {server_counter}",
                                url=src,
                                quality=extract_quality("", src),
                                type=get_server_type(src)
                            ))
                            server_counter += 1
            
            # Method 3: Look for video player containers and their links
            player_containers = soup.find_all(["div", "section"], class_=lambda x: x and ("player" in " ".join(x).lower() or "video" in " ".join(x).lower() or "embed" in " ".join(x).lower()) if x else False)
            for container in player_containers:
                # Find all links in player container
                links = container.find_all("a", href=True)
                for link in links:
                    href = link.get("href")
                    if href and href.startswith(('http://', 'https://')):
                        # Check if it's a streaming link
                        link_text = link.text.lower() if link.text else ""
                        if any(keyword in link_text or keyword in href.lower() for keyword in ['watch', 'stream', 'play', 'server', 'embed', 'player']):
                            exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram']
                            if not any(exclude in href.lower() for exclude in exclude_keywords):
                                if href not in processed_urls:
                                    processed_urls.add(href)
                                    streaming_links.append(href)  # Legacy
                                    servers.append(StreamingServer(
                                        name=extract_server_name(link, f"Server {server_counter}"),
                                        url=href,
                                        quality=extract_quality(link_text, href),
                                        type=get_server_type(href)
                                    ))
                                    server_counter += 1
                
                # Also check for data attributes that might contain video URLs
                for attr in ['data-src', 'data-url', 'data-video', 'data-embed']:
                    data_url = container.get(attr)
                    if data_url:
                        if data_url.startswith(('http://', 'https://')):
                            if data_url not in processed_urls:
                                processed_urls.add(data_url)
                                streaming_links.append(data_url)  # Legacy
                                servers.append(StreamingServer(
                                    name=f"Data {server_counter}",
                                    url=data_url,
                                    quality=extract_quality("", data_url),
                                    type=get_server_type(data_url)
                                ))
                                server_counter += 1
            
            # Method 4: Look for embed/video links in script tags (enhanced)
            script_tags = soup.find_all("script")
            for script in script_tags:
                script_text = script.string or ""
                if not script_text:
                    continue
                
                # Enhanced URL patterns for video links
                url_patterns = [
                    r'["\'](https?://[^"\']*(?:embed|player|video|watch|stream|play|server|cdn|mp4|m3u8|webm|mkv|flv)[^"\']*)["\']',
                    r'src["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'file["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'video["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'source["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'fileurl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'video_url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'videoUrl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'getElementById\(["\']([^"\']+)["\']\)\.src\s*=\s*["\'](https?://[^"\']+)["\']',
                    r'\.setAttribute\(["\']src["\'],\s*["\'](https?://[^"\']+)["\']',
                    r'iframe\.src\s*=\s*["\'](https?://[^"\']+)["\']',
                ]
                
                for pattern in url_patterns:
                    matches = re.findall(pattern, script_text, re.IGNORECASE)
                    for match in matches:
                        # Handle tuple matches
                        url = match[1] if isinstance(match, tuple) and len(match) > 1 else match
                        if isinstance(match, tuple):
                            url = match[1] if len(match) > 1 else match[0]
                        else:
                            url = match
                        
                        if url and url.startswith(('http://', 'https://')):
                            exclude_keywords = ['facebook', 'twitter', 'instagram', 'google', 'ads', 'analytics', 'tracking', 'login', 'signup']
                            if not any(exclude in url.lower() for exclude in exclude_keywords):
                                # Check if it looks like a video URL
                                video_indicators = ['stream', 'embed', 'player', 'watch', 'video', 'play', 'server', 'cdn', 'mp4', 'm3u8', 'webm', 'mkv', 'dood', 'streamtape', 'mixdrop', 'trembed']
                                if any(indicator in url.lower() for indicator in video_indicators):
                                    if url not in processed_urls:
                                        processed_urls.add(url)
                                        streaming_links.append(url)  # Legacy
                                        servers.append(StreamingServer(
                                            name=f"Script {server_counter}",
                                            url=url,
                                            quality=extract_quality("", url),
                                            type=get_server_type(url)
                                        ))
                                        server_counter += 1
            
            # Method 5: Look for links with specific text patterns
            watch_links = soup.find_all("a", string=re.compile(r'(Watch|Stream|Play|Server|Embed)', re.IGNORECASE))
            for link in watch_links:
                href = link.get("href")
                if href:
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = base_url + href
                    elif not href.startswith(('http://', 'https://')):
                        continue
                    exclude_keywords = ['login', 'signup', 'advert', 'advertisement']
                    if not any(exclude in href.lower() for exclude in exclude_keywords):
                        if href not in processed_urls:
                            processed_urls.add(href)
                            streaming_links.append(href)  # Legacy
                            servers.append(StreamingServer(
                                name=extract_server_name(link, f"Server {server_counter}"),
                                url=href,
                                quality=extract_quality(link.text if hasattr(link, 'text') else "", href),
                                type=get_server_type(href)
                            ))
                            server_counter += 1
            
            # Method 6: Look for video source tags
            video_tags = soup.find_all("video")
            for video in video_tags:
                source_tags = video.find_all("source")
                for source in source_tags:
                    src = source.get("src")
                    if src:
                        if src.startswith("//"):
                            src = "https:" + src
                        elif src.startswith("/"):
                            src = base_url + src
                        elif src.startswith(('http://', 'https://')):
                            if src not in processed_urls:
                                processed_urls.add(src)
                                streaming_links.append(src)  # Legacy
                                quality = source.get('label') or extract_quality("", src)
                                servers.append(StreamingServer(
                                    name=f"Direct {server_counter}",
                                    url=src,
                                    quality=quality,
                                    type="direct"
                                ))
                                server_counter += 1
            
            episode_data['streaming_links'] = streaming_links  # Legacy support
            episode_data['servers'] = servers  # New structured format
            
            # Deep resolve embed URLs to direct playable links
            resolved_servers = []
            for server in servers:
                server_url_str = str(server.url)
                if server.type in ['embed', 'iframe'] or '/embed/' in server_url_str.lower() or 'embed' in server_url_str.lower():
                    # Try to resolve embed URL to direct link
                    resolved_url = await resolve_embed_to_direct(client, server_url_str, max_depth=2)
                    if resolved_url and resolved_url != server_url_str:
                        # Use resolved URL if different
                        resolved_servers.append(StreamingServer(
                            name=server.name,
                            url=resolved_url,
                            quality=server.quality,
                            type="direct" if resolved_url else server.type
                        ))
                    else:
                        # Keep original if resolution failed or same
                        resolved_servers.append(server)
                else:
                    # Keep non-embed links as-is
                    resolved_servers.append(server)
            
            # Update episode data with resolved servers
            episode_data['servers'] = resolved_servers
            # Also update legacy streaming_links with resolved URLs
            episode_data['streaming_links'] = [str(server.url) for server in resolved_servers]
            
            # Extract duration
            duration = None
            page_text = soup.get_text()
            duration_match = re.search(r'(\d+\s*(?:min|minutes?|mins?))', page_text, re.IGNORECASE)
            if duration_match:
                duration = duration_match.group(1).strip()
            else:
                # Try to find in meta tags or specific divs
                duration_tag = soup.find("span", class_=lambda x: x and "duration" in " ".join(x).lower() if x else False)
                if duration_tag:
                    duration = duration_tag.text.strip()
            
            episode_data['duration'] = duration
            
            # Extract language
            language = None
            # Look for language in page text
            lang_match = re.search(r'Language[:\s]*(.+?)(?:\n|Quality|Duration|Cast|TMDB|$)', page_text, re.IGNORECASE)
            if lang_match:
                lang_text = lang_match.group(1).strip()
                # Extract first language mentioned
                for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam', 'japanese', 'subbed', 'dubbed']:
                    if lang in lang_text.lower():
                        language = lang
                        break
            
            # Fallback: check title or description
            if not language:
                search_text = (title + " " + (description or "")).lower()
                for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam', 'japanese']:
                    if lang in search_text:
                        language = lang
                        break
            
            # Default to 'english' if no language detected
            if not language:
                language = 'english'
            
            episode_data['language'] = language
            
            # Validate required fields
            if not episode_data.get('title') or not episode_data.get('url'):
                logger.warning(f"Failed to parse episode data for {series_slug} S{season:02d}E{episode:02d}")
                raise HTTPException(
                    status_code=404,
                    detail=f"Episode not found for {series_slug} season {season} episode {episode}"
                )
            
            logger.info(f"Scraped episode detail for {series_slug} S{season:02d}E{episode:02d}: {len(streaming_links)} streaming links found")
            return Episode(**episode_data)
            
        except HTTPStatusError as e:
            status_code = e.response.status_code if e.response else 502
            logger.error(f"HTTP error {status_code} while scraping {url}: {e}")
            if status_code == 404:
                raise HTTPException(
                    status_code=404,
                    detail=f"Episode not found for {series_slug} season {season} episode {episode}"
                )
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except RequestError as e:
            logger.error(f"Network error while scraping {url}: {e}")
            raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
        except HTTPException:
            raise  # Re-raise HTTPException as-is
        except Exception as e:
            logger.exception(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

# Function to scrape toonstream.one movies listing page
async def scrape_toonstream_movies_page(page: int, client: AsyncClient) -> List[Anime]:
    """
    Scrape movies listing page from toonstream.one/movies/
    
    Args:
        page: Page number (1, 2, 3, etc.)
        client: Async HTTP client
        
    Returns:
        List of Anime objects representing movies
    """
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be a positive integer")
    
    base_url = TOONSTREAM_BASE_URL.rstrip('/')
    # Construct URL: https://toonstream.one/movies/ or https://toonstream.one/movies/page/2/
    if page == 1:
        url = f"{base_url}/movies/"
    else:
        url = f"{base_url}/movies/page/{page}/"
    
    semaphore = asyncio.Semaphore(5)
    
    logger.info(f"Scraping toonstream movies page URL: {url}")
    
    async with semaphore:
        await asyncio.sleep(1)  # Respectful delay
        try:
            response = await client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Try multiple selectors to find movie items
            articles = []
            articles = soup.find_all("article", class_="post dfx fcl movies")
            if not articles:
                articles = soup.find_all("article", class_=lambda x: x and ("post" in " ".join(x) or "movies" in " ".join(x)) if x else False)
            if not articles:
                articles = soup.find_all("div", class_=lambda x: x and ("post" in " ".join(x) or "movie" in " ".join(x)) if x else False)
            if not articles:
                list_container = soup.find("div", class_=lambda x: x and ("list" in " ".join(x).lower() or "grid" in " ".join(x).lower()) if x else False)
                if list_container:
                    articles = list_container.find_all(["article", "div"], recursive=True)
            if not articles:
                all_items = soup.find_all(["article", "div"], class_=True)
                articles = [item for item in all_items if item.find("h2") and item.find("a")]
            
            if not articles:
                logger.warning(f"No movies found on page {page}")
                raise HTTPException(status_code=404, detail=f"No movies found on page {page}")
            
            movies_list = []
            for article in articles:
                try:
                    # Extract title
                    title = None
                    title_tag = article.find("h2", class_="entry-title") or article.find("h2") or article.find("h3")
                    if title_tag:
                        title = title_tag.text.strip()
                    else:
                        link_tag = article.find("a")
                        if link_tag:
                            title = link_tag.get("title") or link_tag.get("alt") or link_tag.text.strip()
                    
                    if not title:
                        continue
                    
                    # Extract TMDB rating
                    tmdb_rating = None
                    rating_tag = article.find("span", class_="vote") or article.find("span", class_=lambda x: x and "vote" in " ".join(x) if x else False)
                    if rating_tag:
                        rating_text = rating_tag.text.replace("TMDB", "").strip()
                        try:
                            tmdb_rating = float(rating_text)
                        except ValueError:
                            rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                            if rating_match:
                                tmdb_rating = float(rating_match.group(1))
                    
                    # Extract image URL
                    image_url = None
                    img_tag = article.find("img")
                    if img_tag:
                        image_url = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original") or img_tag.get("data-lazy-src")
                        if image_url:
                            if image_url.startswith("//"):
                                image_url = "https:" + image_url
                            elif image_url.startswith("/"):
                                image_url = base_url + image_url
                            elif not image_url.startswith(('http://', 'https://')):
                                image_url = base_url + '/' + image_url.lstrip('/')
                    
                    # Extract movie URL - look for "View Movie" link
                    movie_url = None
                    view_link = article.find("a", string=re.compile(r'View\s+Movie', re.IGNORECASE))
                    if view_link:
                        movie_url = view_link.get("href")
                    else:
                        link_tag = article.find("a", href=True)
                        if link_tag:
                            movie_url = link_tag.get("href")
                    
                    if movie_url:
                        if movie_url.startswith("//"):
                            movie_url = "https:" + movie_url
                        elif movie_url.startswith("/"):
                            movie_url = base_url + movie_url
                        elif not movie_url.startswith(('http://', 'https://')):
                            movie_url = base_url + '/' + movie_url.lstrip('/')
                    
                    if not movie_url:
                        continue
                    
                    # Basic language detection
                    language = None
                    for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam', 'japanese']:
                        if lang in title.lower():
                            language = lang
                            break
                    
                    anime_data = {
                        "title": title,
                        "url": movie_url,
                        "image": image_url,
                        "rating": str(tmdb_rating) if tmdb_rating else None,
                        "language": language,
                        "description": None,
                        "genres": [],
                        "year": None
                    }
                    
                    movies_list.append(Anime(**anime_data))
                except Exception as e:
                    logger.error(f"Failed to parse movie item on page {page}: {e}")
                    continue
            
            if not movies_list:
                logger.warning(f"Could not parse any movies from page {page}")
                raise HTTPException(status_code=404, detail=f"No movies found on page {page}")
            
            logger.info(f"Scraped {len(movies_list)} movies from page {page}")
            return movies_list
            
        except HTTPStatusError as e:
            status_code = e.response.status_code if e.response else 502
            logger.error(f"HTTP error {status_code} while scraping {url}: {e}")
            if status_code == 404:
                raise HTTPException(status_code=404, detail=f"Movies page {page} not found")
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except RequestError as e:
            logger.error(f"Network error while scraping {url}: {e}")
            raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
        except HTTPException:
            raise  # Re-raise HTTPException as-is
        except Exception as e:
            logger.exception(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

# Function to scrape toonstream.one movie detail page
async def scrape_toonstream_movie_detail(movie_slug: str, client: AsyncClient) -> ToonstreamMovieDetail:
    """
    Scrape detailed information about a movie from toonstream.one/movies/{movie-slug}/
    
    Args:
        movie_slug: The slug identifier for the movie (e.g., 'crayon-shin-chan-the-movie-super-hot-the-spicy-kasukabe-dancers')
                   Can also be a full URL - will extract slug automatically
        client: Async HTTP client
        
    Returns:
        ToonstreamMovieDetail object with movie information including streaming links
    """
    # Store original input for potential fallback
    original_input = movie_slug
    
    # If input is a URL, extract the slug
    if movie_slug.startswith('http') or movie_slug.startswith('/'):
        extracted_slug = extract_movie_slug_from_url(movie_slug)
        if extracted_slug:
            movie_slug = extracted_slug
        else:
            movie_slug = normalize_movie_slug(movie_slug)
    else:
        movie_slug = normalize_movie_slug(movie_slug)
    
    base_url = TOONSTREAM_BASE_URL.rstrip('/')
    url = f"{base_url}/movies/{movie_slug}/"
    
    logger.info(f"Scraping toonstream movie detail URL: {url}")
    
    semaphore = asyncio.Semaphore(5)
    
    async with semaphore:
        await asyncio.sleep(1)  # Respectful delay
        try:
            response = await client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            movie_data = {}
            
            # Extract title
            title = None
            title_tag = soup.find("h1") or soup.find("h2", class_="entry-title")
            if title_tag:
                title = title_tag.text.strip()
            else:
                # Fallback: construct from slug
                title = movie_slug.replace('-', ' ').title()
            
            movie_data['title'] = title
            movie_data['url'] = url
            
            # Extract image
            image_url = None
            img_tag = soup.find("img") or soup.find("img", class_=lambda x: x and ("poster" in " ".join(x).lower() or "thumb" in " ".join(x).lower()) if x else False)
            if img_tag:
                image_url = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-original") or img_tag.get("data-lazy-src")
                if image_url:
                    if image_url.startswith("//"):
                        image_url = "https:" + image_url
                    elif image_url.startswith("/"):
                        image_url = base_url + image_url
                    elif not image_url.startswith(('http://', 'https://')):
                        image_url = base_url + '/' + image_url.lstrip('/')
            
            movie_data['image'] = image_url
            
            # Extract description
            description = None
            paragraphs = soup.find_all("p")
            for p in paragraphs:
                desc_text = p.get_text(strip=True)
                if len(desc_text) > 100 and "menu" not in desc_text.lower() and "navigation" not in desc_text.lower():
                    if any(word in desc_text.lower() for word in ["is", "are", "the", "a", "an", "with", "story", "follows", "about"]):
                        description = desc_text
                        break
            
            if not description:
                meta_desc = soup.find("meta", property="og:description") or soup.find("meta", attrs={"name": "description"})
                if meta_desc:
                    description = meta_desc.get("content", "").strip()
            
            movie_data['description'] = description
            
            # Extract page text for metadata extraction
            page_text = soup.get_text()
            
            # Extract genres
            genres = []
            genre_tags = soup.find_all("a", href=lambda x: x and "genre" in x.lower() if x else False)
            if genre_tags:
                genres = [tag.text.strip() for tag in genre_tags if tag.text.strip()]
            else:
                genre_match = re.search(r'(Adventure|Animation|Anime|Comedy|Crime|Drama|Family|Fantasy|Horror|Kids|Martial|Mystery|Romance|Sci-Fi|Superhero|Thriller|War)[\w\s&,]*', page_text, re.IGNORECASE)
                if genre_match:
                    genres_text = genre_match.group(0)
                    genres = [g.strip() for g in re.split(r'[,&]', genres_text) if g.strip()]
            
            movie_data['genres'] = genres[:10] if genres else []
            
            # Extract languages
            languages = []
            lang_match = re.search(r'Language[:\s]*(.+?)(?:\n|Quality|Running|Cast|TMDB|Director|$)', page_text, re.IGNORECASE)
            if lang_match:
                lang_text = lang_match.group(1).strip()
                languages = [l.strip() for l in re.split(r'[–-]', lang_text) if l.strip()]
            else:
                lang_tags = soup.find_all("a", href=lambda x: x and "language" in x.lower() if x else False)
                languages = [tag.text.strip() for tag in lang_tags if tag.text.strip()]
            
            movie_data['languages'] = languages[:10] if languages else []
            
            # Extract year
            year = None
            year_match = re.search(r'\b(19|20)\d{2}\b', page_text)
            if year_match:
                year = year_match.group(0)
            movie_data['year'] = year
            
            # Extract TMDB rating
            rating = None
            rating_match = re.search(r'TMDB[:\s]*(\d+\.?\d*)', page_text, re.IGNORECASE)
            if rating_match:
                rating = rating_match.group(1).strip()
            else:
                rating_tag = soup.find("span", class_=lambda x: x and "vote" in " ".join(x).lower() if x else False)
                if rating_tag:
                    rating_text = rating_tag.text.replace("TMDB", "").strip()
                    rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                    if rating_match:
                        rating = rating_match.group(1)
            movie_data['rating'] = rating
            
            # Extract quality
            quality = None
            quality_match = re.search(r'Quality[:\s]*(.+?)(?:\n|Running|Cast|TMDB|Director|$)', page_text, re.IGNORECASE)
            if quality_match:
                quality = quality_match.group(1).strip()
            movie_data['quality'] = quality
            
            # Extract duration
            duration = None
            duration_match = re.search(r'(?:Running time|Duration)[:\s]*(\d+\s*(?:h|hours?)?\s*\d*\s*(?:min|minutes?|mins?)?)', page_text, re.IGNORECASE)
            if duration_match:
                duration = duration_match.group(1).strip()
            else:
                duration_match = re.search(r'(\d+\s*(?:h|hours?)?\s*\d*\s*(?:min|minutes?|mins?))', page_text, re.IGNORECASE)
                if duration_match:
                    duration = duration_match.group(1).strip()
            movie_data['duration'] = duration
            
            # Extract director
            director = None
            director_match = re.search(r'Director[:\s]*(.+?)(?:\n|Cast|TMDB|Quality|Running|$)', page_text, re.IGNORECASE)
            if director_match:
                director = director_match.group(1).strip()
            movie_data['director'] = director
            
            # Extract cast
            cast = []
            cast_match = re.search(r'Cast[:\s]*(.+?)(?:\n\n|\nTMDB|$)', page_text, re.IGNORECASE | re.DOTALL)
            if cast_match:
                cast_text = cast_match.group(1).strip()
                cast = [c.strip() for c in re.split(r'[,]', cast_text) if c.strip() and len(c.strip()) > 2]
            movie_data['cast'] = cast[:10] if cast else []
            
            # --- Enhanced Streaming Link Extraction ---
            streaming_links = []
            
            # Helper function for deep resolution of embed pages
            async def resolve_embed_url(embed_url: str) -> Optional[str]:
                try:
                    logger.debug(f"Deep resolving embed: {embed_url}")
                    resp = await client.get(embed_url)
                    # We don't check status here strictly to allow softer failure
                    if resp.status_code != 200:
                        return None
                    
                    sub_soup = BeautifulSoup(resp.text, 'html.parser')
                    
                    # 1. Look for iframe inside the embed page
                    iframe = sub_soup.find("iframe")
                    if iframe:
                        src = iframe.get("src") or iframe.get("data-src")
                        if src and src.startswith(('http', '//')):
                             if src.startswith('//'):
                                 src = 'https:' + src
                             return src
                    
                    # 2. Look for video tag
                    video = sub_soup.find("video")
                    if video:
                        src = video.get("src")
                        if src: return src
                        source = video.find("source")
                        if source and source.get("src"): return source.get("src")
                    
                    # 3. Look for script redirects or window.location
                    script_content = sub_soup.find_all("script")
                    for s in script_content:
                        if s.string:
                            # Look for typical patterns
                            redirect_match = re.search(r'window\.location\.href\s*=\s*["\'](https?://[^"\']+)["\']', s.string)
                            if redirect_match: return redirect_match.group(1)
                            
                            src_match = re.search(r'src\s*:\s*["\'](https?://[^"\']+)["\']', s.string)
                            if src_match: return src_match.group(1)

                            iframe_match = re.search(r'iframe\s*src=["\'](https?://[^"\']+)["\']', s.string)
                            if iframe_match: return iframe_match.group(1)

                    return None
                except Exception as e:
                    logger.warning(f"Failed to resolve embed {embed_url}: {e}")
                    return None

            # Collect initial candidate links
            candidate_links = []

            # Method 1: Server/Watch links
            # More extensive regex for button text
            watch_links = soup.find_all("a", string=re.compile(r'(Server|Watch|Stream|Play|DL|Multi|Audio|Link)', re.IGNORECASE))
            for link in watch_links:
                href = link.get("href")
                if href: candidate_links.append(href)

            # Method 2: Iframes directly on page
            iframes = soup.find_all("iframe")
            for iframe in iframes:
                src = iframe.get("src") or iframe.get("data-src")
                if src: candidate_links.append(src)

            # Method 3: Player containers
            player_containers = soup.find_all(["div", "section"], class_=lambda x: x and ("player" in " ".join(x).lower() or "video" in " ".join(x).lower() or "embed" in " ".join(x).lower()) if x else False)
            for container in player_containers:
                links = container.find_all("a", href=True)
                for link in links:
                    href = link.get("href")
                    if href: candidate_links.append(href)
                # Data attributes
                for attr in ['data-src', 'data-url', 'data-video', 'data-embed', 'data-frame']:
                    val = container.get(attr)
                    if val: candidate_links.append(val)

            # Method 4: Script processing for this page (enhanced)
            script_tags = soup.find_all("script")
            for script in script_tags:
                script_text = script.string or ""
                if not script_text:
                    continue
                
                # Enhanced URL patterns for video links
                url_patterns = [
                    r'["\'](https?://[^"\']*(?:embed|player|video|watch|stream|play|server|cdn|mp4|m3u8|webm|mkv|flv)[^"\']*)["\']',
                    r'src["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'file["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'video["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'source["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'fileurl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'video_url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'videoUrl["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    r'getElementById\(["\']([^"\']+)["\']\)\.src\s*=\s*["\'](https?://[^"\']+)["\']',
                    r'\.setAttribute\(["\']src["\'],\s*["\'](https?://[^"\']+)["\']',
                    r'iframe\.src\s*=\s*["\'](https?://[^"\']+)["\']',
                ]
                
                for pattern in url_patterns:
                    matches = re.findall(pattern, script_text, re.IGNORECASE)
                    for match in matches:
                        # Handle tuple matches
                        url = match[1] if isinstance(match, tuple) and len(match) > 1 else match
                        if isinstance(match, tuple):
                            url = match[1] if len(match) > 1 else match[0]
                        else:
                            url = match
                        
                        if url and url.startswith(('http://', 'https://')):
                            # Filter out non-video URLs
                            exclude_keywords = ['facebook', 'twitter', 'instagram', 'google', 'ads', 'analytics', 'tracking', 'login', 'signup']
                            if not any(exclude in url.lower() for exclude in exclude_keywords):
                                # Check if it looks like a video URL
                                video_indicators = ['stream', 'embed', 'player', 'watch', 'video', 'play', 'server', 'cdn', 'mp4', 'm3u8', 'webm', 'mkv', 'dood', 'streamtape', 'mixdrop', 'trembed']
                                if any(indicator in url.lower() for indicator in video_indicators):
                                    candidate_links.append(url)

            # Process candidates
            processed_candidates = set()
            
            for href in candidate_links:
                if not href: continue
                # Normalize URL
                if href.startswith("//"): href = "https:" + href
                elif href.startswith("/"): href = base_url + href
                elif not href.startswith(('http://', 'https://')): continue

                if href in processed_candidates: continue
                processed_candidates.add(href)

                # Filter junk
                exclude_keywords = ['login', 'signup', 'advert', 'facebook', 'twitter', 'instagram', 'google', 'analytics', 'wp-json', '#']
                if any(exclude in href.lower() for exclude in exclude_keywords): continue

                # Deep Resolution for internal embeds
                resolved_url = None
                if 'trembed=' in href or '/embed/' in href:
                    # Resolve this link using the helper function
                    resolved_url = await resolve_embed_to_direct(client, href, max_depth=2)
                    if resolved_url:
                        if resolved_url not in streaming_links:
                            streaming_links.append(str(resolved_url))
                        # Will add to servers list below
                        href = resolved_url  # Use resolved URL for server creation
                
                # Add if it looks like a video/stream link
                # Common domains or keywords
                is_video = False
                video_keywords = ['mp4', 'm3u8', 'stream', 'cdn', 'video', 'watch', 'player', 'drive.google', 'dood', 'vid', 'upload']
                if any(k in href.lower() for k in video_keywords):
                    is_video = True
                
                if is_video or 'trembed' in href: # Keep trembed if resolution didn't happen or just as fallback
                     if href not in streaming_links:
                         streaming_links.append(str(href)) # Explicit str conversion

            # Create structured server objects from streaming links
            servers = []
            server_counter = 1
            processed_urls = set()
            
            for href in streaming_links:
                if href in processed_urls:
                    continue
                processed_urls.add(href)
                
                # Try to extract server name from URL or use default
                server_name = f"Server {server_counter}"
                # Check if we can extract from any watch links
                for watch_link in watch_links:
                    watch_href = watch_link.get("href")
                    if watch_href and (watch_href == href or str(watch_href) == str(href)):
                        server_name = extract_server_name(watch_link, server_name)
                        break
                
                servers.append(StreamingServer(
                    name=server_name,
                    url=href,
                    quality=extract_quality("", href),
                    type=get_server_type(href)
                ))
                server_counter += 1
            
            # Deep resolve all embed URLs to direct playable links
            resolved_servers = []
            for server in servers:
                server_url_str = str(server.url)
                if server.type in ['embed', 'iframe'] or '/embed/' in server_url_str.lower() or 'embed' in server_url_str.lower() or 'trembed' in server_url_str.lower():
                    # Try to resolve embed URL to direct link
                    resolved_url = await resolve_embed_to_direct(client, server_url_str, max_depth=3)
                    if resolved_url and resolved_url != server_url_str:
                        # Use resolved URL if different
                        resolved_servers.append(StreamingServer(
                            name=server.name,
                            url=resolved_url,
                            quality=server.quality,
                            type="direct" if resolved_url else server.type
                        ))
                    else:
                        # Keep original if resolution failed or same
                        resolved_servers.append(server)
                else:
                    # Keep non-embed links as-is
                    resolved_servers.append(server)
            
            # Update movie data with resolved servers
            movie_data['servers'] = resolved_servers
            # Also update legacy streaming_links with resolved URLs
            movie_data['streaming_links'] = [str(server.url) for server in resolved_servers]
            
            if not movie_data.get('title') or not movie_data.get('url'):
                logger.warning(f"Failed to parse movie data for {movie_slug}")
                raise HTTPException(status_code=404, detail=f"Movie not found: {movie_slug}")
            
            logger.info(f"Scraped movie detail for '{title}': {len(resolved_servers)} streaming links found (all resolved to direct playable URLs)")
            return ToonstreamMovieDetail(**movie_data)
            
        except HTTPStatusError as e:
            logger.error(f"HTTP error while scraping {url}: {e}")
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Movie not found: {movie_slug}")
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except Exception as e:
            logger.exception(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")