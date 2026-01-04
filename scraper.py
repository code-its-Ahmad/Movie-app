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
import requests
from fastapi import HTTPException
from typing import List, Optional, Tuple
from models import  Anime, Episode, Movie, Series, AnimeSeriesDetail, AnimeEpisode, ToonstreamMovieDetail, SeriesDetail, SeriesEpisode, MovieDetail

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base URLs for scraping
HINDILINKS_BASE_URL = "https://hindilinks4u.host"
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

    streaming_links = []
    # Comprehensive video server extraction
    # 1. Look for player containers and iframes
    player_containers = soup.find_all(["div", "section"], class_=lambda x: x and ("player" in " ".join(x).lower() or "video" in " ".join(x).lower() or "embed" in " ".join(x).lower()) if x else False)
    for container in player_containers:
        # Check for iframe sources
        iframes = container.find_all("iframe")
        for iframe in iframes:
            src = iframe.get("src") or iframe.get("data-src")
            if src and src.startswith(('http://', 'https://')):
                if src not in streaming_links:
                    streaming_links.append(src)
        # Check for video sources
        videos = container.find_all("video")
        for video in videos:
            src = video.get("src")
            if src and src.startswith(('http://', 'https://')):
                if src not in streaming_links:
                    streaming_links.append(src)
            source_tags = video.find_all("source")
            for source in source_tags:
                src = source.get("src")
                if src and src.startswith(('http://', 'https://')):
                    if src not in streaming_links:
                        streaming_links.append(src)
    
    # 2. Look for server selection buttons/links
    server_selectors = [
        ('a', {'class': lambda x: x and ('server' in " ".join(x).lower() or 'link' in " ".join(x).lower() or 'watch' in " ".join(x).lower()) if x else False}),
        ('button', {'class': lambda x: x and ('server' in " ".join(x).lower() or 'play' in " ".join(x).lower()) if x else False}),
        ('div', {'class': lambda x: x and ('server' in " ".join(x).lower() or 'player-option' in " ".join(x).lower()) if x else False}),
        ('a', {'class': 'link'}),
        ('a', {'class': 'watch-link'}),
        ('a', {'class': 'stream-link'}),
        ('div', {'class': 'player-options'}),
    ]
    
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
                    
                    # Filter out unwanted links
                    exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram', 'youtube.com/channel']
                    if not any(exclude in href.lower() for exclude in exclude_keywords):
                        # Check if it's a valid streaming link
                        streaming_domains = ['stream', 'embed', 'player', 'watch', 'video', 'play', 'server', 'cdn', 'mp4', 'm3u8']
                        if any(domain in href.lower() for domain in streaming_domains) or 'episode' in href.lower() or 'movie' in href.lower():
                            if href not in streaming_links:
                                streaming_links.append(href)
    
    # 3. Look for data attributes with video URLs
    data_attrs = ['data-url', 'data-src', 'data-link', 'data-video', 'data-embed']
    for attr in data_attrs:
        elements = soup.find_all(attrs={attr: True})
        for elem in elements:
            data_url = elem.get(attr)
            if data_url and data_url.startswith(('http://', 'https://')):
                if data_url not in streaming_links:
                    streaming_links.append(data_url)
    
    # 4. Extract from script tags (JSON data, embedded URLs)
    script_tags = soup.find_all("script")
    for script in script_tags:
        if script.string:
            # Look for URLs in script content
            url_patterns = [
                r'["\'](https?://[^"\']*(?:stream|embed|player|watch|video|play|server|cdn|mp4|m3u8)[^"\']*)["\']',
                r'url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                r'src["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
            ]
            for pattern in url_patterns:
                matches = re.findall(pattern, script.string, re.IGNORECASE)
                for match in matches:
                    if match.startswith(('http://', 'https://')):
                        exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram']
                        if not any(exclude in match.lower() for exclude in exclude_keywords):
                            if match not in streaming_links:
                                streaming_links.append(match)
    
    # 5. Fallback: search for any links with streaming keywords
    if not streaming_links:
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
                    
                if any(keyword in text or keyword in href.lower() for keyword in ['watch', 'stream', 'play', 'download', 'server', 'embed', 'player']):
                    exclude_keywords = ['login', 'signup', 'advert', 'advertisement', 'facebook', 'twitter', 'instagram']
                    if not any(exclude in href.lower() for exclude in exclude_keywords):
                        if href not in streaming_links:
                            streaming_links.append(href)
    
    episode_data['streaming_links'] = streaming_links

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

    url = f"{HINDILINKS_BASE_URL}/episode/{series_slug}-season-{season}-episode-{episode}/"
    semaphore = asyncio.Semaphore(5)

    async with semaphore:
        logger.info(f"Scraping episode URL: {url}")
        await asyncio.sleep(1)
        try:
            response = await client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            episode_data = parse_episode(soup, logger, series_slug, season, episode)

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

    # 2. Episodes Extraction
    episodes = []
    seasons_count = 0
    total_episodes_count = 0
    
    seasons_divs = soup.find_all('div', class_='se-c')
    if not seasons_divs:
        # Fallback for some layouts without 'se-c' container
        seasons_divs = soup.find_all('div', id=lambda x: x and x.startswith('season-'))
        
    for season_div in seasons_divs:
        seasons_count += 1
        
        season_num_tag = season_div.find('span', class_='se-t') or season_div.find('div', class_='se-q')
        season_num = "1"
        if season_num_tag:
            # Try to extract number
            txt = season_num_tag.text.strip()
            # Extract digits
            digits = re.findall(r'\d+', txt)
            if digits:
                season_num = digits[0]
        
        episode_list = season_div.find_all('li')
        for ep in episode_list:
            total_episodes_count += 1
            
            ep_link = ep.find('a')
            if not ep_link:
                continue
                
            ep_url = ep_link.get('href')
            ep_title = ep_link.text.strip()
            
            # Refine ep title if it's just "Episode 1"
            ep_img_tag = ep.find('img')
            ep_image_url = None
            if ep_img_tag:
                ep_image_url = ep_img_tag.get('src') or ep_img_tag.get('data-src')
                if ep_image_url:
                     if ep_image_url.startswith('//'):
                        ep_image_url = 'https:' + ep_image_url
            
            desc_div = ep.find('div', class_='episodiotitle')
            # Extract episode number from URL or title
            # Expected URL format: .../episode/{series}-season-{s}-episode-{e}/
            ep_num = str(total_episodes_count) # Fallback
            
            try:
                if 'episode-' in ep_url:
                    ep_num = ep_url.rstrip('/').split('episode-')[-1]
                elif 'episode-' in ep_title.lower():
                     match = re.search(r'episode\s*(\d+)', ep_title, re.IGNORECASE)
                     if match:
                         ep_num = match.group(1)
            except Exception:
                pass
                
            episodes.append(SeriesEpisode(
                title=ep_title,
                url=ep_url,
                season=season_num,
                episode_number=ep_num,
                image=ep_image_url,
                duration=None, # Usually not visible in list
                language=None 
            ))
            
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
async def scrape_series_detail(series_slug: str, client: AsyncClient) -> SeriesDetail:
    url = f"{HINDILINKS_BASE_URL}/series/{series_slug}/"
    semaphore = asyncio.Semaphore(5)
    
    async with semaphore:
        logger.info(f"Scraping series detail URL: {url}")
        try:
            response = await client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            return parse_series_detail(soup, logger, series_slug)
            
        except HTTPStatusError as e:
            logger.error(f"HTTP error while scraping {url}: {e}")
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail="Series not found")
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")

def scrape_anime_data_sync(search_term: str) -> List[Anime]:
    base_url = TOONSTREAM_BASE_URL.rstrip('/')
    formatted_search_term = quote(search_term)
    url = f"{base_url}/home/?s={formatted_search_term}"
    
    logger.info(f"Scraping anime search URL: {url}")
    
    try:
        response = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Try multiple selectors to find anime items (same logic as async version)
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
        
        if not articles:
            logger.warning(f"No anime found for search term: {search_term}")
            raise HTTPException(status_code=404, detail=f"No anime found for search term: {search_term}")
        
        anime_list = []
        for article in articles:
            try:
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
                
                if not series_url:
                    continue
                
                language = None
                for lang in ['hindi', 'english', 'tamil', 'telugu', 'malayalam']:
                    if lang in title.lower():
                        language = lang
                        break
                
                anime_data = {
                    "title": title,
                    "url": series_url,
                    "image": image_url,
                    "rating": str(tmdb_rating) if tmdb_rating else None,
                    "language": language,
                    "description": None,
                    "genres": [],
                    "year": None
                }
                
                anime_list.append(Anime(**anime_data))
            except Exception as e:
                logger.error(f"Failed to parse anime item: {e}")
                continue
        
        if not anime_list:
            logger.warning(f"Could not parse any anime items from the page for search term: {search_term}")
            raise HTTPException(status_code=404, detail=f"No anime found for search term: {search_term}")
        
        logger.info(f"Scraped {len(anime_list)} anime for search term: {search_term}")
        return anime_list
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error while scraping {url}: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error while scraping {url}: {e}")
        raise HTTPException(status_code=503, detail=f"Network error: {str(e)}")
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
async def scrape_anime_series_detail(series_slug: str, client: AsyncClient, original_title: Optional[str] = None, _retry_from_search: bool = False) -> AnimeSeriesDetail:
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
        
        # Extract episodes
        episodes = []
        # Look for episode items - they usually have format like "1x1", "1x2", etc.
        # Find all elements that might contain episode information
        episode_items = soup.find_all(["div", "li", "article"], class_=lambda x: x and "episode" in " ".join(x).lower() if x else False)
        
        if not episode_items:
            # Fallback: look for h2 or h3 with episode format (e.g., "1x1", "1x2")
            all_headings = soup.find_all(["h2", "h3"])
            for heading in all_headings:
                heading_text = heading.text.strip()
                # Check if it matches episode format like "1x1" or contains "1x1" pattern
                episode_pattern = re.search(r'(\d+)x(\d+)', heading_text)
                if episode_pattern:
                    # Find parent container
                    parent = heading.find_parent(["div", "li", "article"])
                    if parent:
                        episode_items.append(parent)
        
        # If still not found, try finding items with "View" link and episode pattern
        if not episode_items:
            view_links = soup.find_all("a", string=re.compile(r'View', re.IGNORECASE))
            for link in view_links:
                # Check parent or nearby elements for episode pattern
                parent = link.find_parent(["div", "li", "article", "section"])
                if parent:
                    parent_text = parent.get_text()
                    if re.search(r'\d+x\d+', parent_text):
                        episode_items.append(parent)
        
        # Parse episodes from found items
        for item in episode_items:
            try:
                item_text = item.get_text()
                episode_match = re.search(r'(\d+)x(\d+)', item_text)
                if not episode_match:
                    continue
                
                season_num = episode_match.group(1)
                episode_num = episode_match.group(2)
                
                # Extract episode title
                ep_title_tag = item.find(["h2", "h3", "h4"]) or item.find("a")
                ep_title = ep_title_tag.text.strip() if ep_title_tag and ep_title_tag.text.strip() else f"Episode {episode_num}"
                # Remove episode number from title if present (e.g., "1x1", "1x2")
                ep_title = re.sub(r'\d+x\d+\s*', '', ep_title).strip()
                # If title is empty or just repeats series name, use default
                if not ep_title or ep_title.lower() == title.lower():
                    ep_title = f"Episode {episode_num}"
                # Clean up title (remove extra whitespace, series name if it appears)
                ep_title = re.sub(r'\s+', ' ', ep_title).strip()
                
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
                date_match = re.search(r'(\d+\s*(?:months?|days?|weeks?)\s*ago)', item_text, re.IGNORECASE)
                if date_match:
                    aired_date = date_match.group(1).strip()
                
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
            streaming_links = []
            
            # Method 1: Look for iframe embeds (common for video players)
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
                        if src not in streaming_links:
                            streaming_links.append(src)
            
            # Method 2: Look for video player containers and their links
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
                                if href not in streaming_links:
                                    streaming_links.append(href)
                
                # Also check for data attributes that might contain video URLs
                for attr in ['data-src', 'data-url', 'data-video', 'data-embed']:
                    data_url = container.get(attr)
                    if data_url:
                        if data_url.startswith(('http://', 'https://')):
                            if data_url not in streaming_links:
                                streaming_links.append(data_url)
            
            # Method 3: Look for server/player option links
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
                        if href not in streaming_links:
                            streaming_links.append(href)
            
            # Method 4: Look for embed/video links in script tags (some sites use JavaScript to load players)
            script_tags = soup.find_all("script")
            for script in script_tags:
                if script.string:
                    # Look for URLs in script content
                    url_patterns = [
                        r'["\'](https?://[^"\']+embed[^"\']*)["\']',
                        r'["\'](https?://[^"\']+player[^"\']*)["\']',
                        r'["\'](https?://[^"\']+video[^"\']*)["\']',
                        r'src["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                        r'url["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    ]
                    for pattern in url_patterns:
                        matches = re.findall(pattern, script.string, re.IGNORECASE)
                        for match in matches:
                            if match.startswith(('http://', 'https://')):
                                exclude_keywords = ['facebook', 'twitter', 'instagram', 'google', 'ads', 'analytics']
                                if not any(exclude in match.lower() for exclude in exclude_keywords):
                                    if match not in streaming_links:
                                        streaming_links.append(match)
            
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
                        if href not in streaming_links:
                            streaming_links.append(href)
            
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
                            if src not in streaming_links:
                                streaming_links.append(src)
            
            episode_data['streaming_links'] = streaming_links
            
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

            # Method 4: Script processing for this page
            script_tags = soup.find_all("script")
            for script in script_tags:
                if script.string:
                    # Look for URLs in script content
                    url_patterns = [
                        r'["\'](https?://[^"\']+embed[^"\']*)["\']',
                        r'["\'](https?://[^"\']+player[^"\']*)["\']',
                         r'["\'](https?://[^"\']+video[^"\']*)["\']',
                         r'src["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                         r'file["\']?\s*[:=]\s*["\'](https?://[^"\']+)["\']',
                    ]
                    for pattern in url_patterns:
                        matches = re.findall(pattern, script.string, re.IGNORECASE)
                        for match in matches:
                            if match: candidate_links.append(match)

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
                if 'trembed=' in href  or '/embed/' in href:
                    # Resolve this link
                    resolved = await resolve_embed_url(href)
                    if resolved:
                        if resolved not in streaming_links:
                            streaming_links.append(str(resolved)) # Explicit str conversion
                        continue # If resolved, don't add the wrapper (unless checking if wrapper is useful?)
                        # Actually, keeping the wrapper might be useful if resolution fails, but we want the video.
                        # For now, let's prioritize resolved, but fallback to wrapper if needed.
                        # If resolution worked, we have the better link.
                
                # Add if it looks like a video/stream link
                # Common domains or keywords
                is_video = False
                video_keywords = ['mp4', 'm3u8', 'stream', 'cdn', 'video', 'watch', 'player', 'drive.google', 'dood', 'vid', 'upload']
                if any(k in href.lower() for k in video_keywords):
                    is_video = True
                
                if is_video or 'trembed' in href: # Keep trembed if resolution didn't happen or just as fallback
                     if href not in streaming_links:
                         streaming_links.append(str(href)) # Explicit str conversion

            movie_data['streaming_links'] = streaming_links
            
            if not movie_data.get('title') or not movie_data.get('url'):
                logger.warning(f"Failed to parse movie data for {movie_slug}")
                raise HTTPException(status_code=404, detail=f"Movie not found: {movie_slug}")
            
            logger.info(f"Scraped movie detail for '{title}': {len(streaming_links)} streaming links found")
            return ToonstreamMovieDetail(**movie_data)
            
        except HTTPStatusError as e:
            logger.error(f"HTTP error while scraping {url}: {e}")
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Movie not found: {movie_slug}")
            raise HTTPException(status_code=502, detail=f"Failed to fetch data: {str(e)}")
        except Exception as e:
            logger.exception(f"Unexpected error while scraping {url}: {e}")
            raise HTTPException(status_code=500, detail=f"Scraping error: {str(e)}")