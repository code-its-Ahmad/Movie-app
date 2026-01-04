#  app.py
import logging
from fastapi import FastAPI, HTTPException, Depends, Path, Query
from fastapi.responses import JSONResponse
from typing import List, Optional
import re
from models import   Anime, Episode, Movie, ErrorResponse, Series, AnimeSeriesDetail, ToonstreamMovieDetail, SeriesDetail
from scraper import (
    scrape_anime_by_category,
    scrape_anime_data,
    scrape_episode_data,
    scrape_movie_data,
    get_http_client,
    scrape_movies_by_year_page,
    scrape_movies_by_genre_page,
    scrape_movies_by_director_page,
    scrape_series_page,
    scrape_series_search,
    scrape_anime_series_detail,
    scrape_anime_episode_detail,
    normalize_series_slug,
    extract_series_slug_from_url,
    scrape_toonstream_movies_page,
    scrape_toonstream_movie_detail,
    normalize_movie_slug,
    extract_movie_slug_from_url,
    scrape_series_detail,
)
from httpx import AsyncClient

# Configure logging
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Movie Scraper API",
    description="API to scrape movie and series data from hindilinks4u.host and anime from toonstream.one. Supports filtering by language.",
    version="1.0.0"
)

# Supported languages for validation
SUPPORTED_LANGUAGES = {'hindi', 'english', 'tamil', 'telugu', 'malayalam'}

# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "Movie Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "movies": {
                "search": "/search-movie?search_term={term}&language={lang}&max_pages={pages}",
                "by_year": "/release-year/{year}/{page}",
                "by_genre": "/genre/{genre}/{page}",
                "by_director": "/director/{director}/{page}"
            },
            "series": {
                "list": "/series/{page}",
                "search": "/series/search/{search_series}"
            },
            "episodes": {
                "get": "/episode/{series_slug}/{season}/{episode}?language={lang}"
            },
            "anime": {
                "search": "/search-anime?search_term={term}",
                "by_category": "/anime/category/{category}/{page}?type={movies|series}",
                "series_detail": "/anime/series/{series_slug}",
                "episode_detail": "/anime/episode/{series_slug}/{season}/{episode}"
            },
            "toonstream_movies": {
                "list": "/toonstream/movies/{page}",
                "detail": "/toonstream/movie/{movie_slug}"
            }
        },
        "documentation": "/docs"
    }

# Search movies endpoint
@app.get(
    "/search-movie",
    response_model=List[Movie],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid search term or language"},
        404: {"model": ErrorResponse, "description": "No movies found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Search for movies",
    description="Search for movies by name and optionally filter by language. Example: `?search_term=Avengers&language=Hindi&max_pages=3`"
)
async def search_movies(
    search_term: str = Query(..., description="Search term for movies"),
    language: Optional[str] = Query(None, description="Filter by language (e.g., Hindi, English, Tamil)"),
    max_pages: int = Query(3, ge=1, le=10, description="Maximum number of pages to scrape"),
    client: AsyncClient = Depends(get_http_client)
):
    search_term = re.sub(r'[^\w\s]', '', search_term.strip())
    if not search_term:
        raise HTTPException(status_code=400, detail="Search term cannot be empty or invalid")
    
    if language:
        language = re.sub(r'[^\w\s]', '', language.strip().lower())
        if not language or language not in SUPPORTED_LANGUAGES:
            raise HTTPException(status_code=400, detail=f"Invalid language. Supported languages: {', '.join(SUPPORTED_LANGUAGES)}")
    
    movies = await scrape_movie_data(search_term, client, language=language, max_pages=max_pages)
    return movies

# Get movies by release year and page
@app.get(
    "/release-year/{year}/{page}",
    response_model=List[Movie],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid year or page provided"},
        404: {"model": ErrorResponse, "description": "No movies found for the specified year and page"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get movies by release year and page",
    description="Fetch movies released in the specified year and page from hindilinks4u.host/release-year/{year}/page/{page}/"
)
async def get_movies_by_year_page(
    year: int = Path(..., ge=1900, le=2025, description="Release year for movies"),
    page: int = Path(..., ge=1, description="Page number to fetch"),
    client: AsyncClient = Depends(get_http_client)
):
    movies = await scrape_movies_by_year_page(year, page, client)
    return movies

# Get movies by genre and page
@app.get(
    "/genre/{genre}/{page}",
    response_model=List[Movie],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid genre or page provided"},
        404: {"model": ErrorResponse, "description": "No movies found for the specified genre and page"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get movies by genre and page",
    description="Fetch movies for the specified genre and page from hindilinks4u.host/genre/{genre}/page/{page}/"
)
async def get_movies_by_genre_page(
    genre: str = Path(..., description="Genre to fetch movies for (e.g., top-rated, action, drama)"),
    page: int = Path(..., ge=1, description="Page number to fetch"),
    client: AsyncClient = Depends(get_http_client)
):
    genre = re.sub(r'[^\w-]', '', genre.strip().lower())
    if not genre:
        raise HTTPException(status_code=400, detail="Genre cannot be empty or invalid")
    
    movies = await scrape_movies_by_genre_page(genre, page, client)
    return movies

# Get movies by director and page
@app.get(
    "/director/{director}/{page}",
    response_model=List[Movie],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid director or page provided"},
        404: {"model": ErrorResponse, "description": "No movies found for the specified director and page"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get movies by director and page",
    description="Fetch movies for the specified director and page from hindilinks4u.host/director/{director}/page/{page}/"
)
async def get_movies_by_director_page(
    director: str = Path(..., description="Director to fetch movies for (e.g., christopher-nolan)"),
    page: int = Path(..., ge=1, description="Page number to fetch"),
    client: AsyncClient = Depends(get_http_client)
):
    director = re.sub(r'[^\w-]', '', director.strip().lower())
    if not director:
        raise HTTPException(status_code=400, detail="Director cannot be empty or invalid")
    
    movies = await scrape_movies_by_director_page(director, page, client)
    return movies

# Get series by page
@app.get(
    "/series/{page}",
    response_model=List[Series],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid page provided"},
        404: {"model": ErrorResponse, "description": "No series found for the specified page"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get series by page",
    description="Fetch series from hindilinks4u.host/series/page/{page}/"
)
async def get_series_page(
    page: int = Path(..., ge=1, description="Page number to fetch"),
    client: AsyncClient = Depends(get_http_client)
):
    series = await scrape_series_page(page, client)
    return series

@app.get(
    "/series/search/{search_series}",
    response_model=List[Series],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid search query provided"},
        404: {"model": ErrorResponse, "description": "No series found for the search query"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Search series by name",
    description="Search for series on hindilinks4u.host/series/ using a search query"
)
async def search_series(
    search_series: str = Path(..., min_length=1, description="Series name to search for"),
    client: AsyncClient = Depends(get_http_client)
):
    series = await scrape_series_search(search_series, client)
    return series

@app.get(
    "/series/detail/{series_slug}",
    response_model=SeriesDetail,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid series slug provided"},
        404: {"model": ErrorResponse, "description": "Series not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get series detail with episodes",
    description="Fetch detailed information about a series from hindilinks4u.host/series/{series_slug}/ including full episode list. Example: `/series/detail/the-vanishing-triangle`"
)
async def get_series_detail(
    series_slug: str = Path(..., description="Series slug identifier"),
    client: AsyncClient = Depends(get_http_client)
):
    # Normalize slug
    series_slug = re.sub(r'[^\w-]', '', series_slug.strip().lower())
    if not series_slug:
        raise HTTPException(status_code=400, detail="Series slug cannot be empty or invalid")
    
    series_detail = await scrape_series_detail(series_slug, client)
    return series_detail

# Get episode data endpoint
@app.get(
    "/episode/{series_slug}/{season}/{episode}",
    response_model=Episode,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid series slug, season, episode, or language provided"},
        404: {"model": ErrorResponse, "description": "Episode not found or language mismatch"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get episode data",
    description="Fetch data for a specific episode of a series from hindilinks4u.host/episode/{series_slug}-season-{season}-episode-{episode}/. Optionally filter by language."
)
async def get_episode_data(
    series_slug: str = Path(..., description="Series slug (e.g., 'the-vanishing-triangle')"),
    season: int = Path(..., ge=1, description="Season number"),
    episode: int = Path(..., ge=1, description="Episode number"),
    language: Optional[str] = Query(None, description="Filter by language (e.g., Hindi, English, Tamil)"),
    client: AsyncClient = Depends(get_http_client)
):
    # Clean and validate series slug
    series_slug = re.sub(r'\s+', '-', series_slug.strip().lower())
    series_slug = re.sub(r'[^\w-]', '', series_slug)
    if not series_slug or not re.match(r'^[\w-]+$', series_slug):
        raise HTTPException(status_code=400, detail="Series slug cannot be empty or invalid")

    # Validate language
    SUPPORTED_LANGUAGES = {'hindi', 'english', 'tamil', 'telugu', 'malayalam'}
    if language:
        language = re.sub(r'[^\w\s]', '', language.strip().lower())
        if not language or language not in SUPPORTED_LANGUAGES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid language. Supported languages: {', '.join(SUPPORTED_LANGUAGES)}"
            )

    logger.info(f"Processing request for series: {series_slug}, season: {season}, episode: {episode}, language: {language}")
    episode_data = await scrape_episode_data(series_slug, season, episode, client, language)
    return episode_data

@app.get(
    "/search-anime",
    response_model=List[Anime],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid search term or language"},
        404: {"model": ErrorResponse, "description": "No anime found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Search for anime",
    description="Search for anime by name on toonstream.one. Example: `?search_term=Naruto`"
)
async def search_anime(
    search_term: str = Query(..., description="Search term for anime"),
    client: AsyncClient = Depends(get_http_client)
):
    search_term = re.sub(r'[^\w\s]', '', search_term.strip())
    if not search_term:
        raise HTTPException(status_code=400, detail="Search term cannot be empty or invalid")
    
    anime = await scrape_anime_data(search_term, client)
    return anime

@app.get(
    "/anime/category/{anime}/{page}",
    response_model=List[Anime],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid category or page provided"},
        404: {"model": ErrorResponse, "description": "No anime found for the specified category and page"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get anime by category and page",
    description="Fetch anime for the specified category and page from toonstream.one/category/{anime}/page/{page}/. Optional query parameter 'type' can be 'movies' or 'series' to filter content. Example: `/anime/category/hungama/1?type=movies`"
)
async def get_anime_by_category_page(
    anime: str = Path(..., description="Anime category to fetch (e.g., action, adventure, hungama)"),
    page: int = Path(..., ge=1, description="Page number to fetch"),
    type: Optional[str] = Query(None, description="Filter by type: 'movies' or 'series'"),
    client: AsyncClient = Depends(get_http_client)
):
    # Clean and validate category slug
    anime = re.sub(r'[^\w-]', '', anime.strip().lower())
    if not anime or not re.match(r'^[\w-]+$', anime):
        raise HTTPException(status_code=400, detail="Category cannot be empty or invalid")
    
    # Validate type parameter if provided
    if type:
        type = type.lower().strip()
        if type not in ['movies', 'series']:
            raise HTTPException(status_code=400, detail="Type parameter must be 'movies' or 'series'")
    
    anime_list = await scrape_anime_by_category(anime, page, client, type=type)
    return anime_list

@app.get(
    "/anime/series/{series_slug}",
    response_model=AnimeSeriesDetail,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid series slug provided"},
        404: {"model": ErrorResponse, "description": "Series not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get anime series detail",
    description="Fetch detailed information about an anime series from toonstream.one/series/{series_slug}/ including episodes list. Example: `/anime/series/a-gatherers-adventure-in-isekai`"
)
async def get_anime_series_detail(
    series_slug: str = Path(..., description="Series slug identifier (e.g., 'a-gatherers-adventure-in-isekai') or title"),
    client: AsyncClient = Depends(get_http_client)
):
    # Store original input for fallback search if needed
    original_title = series_slug
    
    # If input is a URL, extract the slug from it
    if series_slug.startswith('http') or series_slug.startswith('/'):
        extracted_slug = extract_series_slug_from_url(series_slug)
        if extracted_slug:
            series_slug = extracted_slug
        else:
            # If URL extraction failed, normalize it as a title
            series_slug = normalize_series_slug(series_slug)
    else:
        # Normalize the slug (handles titles like "A Wild Last Boss Appeared")
        series_slug = normalize_series_slug(series_slug)
    
    if not series_slug or not re.match(r'^[\w-]+$', series_slug):
        raise HTTPException(status_code=400, detail="Series slug cannot be empty or invalid")
    
    # Pass original title for fallback search
    series_detail = await scrape_anime_series_detail(series_slug, client, original_title=original_title)
    return series_detail

@app.get(
    "/anime/episode/{series_slug}/{season}/{episode}",
    response_model=Episode,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid series slug, season, or episode provided"},
        404: {"model": ErrorResponse, "description": "Episode not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get anime episode detail",
    description="Fetch detailed information about an anime episode from toonstream.one/episode/{series_slug}-{season}x{episode}/ including streaming links. Example: `/anime/episode/tojima-wants-to-be-a-kamen-rider/1/2`"
)
async def get_anime_episode_detail(
    series_slug: str = Path(..., description="Series slug identifier (e.g., 'tojima-wants-to-be-a-kamen-rider') or title"),
    season: int = Path(..., ge=1, description="Season number"),
    episode: int = Path(..., ge=1, description="Episode number"),
    client: AsyncClient = Depends(get_http_client)
):
    # Normalize the slug
    series_slug = normalize_series_slug(series_slug)
    
    if not series_slug or not re.match(r'^[\w-]+$', series_slug):
        raise HTTPException(status_code=400, detail="Series slug cannot be empty or invalid")
    
    episode_detail = await scrape_anime_episode_detail(series_slug, season, episode, client)
    return episode_detail

@app.get(
    "/toonstream/movies/{page}",
    response_model=List[Anime],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid page provided"},
        404: {"model": ErrorResponse, "description": "No movies found for the specified page"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get toonstream movies by page",
    description="Fetch movies from toonstream.one/movies/ page. Example: `/toonstream/movies/1`"
)
async def get_toonstream_movies_page(
    page: int = Path(..., ge=1, description="Page number to fetch"),
    client: AsyncClient = Depends(get_http_client)
):
    movies = await scrape_toonstream_movies_page(page, client)
    return movies

@app.get(
    "/toonstream/movie/{movie_slug}",
    response_model=ToonstreamMovieDetail,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid movie slug provided"},
        404: {"model": ErrorResponse, "description": "Movie not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Failed to fetch data from source"},
        503: {"model": ErrorResponse, "description": "Network error"},
    },
    summary="Get toonstream movie detail",
    description="Fetch detailed information about a movie from toonstream.one/movies/{movie_slug}/ including streaming links. Example: `/toonstream/movie/crayon-shin-chan-the-movie-super-hot-the-spicy-kasukabe-dancers`"
)
async def get_toonstream_movie_detail(
    movie_slug: str = Path(..., description="Movie slug identifier (e.g., 'crayon-shin-chan-the-movie-super-hot-the-spicy-kasukabe-dancers') or title"),
    client: AsyncClient = Depends(get_http_client)
):
    # Normalize the slug
    movie_slug = normalize_movie_slug(movie_slug)
    
    if not movie_slug or not re.match(r'^[\w-]+$', movie_slug):
        raise HTTPException(status_code=400, detail="Movie slug cannot be empty or invalid")
    
    movie_detail = await scrape_toonstream_movie_detail(movie_slug, client)
    return movie_detail