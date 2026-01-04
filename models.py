# models.py
from pydantic import BaseModel, HttpUrl, Field
from typing import List, Optional

class Movie(BaseModel):
    title: str = Field(..., description="Movie title")
    url: HttpUrl = Field(..., description="Movie page URL")
    image: Optional[HttpUrl] = Field(default=None, description="Movie poster URL")
    quality: Optional[str] = Field(default=None, description="Movie quality or episode count")
    imdb: Optional[str] = Field(default=None, description="IMDb rating")
    year: Optional[str] = Field(default=None, description="Release year")
    duration: Optional[str] = Field(default=None, description="Movie duration")
    description: Optional[str] = Field(default=None, description="Movie description")
    genres: List[str] = Field(default_factory=list, description="List of genres")
    language: Optional[str] = Field(default=None, description="Language of the movie")

    class Config:
        from_attributes = True

class Series(BaseModel):
    title: str = Field(..., description="Series title")
    url: HttpUrl = Field(..., description="Series page URL")
    image: Optional[HttpUrl] = Field(default=None, description="Series poster URL")
    quality: Optional[str] = Field(default=None, description="Series quality")
    imdb: Optional[str] = Field(default=None, description="IMDb rating")
    year: Optional[str] = Field(default=None, description="Release year")
    duration: Optional[str] = Field(default=None, description="Episode duration")
    description: Optional[str] = Field(default=None, description="Series description")
    genres: List[str] = Field(default_factory=list, description="List of genres")
    seasons: Optional[str] = Field(default=None, description="Number of seasons")
    episodes: Optional[str] = Field(default=None, description="Number of episodes")
    language: Optional[str] = Field(default=None, description="Language of the series")

    class Config:
        from_attributes = True

class Episode(BaseModel):
    title: str = Field(..., description="Episode title")
    url: HttpUrl = Field(..., description="Episode page URL")
    series_title: str = Field(..., description="Series title")
    season: str = Field(..., description="Season number")
    episode_number: str = Field(..., description="Episode number")
    description: Optional[str] = Field(default=None, description="Episode description")
    image: Optional[HttpUrl] = Field(default=None, description="Episode thumbnail URL")
    streaming_links: List[HttpUrl] = Field(default_factory=list, description="Streaming or download links")
    duration: Optional[str] = Field(default=None, description="Episode duration")
    language: Optional[str] = Field(default=None, description="Language of the episode")

    class Config:
        from_attributes = True

class Anime(BaseModel):
    title: str = Field(..., description="Anime title")
    url: HttpUrl = Field(..., description="Anime page URL")
    image: Optional[HttpUrl] = Field(None, description="Anime poster URL")
    description: Optional[str] = Field(None, description="Anime description")
    genres: List[str] = Field(default_factory=list, description="List of genres")
    language: Optional[str] = Field(None, description="Language of the anime (e.g., Hindi, English, Tamil, Telugu, Multi)")
    year: Optional[str] = Field(None, description="Release year")
    rating: Optional[str] = Field(None, description="TMDB or other rating")

    class Config:
        from_attributes = True

class AnimeEpisode(BaseModel):
    title: str = Field(..., description="Episode title")
    url: HttpUrl = Field(..., description="Episode page URL")
    season: str = Field(..., description="Season number")
    episode_number: str = Field(..., description="Episode number")
    image: Optional[HttpUrl] = Field(None, description="Episode thumbnail URL")
    aired_date: Optional[str] = Field(None, description="Episode aired date")

    class Config:
        from_attributes = True

class AnimeSeriesDetail(BaseModel):
    title: str = Field(..., description="Anime series title")
    url: HttpUrl = Field(..., description="Series page URL")
    image: Optional[HttpUrl] = Field(None, description="Series poster URL")
    description: Optional[str] = Field(None, description="Series description")
    genres: List[str] = Field(default_factory=list, description="List of genres")
    languages: List[str] = Field(default_factory=list, description="List of available languages")
    year: Optional[str] = Field(None, description="Release year")
    rating: Optional[str] = Field(None, description="TMDB or other rating")
    quality: Optional[str] = Field(None, description="Available quality options")
    duration: Optional[str] = Field(None, description="Episode duration")
    seasons: Optional[str] = Field(None, description="Number of seasons")
    episodes_count: Optional[str] = Field(None, description="Total number of episodes")
    cast: List[str] = Field(default_factory=list, description="List of cast members")
    episodes: List[AnimeEpisode] = Field(default_factory=list, description="List of episodes")

    class Config:
        from_attributes = True

class ToonstreamMovieDetail(BaseModel):
    title: str = Field(..., description="Movie title")
    url: HttpUrl = Field(..., description="Movie page URL")
    image: Optional[HttpUrl] = Field(None, description="Movie poster URL")
    description: Optional[str] = Field(None, description="Movie description")
    genres: List[str] = Field(default_factory=list, description="List of genres")
    languages: List[str] = Field(default_factory=list, description="List of available languages")
    year: Optional[str] = Field(None, description="Release year")
    rating: Optional[str] = Field(None, description="TMDB rating")
    quality: Optional[str] = Field(None, description="Available quality options")
    duration: Optional[str] = Field(None, description="Movie duration")
    director: Optional[str] = Field(None, description="Director name")
    cast: List[str] = Field(default_factory=list, description="List of cast members")
    streaming_links: List[HttpUrl] = Field(default_factory=list, description="Streaming or download links from multiple servers")

    class Config:
        from_attributes = True

class SeriesEpisode(BaseModel):
    title: str = Field(..., description="Episode title")
    url: HttpUrl = Field(..., description="Episode page URL")
    season: str = Field(..., description="Season number")
    episode_number: str = Field(..., description="Episode number")
    image: Optional[HttpUrl] = Field(None, description="Episode thumbnail URL")
    duration: Optional[str] = Field(None, description="Episode duration")
    language: Optional[str] = Field(None, description="Language of the episode")

    class Config:
        from_attributes = True

class SeriesDetail(BaseModel):
    title: str = Field(..., description="Series title")
    url: HttpUrl = Field(..., description="Series page URL")
    image: Optional[HttpUrl] = Field(None, description="Series poster URL")
    description: Optional[str] = Field(None, description="Series description")
    genres: List[str] = Field(default_factory=list, description="List of genres")
    languages: List[str] = Field(default_factory=list, description="List of available languages")
    year: Optional[str] = Field(None, description="Release year")
    imdb: Optional[str] = Field(None, description="IMDb rating")
    quality: Optional[str] = Field(None, description="Available quality options")
    duration: Optional[str] = Field(None, description="Episode duration")
    seasons: Optional[str] = Field(None, description="Number of seasons")
    episodes_count: Optional[str] = Field(None, description="Total number of episodes")
    cast: List[str] = Field(default_factory=list, description="List of cast members")
    episodes: List[SeriesEpisode] = Field(default_factory=list, description="List of episodes")

    class Config:
        from_attributes = True

class MovieDetail(BaseModel):
    title: str = Field(..., description="Movie title")
    url: HttpUrl = Field(..., description="Movie page URL")
    image: Optional[HttpUrl] = Field(None, description="Movie poster URL")
    description: Optional[str] = Field(None, description="Movie description")
    genres: List[str] = Field(default_factory=list, description="List of genres")
    languages: List[str] = Field(default_factory=list, description="List of available languages")
    year: Optional[str] = Field(None, description="Release year")
    imdb: Optional[str] = Field(None, description="IMDb rating")
    quality: Optional[str] = Field(None, description="Available quality options")
    duration: Optional[str] = Field(None, description="Movie duration")
    director: Optional[str] = Field(None, description="Director name")
    cast: List[str] = Field(default_factory=list, description="List of cast members")
    streaming_links: List[HttpUrl] = Field(default_factory=list, description="Streaming or download links from multiple servers")

    class Config:
        from_attributes = True

class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")
    code: Optional[int] = Field(None, description="HTTP status code")
    details: Optional[str] = Field(None, description="Additional error details")

    class Config:
        from_attributes = True