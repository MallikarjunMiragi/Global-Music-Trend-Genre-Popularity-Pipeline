"""
Data Models for Global Music Trend Pipeline
Author: Mallikarjun Miragi
"""

from pydantic import BaseModel, HttpUrl, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class DataSource(str, Enum):
    SPOTIFY_FEATURED = "featured_playlists"
    SPOTIFY_NEW_RELEASES = "new_releases"
    SPOTIFY_CHARTS = "charts"
    LASTFM = "lastfm"
    CUSTOM = "custom"

class AudioFeatures(BaseModel):
    """Audio features for ML analysis"""
    danceability: float = Field(ge=0, le=1, description="How suitable a track is for dancing")
    energy: float = Field(ge=0, le=1, description="Perceptual measure of intensity")
    valence: float = Field(ge=0, le=1, description="Musical positivity/mood")
    acousticness: float = Field(ge=0, le=1, description="Confidence measure of acoustic-ness")
    instrumentalness: float = Field(ge=0, le=1, description="Predicts whether track contains vocals")
    liveness: float = Field(ge=0, le=1, description="Detects presence of audience")
    speechiness: float = Field(ge=0, le=1, description="Detects presence of spoken words")
    tempo: float = Field(gt=0, description="Overall estimated tempo (BPM)")
    key: int = Field(ge=-1, le=11, description="Key the track is in")
    mode: int = Field(ge=0, le=1, description="Modality (major=1, minor=0)")
    time_signature: int = Field(ge=1, le=7, description="Time signature")

class Artist(BaseModel):
    """Artist information"""
    id: str = Field(description="Spotify artist ID")
    name: str = Field(description="Artist name")
    genres: Optional[List[str]] = Field(default=[], description="Artist genres")
    followers: Optional[int] = Field(default=None, description="Follower count")
    popularity: Optional[int] = Field(default=None, ge=0, le=100, description="Artist popularity")

class Album(BaseModel):
    """Album information"""
    id: str = Field(description="Spotify album ID")
    name: str = Field(description="Album name")
    release_date: str = Field(description="Release date (YYYY-MM-DD or YYYY)")
    total_tracks: int = Field(ge=1, description="Number of tracks")
    album_type: Optional[str] = Field(default="album", description="Album type")
    image_url: Optional[HttpUrl] = Field(default=None, description="Album cover image")

class Track(BaseModel):
    """Complete track information"""
    track_id: str = Field(description="Spotify track ID")
    track_name: str = Field(description="Track name")
    artist: str = Field(description="Primary artist name")
    artist_id: Optional[str] = Field(default=None, description="Primary artist ID")
    album: str = Field(description="Album name")
    album_id: str = Field(description="Album ID")
    popularity: int = Field(ge=0, le=100, description="Track popularity score")
    duration_ms: int = Field(gt=0, description="Track duration in milliseconds")
    explicit: bool = Field(description="Whether track has explicit content")
    preview_url: Optional[HttpUrl] = Field(default=None, description="30-second preview URL")
    spotify_url: HttpUrl = Field(description="Spotify track URL")
    image_url: Optional[HttpUrl] = Field(default=None, description="Track/album image")
    release_date: str = Field(description="Release date")
    
    # Metadata
    data_source: DataSource = Field(description="Source of the data")
    playlist_name: Optional[str] = Field(default=None, description="Source playlist name")
    playlist_id: Optional[str] = Field(default=None, description="Source playlist ID")
    fetched_at: datetime = Field(description="When data was fetched")
    
    # Audio features (optional for ML analysis)
    audio_features: Optional[AudioFeatures] = Field(default=None, description="Audio features")

class TrendingResponse(BaseModel):
    """API response for trending tracks"""
    tracks: List[Track]
    total_available: int = Field(description="Total tracks available")
    last_updated: datetime = Field(description="When data was last updated")
    data_sources: Dict[str, int] = Field(description="Distribution of data sources")
    cache_status: Optional[str] = Field(default=None, description="Cache status")

class AnalyticsResponse(BaseModel):
    """API response for analytics"""
    timestamp: datetime
    total_tracks: int
    unique_artists: int
    unique_albums: int
    avg_popularity: float
    max_popularity: int
    min_popularity: int
    top_track: str
    top_artist: str
    top_artists: Dict[str, int] = Field(description="Top 10 artists by track count")
    genre_distribution: Optional[Dict[str, int]] = Field(default=None)
    audio_features_avg: Optional[Dict[str, float]] = Field(default=None)
    data_sources: Dict[str, int] = Field(description="Data source distribution")

class PlaylistInfo(BaseModel):
    """Playlist information"""
    id: str
    name: str
    description: Optional[str] = None
    owner: str
    total_tracks: int
    followers: Optional[int] = None
    image_url: Optional[HttpUrl] = None
    spotify_url: HttpUrl

class MusicTrendConfig(BaseModel):
    """Configuration for music trend analysis"""
    num_playlists: int = Field(default=5, ge=1, le=20, description="Number of playlists to analyze")
    tracks_per_playlist: int = Field(default=25, ge=1, le=100, description="Tracks per playlist")
    include_audio_features: bool = Field(default=True, description="Whether to fetch audio features")
    country: str = Field(default="US", description="Country for regional trends")
    time_range: Optional[str] = Field(default=None, description="Time range for analysis")
    min_popularity: int = Field(default=0, ge=0, le=100, description="Minimum popularity threshold")

class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str
    message: str
    timestamp: datetime
    request_id: Optional[str] = None

# Utility functions for model validation
def validate_spotify_id(spotify_id: str) -> bool:
    """Validate Spotify ID format"""
    return len(spotify_id) == 22 and spotify_id.isalnum()

def validate_popularity_score(score: int) -> bool:
    """Validate popularity score range"""
    return 0 <= score <= 100

def create_track_from_spotify_data(spotify_track: Dict[str, Any], 
                                 source: DataSource = DataSource.SPOTIFY_FEATURED,
                                 playlist_name: str = None) -> Track:
    """Create Track model from Spotify API response"""
    try:
        return Track(
            track_id=spotify_track['id'],
            track_name=spotify_track['name'],
            artist=spotify_track['artists'][0]['name'] if spotify_track['artists'] else 'Unknown',
            artist_id=spotify_track['artists'][0]['id'] if spotify_track['artists'] else None,
            album=spotify_track['album']['name'],
            album_id=spotify_track['album']['id'],
            popularity=spotify_track['popularity'],
            duration_ms=spotify_track['duration_ms'],
            explicit=spotify_track['explicit'],
            preview_url=spotify_track['preview_url'],
            spotify_url=spotify_track['external_urls']['spotify'],
            image_url=spotify_track['album']['images'][0]['url'] if spotify_track['album']['images'] else None,
            release_date=spotify_track['album']['release_date'],
            data_source=source,
            playlist_name=playlist_name,
            fetched_at=datetime.now()
        )
    except KeyError as e:
        raise ValueError(f"Missing required field in Spotify data: {e}")
