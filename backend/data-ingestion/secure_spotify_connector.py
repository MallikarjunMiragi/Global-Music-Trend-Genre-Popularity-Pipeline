"""
Secure Spotify Music Data Connector - Production Ready
Global Music Trend & Genre Popularity Pipeline
Author: Mallikarjun Miragi
Date: August 26, 2025

Production features:
- OAuth2 authentication flow
- Rate limiting compliance
- Error handling & retry logic
- Data caching mechanisms
- Comprehensive logging
- Security best practices
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Load environment variables
load_dotenv('../.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../../logs/spotify_connector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class SpotifyConfig:
    """Configuration class for Spotify API credentials"""
    client_id: str
    client_secret: str
    redirect_uri: str = "http://127.0.0.1:3000/callback"
    scopes: List[str] = None
    
    def __post_init__(self):
        if self.scopes is None:
            self.scopes = [
                'user-read-recently-played',
                'user-top-read',
                'playlist-read-private',
                'playlist-read-collaborative'
            ]

class SecureSpotifyConnector:
    """
    Production-ready Spotify API connector with enhanced security,
    error handling, rate limiting, and data validation.
    """
    
    def __init__(self, use_oauth: bool = False):
        """
        Initialize the secure Spotify connector
        
        Args:
            use_oauth: Whether to use OAuth flow (for user data) or Client Credentials (for public data)
        """
        self.config = self._load_config()
        self.use_oauth = use_oauth
        self.sp = None
        self.authenticated = False
        self.rate_limit_buffer = 0.1  # Buffer time between requests
        self.max_retries = 3
        self.cache = {}
        self.cache_duration = 300  # 5 minutes cache
        
        # Ensure logs directory exists
        os.makedirs('../../logs', exist_ok=True)
        
        if self.config:
            self._authenticate()
    
    def _load_config(self) -> Optional[SpotifyConfig]:
        """Load and validate Spotify configuration from environment"""
        try:
            client_id = os.getenv('SPOTIFY_CLIENT_ID')
            client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
            redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI', 'http://127.0.0.1:3000/callback')
            
            if not client_id or not client_secret:
                logger.error("Missing required Spotify credentials in environment")
                return None
                
            logger.info("Spotify configuration loaded successfully")
            return SpotifyConfig(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri
            )
            
        except Exception as e:
            logger.error(f"Failed to load Spotify configuration: {e}")
            return None
    
    def _authenticate(self) -> bool:
        """Authenticate with Spotify API using appropriate flow"""
        try:
            if self.use_oauth:
                # OAuth flow for user data access
                auth_manager = SpotifyOAuth(
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret,
                    redirect_uri=self.config.redirect_uri,
                    scope=' '.join(self.config.scopes),
                    cache_path='.spotify_cache'
                )
            else:
                # Client Credentials flow for public data
                auth_manager = SpotifyClientCredentials(
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret
                )
            
            self.sp = spotipy.Spotify(auth_manager=auth_manager)
            
            # Test authentication with a simple API call
            self._test_connection()
            
            self.authenticated = True
            logger.info(f"Spotify API authenticated successfully ({'OAuth' if self.use_oauth else 'Client Credentials'})")
            return True
            
        except Exception as e:
            logger.error(f"Spotify authentication failed: {e}")
            self.authenticated = False
            return False
    
    def _test_connection(self) -> bool:
        """Test API connection with a lightweight call"""
        try:
            # Try to get featured playlists as a connection test
            result = self.sp.featured_playlists(limit=1, country='US')
            logger.info("API connection test successful")
            return True
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            raise
    
    def _make_request_with_retry(self, func, *args, **kwargs):
        """Make API request with retry logic and rate limiting"""
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.rate_limit_buffer)  # Rate limiting
                result = func(*args, **kwargs)
                return result
                
            except spotipy.SpotifyException as e:
                if e.http_status == 429:  # Rate limited
                    retry_after = int(e.headers.get('Retry-After', 1))
                    logger.warning(f"Rate limited, waiting {retry_after} seconds")
                    time.sleep(retry_after + 1)
                    continue
                elif e.http_status == 401:  # Unauthorized
                    logger.error("Authentication expired, re-authenticating...")
                    if self._authenticate():
                        continue
                    else:
                        raise
                else:
                    logger.error(f"Spotify API error: {e}")
                    if attempt == self.max_retries - 1:
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
        
        raise Exception(f"Failed after {self.max_retries} attempts")
    
    def _check_cache(self, key: str) -> Optional[Any]:
        """Check if data exists in cache and is still valid"""
        if key in self.cache:
            timestamp, data = self.cache[key]
            if datetime.now() - timestamp < timedelta(seconds=self.cache_duration):
                logger.info(f"Using cached data for key: {key}")
                return data
        return None
    
    def _set_cache(self, key: str, data: Any) -> None:
        """Store data in cache with timestamp"""
        self.cache[key] = (datetime.now(), data)
    
    def get_featured_playlists(self, limit: int = 20, country: str = 'US') -> List[Dict]:
        """Get featured playlists with caching and error handling"""
        if not self.authenticated:
            logger.error("Not authenticated with Spotify")
            return []
        
        cache_key = f"featured_playlists_{limit}_{country}"
        cached_result = self._check_cache(cache_key)
        if cached_result:
            return cached_result
        
        try:
            result = self._make_request_with_retry(
                self.sp.featured_playlists,
                limit=limit,
                country=country
            )
            
            playlists = result['playlists']['items']
            self._set_cache(cache_key, playlists)
            logger.info(f"Retrieved {len(playlists)} featured playlists")
            return playlists
            
        except Exception as e:
            logger.error(f"Failed to get featured playlists: {e}")
            return []
    
    def get_playlist_tracks(self, playlist_id: str, limit: int = 50) -> List[Dict]:
        """Get tracks from a specific playlist with validation"""
        if not self.authenticated:
            logger.error("Not authenticated with Spotify")
            return []
        
        try:
            all_tracks = []
            offset = 0
            
            while len(all_tracks) < limit:
                batch_limit = min(50, limit - len(all_tracks))  # Max 50 per request
                
                result = self._make_request_with_retry(
                    self.sp.playlist_tracks,
                    playlist_id,
                    limit=batch_limit,
                    offset=offset
                )
                
                tracks = result['items']
                if not tracks:
                    break
                
                # Validate and clean track data
                for item in tracks:
                    if item['track'] and item['track']['id']:
                        track_data = self._extract_track_data(item['track'])
                        if track_data:
                            all_tracks.append(track_data)
                
                offset += batch_limit
                if len(tracks) < batch_limit:
                    break
            
            logger.info(f"Retrieved {len(all_tracks)} tracks from playlist {playlist_id}")
            return all_tracks[:limit]
            
        except Exception as e:
            logger.error(f"Failed to get playlist tracks for {playlist_id}: {e}")
            return []
    
    def _extract_track_data(self, track: Dict) -> Optional[Dict]:
        """Extract and validate track data from Spotify track object"""
        try:
            return {
                'track_id': track['id'],
                'track_name': track['name'],
                'artist': track['artists'][0]['name'] if track['artists'] else 'Unknown',
                'artist_id': track['artists'][0]['id'] if track['artists'] else None,
                'album': track['album']['name'],
                'album_id': track['album']['id'],
                'popularity': track['popularity'],
                'duration_ms': track['duration_ms'],
                'explicit': track['explicit'],
                'preview_url': track['preview_url'],
                'spotify_url': track['external_urls']['spotify'],
                'image_url': track['album']['images'][0]['url'] if track['album']['images'] else None,
                'release_date': track['album']['release_date'],
                'fetched_at': datetime.now().isoformat()
            }
        except KeyError as e:
            logger.warning(f"Missing required field in track data: {e}")
            return None
    
    def get_audio_features(self, track_ids: List[str]) -> pd.DataFrame:
        """Get audio features for multiple tracks with batch processing"""
        if not self.authenticated:
            logger.error("Not authenticated with Spotify")
            return pd.DataFrame()
        
        if not track_ids:
            return pd.DataFrame()
        
        try:
            all_features = []
            batch_size = 100  # Spotify API limit
            
            for i in range(0, len(track_ids), batch_size):
                batch = track_ids[i:i + batch_size]
                
                features = self._make_request_with_retry(
                    self.sp.audio_features,
                    batch
                )
                
                # Filter out None values
                valid_features = [f for f in features if f is not None]
                all_features.extend(valid_features)
                
                logger.info(f"Retrieved audio features for batch {i//batch_size + 1}")
            
            logger.info(f"Retrieved audio features for {len(all_features)} tracks")
            return pd.DataFrame(all_features)
            
        except Exception as e:
            logger.error(f"Failed to get audio features: {e}")
            return pd.DataFrame()
    
    def get_comprehensive_music_data(self, 
                                   num_playlists: int = 5,
                                   tracks_per_playlist: int = 20,
                                   include_audio_features: bool = True) -> pd.DataFrame:
        """
        Get comprehensive music data from multiple sources with full analysis
        
        Args:
            num_playlists: Number of featured playlists to process
            tracks_per_playlist: Number of tracks per playlist
            include_audio_features: Whether to fetch audio features for ML analysis
        
        Returns:
            DataFrame with comprehensive music data
        """
        if not self.authenticated:
            logger.error("Not authenticated with Spotify")
            return pd.DataFrame()
        
        logger.info("Starting comprehensive music data collection...")
        
        try:
            # Get featured playlists
            playlists = self.get_featured_playlists(limit=num_playlists)
            if not playlists:
                logger.error("No playlists retrieved")
                return pd.DataFrame()
            
            all_tracks = []
            
            # Process each playlist
            for i, playlist in enumerate(playlists):
                logger.info(f"Processing playlist {i+1}/{len(playlists)}: {playlist['name']}")
                
                playlist_tracks = self.get_playlist_tracks(
                    playlist['id'], 
                    limit=tracks_per_playlist
                )
                
                # Add playlist metadata
                for track in playlist_tracks:
                    track['source_playlist'] = playlist['name']
                    track['source_playlist_id'] = playlist['id']
                    track['data_source'] = 'featured_playlists'
                
                all_tracks.extend(playlist_tracks)
            
            if not all_tracks:
                logger.error("No tracks collected from playlists")
                return pd.DataFrame()
            
            # Convert to DataFrame and remove duplicates
            tracks_df = pd.DataFrame(all_tracks)
            tracks_df = tracks_df.drop_duplicates(subset=['track_id']).reset_index(drop=True)
            
            logger.info(f"Collected {len(tracks_df)} unique tracks")
            
            # Add audio features if requested
            if include_audio_features and not tracks_df.empty:
                logger.info("Fetching audio features for ML analysis...")
                track_ids = tracks_df['track_id'].tolist()
                features_df = self.get_audio_features(track_ids)
                
                if not features_df.empty:
                    # Merge with audio features
                    tracks_df = tracks_df.merge(
                        features_df,
                        left_on='track_id',
                        right_on='id',
                        how='left'
                    )
                    logger.info(f"Added audio features to {len(tracks_df)} tracks")
            
            return tracks_df
            
        except Exception as e:
            logger.error(f"Failed to get comprehensive music data: {e}")
            return pd.DataFrame()
    
    def save_data(self, df: pd.DataFrame, prefix: str = "secure_music_data") -> tuple:
        """Save data with comprehensive metadata and validation"""
        if df is None or df.empty:
            logger.warning("No data to save")
            return None, None
        
        try:
            # Ensure data directories exist
            os.makedirs('../../data/raw', exist_ok=True)
            os.makedirs('../../data/processed', exist_ok=True)
            os.makedirs('../../data/metadata', exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save raw data
            raw_file = f'../../data/raw/{prefix}_{timestamp}.csv'
            df.to_csv(raw_file, index=False)
            
            # Create comprehensive metadata
            metadata = {
                'collection_timestamp': datetime.now().isoformat(),
                'data_source': 'spotify_api',
                'connector_version': '2.0_secure',
                'total_records': len(df),
                'unique_tracks': int(df['track_id'].nunique()) if 'track_id' in df.columns else None,
                'unique_artists': int(df['artist'].nunique()) if 'artist' in df.columns else None,
                'data_sources': df['data_source'].value_counts().to_dict() if 'data_source' in df.columns else {},
                'has_audio_features': 'danceability' in df.columns,
                'columns': list(df.columns),
                'file_size_mb': round(os.path.getsize(raw_file) / (1024*1024), 2),
                'data_quality': {
                    'missing_values': df.isnull().sum().to_dict(),
                    'duplicate_tracks': int(df.duplicated(subset=['track_id']).sum()) if 'track_id' in df.columns else 0
                }
            }
            
            # Save metadata
            metadata_file = f'../../data/metadata/{prefix}_metadata_{timestamp}.json'
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            # Create processed summary
            if 'popularity' in df.columns:
                summary = {
                    'timestamp': datetime.now().isoformat(),
                    'total_tracks': len(df),
                    'avg_popularity': float(df['popularity'].mean()),
                    'top_track': df.loc[df['popularity'].idxmax(), 'track_name'] if len(df) > 0 else None,
                    'top_artist': df['artist'].value_counts().index[0] if len(df) > 0 else None,
                }
                
                summary_file = f'../../data/processed/{prefix}_summary_{timestamp}.json'
                with open(summary_file, 'w') as f:
                    json.dump(summary, f, indent=2)
            else:
                summary_file = None
            
            logger.info(f"Data saved successfully:")
            logger.info(f"  Raw data: {raw_file}")
            logger.info(f"  Metadata: {metadata_file}")
            if summary_file:
                logger.info(f"  Summary: {summary_file}")
            
            return raw_file, metadata_file
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            return None, None
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get connector health status and diagnostics"""
        return {
            'authenticated': self.authenticated,
            'config_loaded': self.config is not None,
            'cache_entries': len(self.cache),
            'authentication_method': 'OAuth' if self.use_oauth else 'Client Credentials',
            'last_request_time': datetime.now().isoformat(),
            'rate_limit_buffer': self.rate_limit_buffer,
            'max_retries': self.max_retries
        }

def main():
    """Main execution function for testing"""
    print("🔒 Secure Spotify Music Data Connector")
    print("🎵 Global Music Trend & Genre Popularity Pipeline")
    print("👨‍💻 by Mallikarjun Miragi")
    print("=" * 60)
    
    # Initialize secure connector
    connector = SecureSpotifyConnector(use_oauth=False)  # Use Client Credentials for public data
    
    if not connector.authenticated:
        logger.error("Failed to authenticate with Spotify API")
        return
    
    # Get comprehensive music data
    logger.info("Fetching comprehensive music data...")
    music_data = connector.get_comprehensive_music_data(
        num_playlists=3,
        tracks_per_playlist=25,
        include_audio_features=True
    )
    
    if not music_data.empty:
        # Display basic analytics
        print(f"\n📊 Data Collection Summary:")
        print(f"Total tracks: {len(music_data)}")
        print(f"Unique artists: {music_data['artist'].nunique()}")
        print(f"Data sources: {music_data['data_source'].value_counts().to_dict()}")
        
        if 'danceability' in music_data.columns:
            print(f"Audio features available: Yes")
            print(f"Average danceability: {music_data['danceability'].mean():.2f}")
        
        # Save data
        raw_file, metadata_file = connector.save_data(music_data, "secure_comprehensive")
        
        print(f"\n🎉 Data collection completed successfully!")
        print(f"📁 Files saved: {raw_file}")
        
        # Show health status
        health = connector.get_health_status()
        print(f"\n🔍 System Health: {'✅ Healthy' if health['authenticated'] else '❌ Issues detected'}")
        
    else:
        logger.error("No data collected")

if __name__ == "__main__":
    main()
