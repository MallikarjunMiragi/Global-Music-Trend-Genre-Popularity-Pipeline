"""
Utility Functions for Global Music Trend Pipeline
Author: Mallikarjun Miragi
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
import logging
import hashlib
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_track_data(track_data: Dict[str, Any]) -> bool:
    """Validate that track data contains required fields"""
    required_fields = ['track_id', 'track_name', 'artist', 'album', 'popularity']
    return all(field in track_data and track_data[field] is not None for field in required_fields)

def clean_track_name(track_name: str) -> str:
    """Clean and normalize track names"""
    if not track_name:
        return "Unknown Track"
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', track_name.strip())
    
    # Remove common suffixes that might cause duplicates
    suffixes_to_remove = [
        r'\s*\(feat\..*?\)',
        r'\s*\(ft\..*?\)',
        r'\s*\(featuring.*?\)',
        r'\s*\- Remaster.*',
        r'\s*\- \d{4} Remaster.*'
    ]
    
    for suffix in suffixes_to_remove:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)
    
    return cleaned.strip()

def clean_artist_name(artist_name: str) -> str:
    """Clean and normalize artist names"""
    if not artist_name:
        return "Unknown Artist"
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', artist_name.strip())
    return cleaned

def format_duration(duration_ms: int) -> str:
    """Convert duration from milliseconds to MM:SS format"""
    if duration_ms <= 0:
        return "0:00"
    
    total_seconds = duration_ms // 1000
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"

def calculate_popularity_tier(popularity: int) -> str:
    """Categorize tracks by popularity tier"""
    if popularity >= 80:
        return "Hot"
    elif popularity >= 60:
        return "Trending"
    elif popularity >= 40:
        return "Rising"
    else:
        return "Emerging"

def extract_release_year(release_date: str) -> Optional[int]:
    """Extract year from various release date formats"""
    if not release_date:
        return None
    
    try:
        # Handle different date formats
        if len(release_date) == 4:  # Just year
            return int(release_date)
        elif len(release_date) == 7:  # YYYY-MM
            return int(release_date[:4])
        elif len(release_date) == 10:  # YYYY-MM-DD
            return int(release_date[:4])
        else:
            # Try to extract 4-digit year from anywhere in string
            year_match = re.search(r'\b(19|20)\d{2}\b', release_date)
            if year_match:
                return int(year_match.group())
    except (ValueError, AttributeError):
        pass
    
    return None

def is_recent_release(release_date: str, days_threshold: int = 30) -> bool:
    """Check if track is a recent release"""
    try:
        if len(release_date) >= 10:  # Full date format
            release_dt = datetime.strptime(release_date[:10], '%Y-%m-%d')
            return (datetime.now() - release_dt).days <= days_threshold
        elif len(release_date) == 7:  # YYYY-MM format
            release_dt = datetime.strptime(release_date + '-01', '%Y-%m-%d')
            return (datetime.now() - release_dt).days <= days_threshold
    except ValueError:
        pass
    
    return False

def deduplicate_tracks(tracks_df: pd.DataFrame, 
                      priority_column: str = 'popularity') -> pd.DataFrame:
    """Remove duplicate tracks, keeping the one with highest priority"""
    if tracks_df.empty:
        return tracks_df
    
    # Clean track names for better duplicate detection
    tracks_df['clean_track_name'] = tracks_df['track_name'].apply(clean_track_name)
    tracks_df['clean_artist_name'] = tracks_df['artist'].apply(clean_artist_name)
    
    # Sort by priority (highest first) then remove duplicates
    sorted_df = tracks_df.sort_values(priority_column, ascending=False)
    deduplicated = sorted_df.drop_duplicates(
        subset=['clean_track_name', 'clean_artist_name'], 
        keep='first'
    )
    
    # Drop the temporary cleaning columns
    deduplicated = deduplicated.drop(['clean_track_name', 'clean_artist_name'], axis=1)
    
    logger.info(f"Deduplicated {len(tracks_df)} tracks to {len(deduplicated)} unique tracks")
    return deduplicated.reset_index(drop=True)

def calculate_trend_score(track_data: Dict[str, Any]) -> float:
    """Calculate a composite trend score for ranking"""
    popularity = track_data.get('popularity', 0)
    release_date = track_data.get('release_date', '')
    
    # Base score from popularity
    score = popularity
    
    # Boost recent releases
    if is_recent_release(release_date, days_threshold=7):
        score *= 1.2  # 20% boost for very recent
    elif is_recent_release(release_date, days_threshold=30):
        score *= 1.1  # 10% boost for recent
    
    # Normalize to 0-100 range
    return min(100, score)

def create_data_hash(data: Union[Dict, List, str]) -> str:
    """Create a hash for data integrity checking"""
    if isinstance(data, (dict, list)):
        data_str = json.dumps(data, sort_keys=True)
    else:
        data_str = str(data)
    
    return hashlib.md5(data_str.encode()).hexdigest()

def save_trending_data(data: pd.DataFrame, 
                      filename_prefix: str = "trending_music",
                      include_metadata: bool = True) -> Dict[str, str]:
    """Save trending data with metadata"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Ensure directories exist
    os.makedirs('../../data/raw', exist_ok=True)
    os.makedirs('../../data/processed', exist_ok=True)
    
    files_created = {}
    
    try:
        # Save raw data
        raw_filename = f"../../data/raw/{filename_prefix}_{timestamp}.csv"
        data.to_csv(raw_filename, index=False)
        files_created['raw_data'] = raw_filename
        
        # Save processed summary
        if not data.empty:
            summary = create_data_summary(data)
            summary_filename = f"../../data/processed/{filename_prefix}_summary_{timestamp}.json"
            with open(summary_filename, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            files_created['summary'] = summary_filename
        
        # Save metadata if requested
        if include_metadata:
            metadata = {
                'created_at': datetime.now().isoformat(),
                'record_count': len(data),
                'columns': list(data.columns),
                'data_hash': create_data_hash(data.to_dict('records')),
                'file_size_bytes': os.path.getsize(raw_filename),
                'data_types': data.dtypes.astype(str).to_dict()
            }
            
            metadata_filename = f"../../data/processed/{filename_prefix}_metadata_{timestamp}.json"
            with open(metadata_filename, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            files_created['metadata'] = metadata_filename
        
        logger.info(f"Saved trending data to {len(files_created)} files")
        
    except Exception as e:
        logger.error(f"Error saving trending data: {e}")
        raise
    
    return files_created

def create_data_summary(data: pd.DataFrame) -> Dict[str, Any]:
    """Create a comprehensive summary of the music data"""
    if data.empty:
        return {"message": "No data available"}
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_tracks': len(data),
        'data_quality': {
            'missing_values': data.isnull().sum().to_dict(),
            'duplicate_count': data.duplicated().sum()
        }
    }
    
    # Add music-specific analytics
    if 'popularity' in data.columns:
        summary.update({
            'popularity_stats': {
                'average': float(data['popularity'].mean()),
                'median': float(data['popularity'].median()),
                'max': int(data['popularity'].max()),
                'min': int(data['popularity'].min())
            },
            'top_track': data.loc[data['popularity'].idxmax(), 'track_name'] if len(data) > 0 else None
        })
    
    if 'artist' in data.columns:
        artist_counts = data['artist'].value_counts()
        summary.update({
            'unique_artists': len(artist_counts),
            'top_artists': artist_counts.head(10).to_dict(),
            'most_prolific_artist': artist_counts.index[0] if len(artist_counts) > 0 else None
        })
    
    if 'release_date' in data.columns:
        data['release_year'] = data['release_date'].apply(extract_release_year)
        year_counts = data['release_year'].value_counts().sort_index()
        summary['release_year_distribution'] = year_counts.to_dict()
    
    # Add trend analysis
    if all(col in data.columns for col in ['track_name', 'artist', 'popularity']):
        data['trend_score'] = data.apply(calculate_trend_score, axis=1)
        data['popularity_tier'] = data['popularity'].apply(calculate_popularity_tier)
        
        summary.update({
            'trend_analysis': {
                'avg_trend_score': float(data['trend_score'].mean()),
                'popularity_tiers': data['popularity_tier'].value_counts().to_dict()
            }
        })
    
    return summary

def load_cached_data(cache_file: str, max_age_minutes: int = 5) -> Optional[pd.DataFrame]:
    """Load cached data if it exists and is fresh enough"""
    if not os.path.exists(cache_file):
        return None
    
    # Check file age
    file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
    if file_age > timedelta(minutes=max_age_minutes):
        logger.info(f"Cache file {cache_file} is too old, ignoring")
        return None
    
    try:
        data = pd.read_csv(cache_file)
        logger.info(f"Loaded {len(data)} records from cache")
        return data
    except Exception as e:
        logger.error(f"Error loading cache file {cache_file}: {e}")
        return None

def filter_tracks_by_criteria(data: pd.DataFrame, 
                            min_popularity: int = 0,
                            max_age_days: Optional[int] = None,
                            exclude_explicit: bool = False) -> pd.DataFrame:
    """Filter tracks based on various criteria"""
    if data.empty:
        return data
    
    filtered = data.copy()
    original_count = len(filtered)
    
    # Filter by popularity
    if min_popularity > 0:
        filtered = filtered[filtered['popularity'] >= min_popularity]
    
    # Filter by age
    if max_age_days:
        if 'release_date' in filtered.columns:
            filtered = filtered[filtered['release_date'].apply(
                lambda x: is_recent_release(x, max_age_days)
            )]
    
    # Filter explicit content
    if exclude_explicit and 'explicit' in filtered.columns:
        filtered = filtered[~filtered['explicit']]
    
    logger.info(f"Filtered {original_count} tracks to {len(filtered)} tracks")
    return filtered.reset_index(drop=True)

def normalize_genre_names(genres: List[str]) -> List[str]:
    """Normalize genre names for consistency"""
    if not genres:
        return []
    
    normalized = []
    for genre in genres:
        if isinstance(genre, str):
            # Convert to title case and remove extra spaces
            clean_genre = re.sub(r'\s+', ' ', genre.strip().title())
            normalized.append(clean_genre)
    
    return list(set(normalized))  # Remove duplicates

def calculate_audio_feature_percentiles(data: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """Calculate percentiles for audio features"""
    audio_features = ['danceability', 'energy', 'valence', 'acousticness', 
                     'instrumentalness', 'liveness', 'speechiness']
    
    percentiles = {}
    for feature in audio_features:
        if feature in data.columns:
            percentiles[feature] = {
                '25th': float(data[feature].quantile(0.25)),
                '50th': float(data[feature].quantile(0.50)),
                '75th': float(data[feature].quantile(0.75)),
                '90th': float(data[feature].quantile(0.90))
            }
    
    return percentiles

# Export utility functions for easy importing
__all__ = [
    'validate_track_data',
    'clean_track_name', 
    'clean_artist_name',
    'format_duration',
    'calculate_popularity_tier',
    'extract_release_year',
    'is_recent_release',
    'deduplicate_tracks',
    'calculate_trend_score',
    'create_data_hash',
    'save_trending_data',
    'create_data_summary',
    'load_cached_data',
    'filter_tracks_by_criteria',
    'normalize_genre_names',
    'calculate_audio_feature_percentiles'
]
