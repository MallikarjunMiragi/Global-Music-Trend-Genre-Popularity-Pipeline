"""
Spotify Music Data Connector - FINAL WORKING VERSION
Global Music Trend & Genre Popularity Pipeline
Author: Mallikarjun Miragi
"""

import os
import pandas as pd
from datetime import datetime
import json
import logging
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SpotifyMusicConnector:
    def __init__(self):
        """Initialize Spotify API connection with environment credentials"""
        
        self.client_id = os.getenv('SPOTIFY_CLIENT_ID')
        self.client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        
        if not self.client_id or not self.client_secret:
            logger.error("❌ Spotify credentials missing in environment variables!")
            self.sp = None
            return
        
        logger.info(f"✅ Using Spotify Client ID: {self.client_id[:6]}...")
        logger.info(f"✅ Using Spotify Client Secret: {self.client_secret[:6]}...")
        
        try:
            client_credentials_manager = SpotifyClientCredentials(
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            logger.info("✅ Spotify API connected successfully!")
            self.test_connection()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Spotify: {e}")
            self.sp = None
    
    def test_connection(self):
        """Test API connection using search endpoint"""
        try:
            result = self.sp.search(q='pop', type='track', limit=1)
            if result and result.get('tracks', {}).get('items'):
                logger.info("✅ API connection test: SUCCESS")
                return True
            else:
                logger.error("❌ API test failed: No results from search")
                return False
        except Exception as e:
            logger.error(f"❌ API test failed: {e}")
            return False
    
    def get_trending_music_via_search(self, limit=40):
        """Get trending music using search API - COMPLETELY FIXED VERSION"""
        try:
            trending_tracks = []
            
            # Use simple search terms that work reliably
            search_terms = ['2024', '2025', 'popular', 'top hits', 'trending', 'viral']
            tracks_per_term = max(1, limit // len(search_terms))
            
            for term in search_terms:
                try:
                    results = self.sp.search(
                        q=term,
                        type='track',
                        limit=tracks_per_term,
                        market='US'
                    )
                    
                    # FIXED: Safe access to nested data
                    tracks_items = results.get('tracks', {}).get('items', [])
                    
                    for track in tracks_items:
                        if not track or not track.get('id'):
                            continue
                            
                        try:
                            # Safe extraction with proper defaults
                            artists = track.get('artists', [])
                            artist_name = artists[0].get('name', 'Unknown') if artists else 'Unknown'
                            
                            album = track.get('album', {})
                            album_name = album.get('name', 'Unknown')
                            
                            images = album.get('images', [])
                            image_url = images[0].get('url') if images else None
                            
                            external_urls = track.get('external_urls', {})
                            spotify_url = external_urls.get('spotify', '')
                            
                            track_data = {
                                'track_id': track.get('id', ''),
                                'track_name': track.get('name', 'Unknown'),
                                'artist': artist_name,
                                'album': album_name,
                                'popularity': track.get('popularity', 0),
                                'duration_ms': track.get('duration_ms', 0),
                                'explicit': track.get('explicit', False),
                                'preview_url': track.get('preview_url'),
                                'spotify_url': spotify_url,
                                'image_url': image_url,
                                'release_date': album.get('release_date', ''),
                                'fetched_at': datetime.now().isoformat(),
                                'genre': 'trending',
                                'data_source': 'search_api'
                            }
                            trending_tracks.append(track_data)
                            
                        except Exception as track_error:
                            logger.warning(f"⚠️ Skipping problematic track: {track_error}")
                            continue
                            
                except Exception as search_error:
                    logger.warning(f"⚠️ Search term '{term}' failed: {search_error}")
                    continue
            
            if trending_tracks:
                df = pd.DataFrame(trending_tracks)
                df = df.drop_duplicates(subset=['track_id']).reset_index(drop=True)
                df = df.sort_values('popularity', ascending=False)
                logger.info(f"✅ Retrieved {len(df)} trending tracks")
                return df
            else:
                logger.warning("⚠️ No tracks found")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Critical error in get_trending_music_via_search: {e}")
            return pd.DataFrame()
    
    def get_featured_playlists(self, limit=10):
        """Get playlists using categories"""
        try:
            categories = self.sp.categories(limit=5, country='US')
            playlists = []
            
            if categories.get('categories', {}).get('items'):
                for category in categories['categories']['items']:
                    try:
                        cat_playlists = self.sp.category_playlists(
                            category['id'], 
                            limit=min(5, limit), 
                            country='US'
                        )
                        if cat_playlists.get('playlists', {}).get('items'):
                            playlists.extend(cat_playlists['playlists']['items'])
                    except Exception as e:
                        logger.warning(f"⚠️ Category {category['id']} failed: {e}")
                        continue
            
            if playlists:
                logger.info(f"✅ Found {len(playlists)} playlists via categories")
                return playlists[:limit]
            
            # Fallback to search
            try:
                results = self.sp.search(q='playlist', type='playlist', limit=limit, market='US')
                playlists = results.get('playlists', {}).get('items', [])
                logger.info(f"✅ Found {len(playlists)} playlists via search fallback")
                return playlists
            except Exception as fallback_error:
                logger.error(f"❌ Fallback search also failed: {fallback_error}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error fetching playlists: {e}")
            return []
    
    def get_playlist_tracks(self, playlist_id, limit=25):
        """Get tracks from a playlist"""
        try:
            results = self.sp.playlist_tracks(playlist_id, limit=limit)
            tracks_data = []
            
            if results.get('items'):
                for item in results['items']:
                    if item and item.get('track') and item['track'].get('id'):
                        track = item['track']
                        try:
                            artists = track.get('artists', [])
                            artist_name = artists[0].get('name', 'Unknown') if artists else 'Unknown'
                            
                            album = track.get('album', {})
                            images = album.get('images', [])
                            image_url = images[0].get('url') if images else None
                            
                            track_data = {
                                'track_id': track.get('id', ''),
                                'track_name': track.get('name', 'Unknown'),
                                'artist': artist_name,
                                'album': album.get('name', 'Unknown'),
                                'popularity': track.get('popularity', 0),
                                'duration_ms': track.get('duration_ms', 0),
                                'explicit': track.get('explicit', False),
                                'preview_url': track.get('preview_url'),
                                'spotify_url': track.get('external_urls', {}).get('spotify', ''),
                                'image_url': image_url,
                                'release_date': album.get('release_date', ''),
                                'fetched_at': datetime.now().isoformat()
                            }
                            tracks_data.append(track_data)
                        except Exception as track_error:
                            logger.warning(f"⚠️ Error processing playlist track: {track_error}")
                            continue
            
            return pd.DataFrame(tracks_data)
        except Exception as e:
            logger.error(f"❌ Error fetching tracks: {e}")
            return pd.DataFrame()
    
    def get_comprehensive_music_data(self, num_playlists=5, tracks_per_playlist=20):
        """Main method called by API"""
        logger.info("🎵 Fetching Live Music Data...")
        
        if not self.sp:
            logger.error("❌ Spotify client not initialized")
            return pd.DataFrame()
        
        # Use the working search method as primary
        return self.get_trending_music_via_search(limit=50)
    
    def analyze_music_trends(self):
        """Complete music trend analysis"""
        music_data = self.get_comprehensive_music_data()
        
        if not music_data.empty:
            self.print_analytics(music_data)
            self.save_data(music_data)
        else:
            logger.warning("⚠️ No music data retrieved for analysis")
        
        return music_data
    
    def print_analytics(self, df):
        """Print music analytics"""
        if df.empty:
            logger.warning("⚠️ No data to analyze")
            return
            
        logger.info(f"\n📊 LIVE MUSIC TREND ANALYSIS")
        logger.info(f"=" * 50)
        logger.info(f"Total tracks: {len(df)}")
        logger.info(f"Average popularity: {df['popularity'].mean():.1f}")
        logger.info(f"Top track: {df.loc[df['popularity'].idxmax(), 'track_name']}")
        logger.info(f"Top artist: {df['artist'].value_counts().index[0]}")
    
    def save_data(self, df):
        """Save analysis data"""
        try:
            os.makedirs('../../data/raw', exist_ok=True)
            os.makedirs('../../data/processed', exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            raw_file = f'../../data/raw/spotify_trends_{timestamp}.csv'
            df.to_csv(raw_file, index=False)
            
            if not df.empty:
                summary = {
                    'timestamp': datetime.now().isoformat(),
                    'total_tracks': len(df),
                    'avg_popularity': float(df['popularity'].mean()),
                    'top_track': df.loc[df['popularity'].idxmax(), 'track_name'],
                    'top_artist': df['artist'].value_counts().index[0],
                    'data_source': df['data_source'].iloc[0] if 'data_source' in df.columns else 'unknown'
                }
                
                summary_file = f'../../data/processed/trend_summary_{timestamp}.json'
                with open(summary_file, 'w') as f:
                    json.dump(summary, f, indent=2)
                
                logger.info(f"💾 Data saved: {raw_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving: {e}")

def main():
    connector = SpotifyMusicConnector()
    if connector.sp:
        results = connector.analyze_music_trends()
        if not results.empty:
            print(f"✅ Successfully retrieved {len(results)} tracks")
            print(f"Sample: {results.iloc[0]['track_name']} by {results.iloc[0]['artist']}")
        else:
            print("❌ No data retrieved")
        logger.info("🎉 Analysis complete!")
    else:
        logger.error("❌ Could not initialize Spotify connector. Check your credentials.")

if __name__ == "__main__":
    main()
