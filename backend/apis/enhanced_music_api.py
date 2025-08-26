"""
Enhanced FastAPI with Caching and Real-time Features
Global Music Trend & Genre Popularity Pipeline
Author: Mallikarjun Miragi
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Optional
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import threading
import time
import sys
sys.path.append('../data-ingestion')
from spotify_connector import SpotifyMusicConnector

app = FastAPI(
    title="Enhanced Music Trend API",
    description="Advanced music analytics with caching and real-time updates",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global cache with thread safety
CACHE = {
    "music_data": None,
    "analytics": None,
    "last_updated": None,
    "is_updating": False
}
CACHE_LOCK = threading.Lock()
CACHE_TIMEOUT = 300  # 5 minutes

def background_data_refresh():
    """Background task to refresh music data"""
    with CACHE_LOCK:
        if CACHE["is_updating"]:
            return
        CACHE["is_updating"] = True
    
    try:
        connector = SpotifyMusicConnector()
        if connector.sp:
            music_data = connector.get_comprehensive_music_data(
                num_playlists=5, 
                tracks_per_playlist=25
            )
            
            if not music_data.empty:
                # Calculate analytics
                analytics = {
                    "total_tracks": len(music_data),
                    "unique_artists": int(music_data['artist'].nunique()),
                    "avg_popularity": float(music_data['popularity'].mean()),
                    "top_track": music_data.loc[music_data['popularity'].idxmax(), 'track_name'],
                    "top_artist": music_data['artist'].value_counts().index[0],
                    "timestamp": datetime.now().isoformat()
                }
                
                with CACHE_LOCK:
                    CACHE["music_data"] = music_data
                    CACHE["analytics"] = analytics
                    CACHE["last_updated"] = datetime.now()
                    print("🔄 Cache updated successfully")
    
    except Exception as e:
        print(f"❌ Background refresh failed: {e}")
    
    finally:
        with CACHE_LOCK:
            CACHE["is_updating"] = False

def is_cache_valid():
    """Check if cache is still valid"""
    if CACHE["last_updated"] is None:
        return False
    return datetime.now() - CACHE["last_updated"] < timedelta(seconds=CACHE_TIMEOUT)

@app.on_event("startup")
async def startup_event():
    """Initialize cache on startup"""
    print("🚀 Initializing Enhanced Music Trend API...")
    background_data_refresh()

@app.get("/")
async def root():
    return {
        "message": "🎵 Enhanced Global Music Trend API",
        "version": "2.0.0",
        "features": ["caching", "real-time-updates", "background-refresh"],
        "cache_status": "active" if is_cache_valid() else "expired",
        "last_updated": CACHE["last_updated"].isoformat() if CACHE["last_updated"] else None
    }

@app.get("/trending")
async def get_trending_tracks(limit: int = 20, force_refresh: bool = False):
    """Get trending tracks with intelligent caching"""
    
    # Check cache validity
    if not is_cache_valid() or force_refresh:
        if not CACHE["is_updating"]:
            # Trigger background refresh
            threading.Thread(target=background_data_refresh).start()
        
        # If no cached data, wait for fresh data
        if CACHE["music_data"] is None:
            # Wait up to 10 seconds for data
            for _ in range(50):
                if CACHE["music_data"] is not None:
                    break
                await asyncio.sleep(0.2)
    
    if CACHE["music_data"] is None:
        raise HTTPException(status_code=503, detail="Music data temporarily unavailable")
    
    # Format response
    tracks = []
    music_data = CACHE["music_data"]
    
    for _, row in music_data.head(limit).iterrows():
        track = {
            "track_id": row['track_id'],
            "track_name": row['track_name'],
            "artist": row['artist'],
            "album": row['album'],
            "popularity": int(row['popularity']),
            "duration_ms": int(row['duration_ms']),
            "explicit": bool(row['explicit']),
            "spotify_url": row['spotify_url'],
            "image_url": row['image_url'],
            "release_date": row['release_date'],
            "playlist_source": row['playlist_name']
        }
        tracks.append(track)
    
    return {
        "tracks": tracks,
        "total_available": len(music_data),
        "cache_status": "fresh" if is_cache_valid() else "stale",
        "last_updated": CACHE["last_updated"].isoformat() if CACHE["last_updated"] else None
    }

@app.get("/analytics")
async def get_enhanced_analytics():
    """Get comprehensive analytics with caching"""
    
    if not is_cache_valid():
        if not CACHE["is_updating"]:
            threading.Thread(target=background_data_refresh).start()
    
    if CACHE["analytics"] is None:
        raise HTTPException(status_code=503, detail="Analytics temporarily unavailable")
    
    return CACHE["analytics"]

@app.post("/refresh")
async def force_refresh(background_tasks: BackgroundTasks):
    """Force refresh music data"""
    background_tasks.add_task(background_data_refresh)
    return {
        "message": "Data refresh initiated",
        "status": "processing"
    }

@app.get("/cache-status")
async def get_cache_status():
    """Get detailed cache status"""
    return {
        "cache_valid": is_cache_valid(),
        "last_updated": CACHE["last_updated"].isoformat() if CACHE["last_updated"] else None,
        "is_updating": CACHE["is_updating"],
        "has_data": CACHE["music_data"] is not None,
        "data_count": len(CACHE["music_data"]) if CACHE["music_data"] is not None else 0,
        "cache_timeout_seconds": CACHE_TIMEOUT
    }

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Enhanced Music Trend API with caching...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
