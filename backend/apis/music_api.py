"""
FastAPI Backend for Global Music Trend Pipeline
Author: Mallikarjun Miragi
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import sys
sys.path.append('../data-ingestion')
from spotify_connector import SpotifyMusicConnector

app = FastAPI(
    title="Global Music Trend API",
    description="REST API for music trend analytics",
    version="1.0.0"
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "message": "🎵 Global Music Trend API",
        "status": "operational",
        "endpoints": {
            "/trending": "Get current trending tracks",
            "/analytics": "Get music trend analytics",
            "/health": "API health check"
        }
    }

@app.get("/trending")
async def get_trending_tracks(limit: int = 20):
    """Get current trending tracks from Spotify"""
    try:
        connector = SpotifyMusicConnector()
        if not connector.sp:
            raise HTTPException(status_code=500, detail="Spotify API connection failed")
        
        # Get fresh music data
        music_data = connector.get_comprehensive_music_data(
            num_playlists=3, 
            tracks_per_playlist=limit//3
        )
        
        if music_data.empty:
            raise HTTPException(status_code=404, detail="No trending data found")
        
        # Convert to JSON format
        tracks = []
        for _, row in music_data.head(limit).iterrows():
            track = {
                "track_id": row['track_id'],
                "track_name": row['track_name'],
                "artist": row['artist'],
                "album": row['album'],
                "popularity": int(row['popularity']),
                "duration_ms": int(row['duration_ms']),
                "spotify_url": row['spotify_url'],
                "image_url": row['image_url'],
                "release_date": row['release_date'],
                "playlist_source": row['playlist_name']
            }
            tracks.append(track)
        
        return {
            "tracks": tracks,
            "total": len(tracks),
            "timestamp": music_data.iloc[0]['fetched_at']
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics")
async def get_analytics():
    """Get music trend analytics summary"""
    try:
        connector = SpotifyMusicConnector()
        if not connector.sp:
            raise HTTPException(status_code=500, detail="Spotify API connection failed")
        
        music_data = connector.get_comprehensive_music_data(num_playlists=3, tracks_per_playlist=30)
        
        if music_data.empty:
            raise HTTPException(status_code=404, detail="No data available")
        
        analytics = {
            "total_tracks": len(music_data),
            "unique_artists": int(music_data['artist'].nunique()),
            "avg_popularity": float(music_data['popularity'].mean()),
            "top_track": music_data.loc[music_data['popularity'].idxmax(), 'track_name'],
            "top_artist": music_data['artist'].value_counts().index[0],
            "top_artists": music_data['artist'].value_counts().head(10).to_dict(),
            "data_sources": music_data['playlist_name'].value_counts().to_dict()
        }
        
        return analytics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """API health check"""
    try:
        connector = SpotifyMusicConnector()
        spotify_status = "connected" if connector.sp else "disconnected"
        
        return {
            "status": "healthy",
            "spotify_api": spotify_status,
            "timestamp": connector.sp.featured_playlists(limit=1) if connector.sp else None
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Global Music Trend API...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
