"""
FastAPI Backend for Global Music Trend Pipeline
Author: Mallikarjun Miragi
Updated to work with latest Spotify API changes
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
from datetime import datetime
import logging
import sys

sys.path.append('../data-ingestion')
from spotify_connector import SpotifyMusicConnector

app = FastAPI(
    title="Global Music Trend API",
    description="REST API for music trend analytics powered by Spotify",
    version="1.0.0"
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("music_api")

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
        "version": "1.0.0",
        "endpoints": {
            "/trending": "Get current trending tracks from Spotify",
            "/analytics": "Get comprehensive music trend analytics",
            "/health": "API health check and Spotify connection status",
            "/docs": "Interactive API documentation"
        },
        "author": "Mallikarjun Miragi - CS3238 Project"
    }

@app.get("/trending")
async def get_trending_tracks(limit: int = 20):
    """Get current trending tracks from Spotify using search API"""
    try:
        logger.info(f"Fetching {limit} trending tracks...")
        
        connector = SpotifyMusicConnector()
        if not connector.sp:
            raise HTTPException(status_code=500, detail="Spotify API connection failed")
        
        # Use the new working method that bypasses deprecated endpoints
        music_data = connector.get_trending_music_via_search(limit=limit)
        
        if music_data.empty:
            raise HTTPException(status_code=404, detail="No trending data found")
        
        # Convert DataFrame to JSON format
        tracks = []
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
                "genre": row.get('genre', 'unknown'),
                "data_source": row.get('data_source', 'search_api')
            }
            tracks.append(track)
        
        logger.info(f"Successfully retrieved {len(tracks)} tracks")
        
        return {
            "tracks": tracks,
            "total": len(tracks),
            "timestamp": datetime.now().isoformat(),
            "api_version": "1.0.0",
            "data_source": "spotify_search_api"
        }
        
    except Exception as e:
        logger.error(f"Error fetching trending tracks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics")
async def get_analytics():
    """Get comprehensive music trend analytics"""
    try:
        logger.info("Generating music analytics...")
        
        connector = SpotifyMusicConnector()
        if not connector.sp:
            raise HTTPException(status_code=500, detail="Spotify API connection failed")
        
        # Get more data for better analytics
        music_data = connector.get_trending_music_via_search(limit=50)
        
        if music_data.empty:
            raise HTTPException(status_code=404, detail="No data available for analytics")
        
        # Calculate comprehensive analytics
        analytics = {
            "summary": {
                "total_tracks": len(music_data),
                "unique_artists": int(music_data['artist'].nunique()),
                "avg_popularity": round(float(music_data['popularity'].mean()), 2),
                "data_timestamp": datetime.now().isoformat()
            },
            "top_performers": {
                "most_popular_track": {
                    "name": music_data.loc[music_data['popularity'].idxmax(), 'track_name'],
                    "artist": music_data.loc[music_data['popularity'].idxmax(), 'artist'],
                    "popularity": int(music_data['popularity'].max())
                },
                "top_artist": music_data['artist'].value_counts().index[0],
                "top_artists": music_data['artist'].value_counts().head(10).to_dict()
            },
            "genre_distribution": music_data['genre'].value_counts().to_dict() if 'genre' in music_data.columns else {},
            "popularity_stats": {
                "min": int(music_data['popularity'].min()),
                "max": int(music_data['popularity'].max()),
                "median": int(music_data['popularity'].median()),
                "std_dev": round(float(music_data['popularity'].std()), 2)
            },
            "data_quality": {
                "total_records": len(music_data),
                "complete_records": len(music_data.dropna()),
                "data_completeness": round(len(music_data.dropna()) / len(music_data) * 100, 2)
            }
        }
        
        logger.info("Analytics generated successfully")
        return analytics
        
    except Exception as e:
        logger.error(f"Error generating analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Comprehensive API health check"""
    try:
        health_status = {
            "api_status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0"
        }
        
        # Test Spotify connection
        connector = SpotifyMusicConnector()
        if connector.sp:
            # Test actual API call
            test_connection = connector.test_connection()
            health_status["spotify_api"] = "connected" if test_connection else "connection_issues"
            health_status["spotify_test"] = "passed" if test_connection else "failed"
        else:
            health_status["spotify_api"] = "disconnected"
            health_status["spotify_test"] = "failed"
        
        # Overall health
        if health_status["spotify_api"] == "connected":
            health_status["overall_status"] = "fully_operational"
        else:
            health_status["overall_status"] = "degraded"
            
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "api_status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/test")
async def test_endpoint():
    """Simple test endpoint to verify API is responding"""
    return {
        "message": "API is working!",
        "timestamp": datetime.now().isoformat(),
        "test_status": "success"
    }

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Global Music Trend API...")
    print("📡 API Documentation: http://localhost:8000/docs")
    print("🎵 Test Endpoint: http://localhost:8000/test")
    print("📊 Health Check: http://localhost:8000/health")
    logger.info("Global Music Trend API starting...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
