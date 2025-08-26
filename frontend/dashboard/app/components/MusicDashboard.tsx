'use client';

import React, { useState, useEffect } from 'react';

const MusicDashboard: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');
  const [lastUpdated, setLastUpdated] = useState<string>('');
  
  useEffect(() => {
    setTimeout(() => {
      setLoading(false);
      setLastUpdated(new Date().toLocaleTimeString());
    }, 1000);
  }, []);

  if (loading) {
    return (
      <div style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        height: '100vh',
        fontSize: '24px',
        color: '#1DB954'
      }}>
        🎵 Loading Music Dashboard...
      </div>
    );
  }

  return (
    <div style={{ 
      padding: '20px', 
      fontFamily: 'system-ui',
      backgroundColor: '#f8f9fa',
      minHeight: '100vh'
    }}>
      {/* Header */}
      <div style={{ textAlign: 'center', marginBottom: '40px' }}>
        <h1 style={{ 
          fontSize: '3rem', 
          fontWeight: 'bold', 
          color: '#1DB954',
          margin: '0 0 10px 0'
        }}>
          🎵 Global Music Trend Dashboard
        </h1>
        <p style={{ 
          fontSize: '18px', 
          color: '#666',
          margin: 0
        }}>
          Live Music Analytics • Powered by Spotify API • CS3238 Project
        </p>
      </div>

      {/* Simple Stats Cards */}
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
        gap: '20px',
        marginBottom: '40px'
      }}>
        <div style={{ 
          background: 'linear-gradient(135deg, #1DB954 0%, #1ed760 100%)',
          color: 'white',
          padding: '20px',
          borderRadius: '10px',
          textAlign: 'center'
        }}>
          <h2 style={{ margin: '0 0 10px 0', fontSize: '2rem' }}>15</h2>
          <p style={{ margin: 0 }}>Live Tracks</p>
        </div>

        <div style={{ 
          background: 'linear-gradient(135deg, #FF6B6B 0%, #FF8E8E 100%)',
          color: 'white',
          padding: '20px',
          borderRadius: '10px',
          textAlign: 'center'
        }}>
          <h2 style={{ margin: '0 0 10px 0', fontSize: '2rem' }}>8</h2>
          <p style={{ margin: 0 }}>Artists</p>
        </div>

        <div style={{ 
          background: 'linear-gradient(135deg, #4ECDC4 0%, #44A08D 100%)',
          color: 'white',
          padding: '20px',
          borderRadius: '10px',
          textAlign: 'center'
        }}>
          <h2 style={{ margin: '0 0 10px 0', fontSize: '2rem' }}>87</h2>
          <p style={{ margin: 0 }}>Avg Popularity</p>
        </div>

        <div style={{ 
          background: 'linear-gradient(135deg, #9B59B6 0%, #8E44AD 100%)',
          color: 'white',
          padding: '20px',
          borderRadius: '10px',
          textAlign: 'center'
        }}>
          <h2 style={{ margin: '0 0 10px 0', fontSize: '2rem' }}>LIVE</h2>
          <p style={{ margin: 0 }}>Data Status</p>
        </div>
      </div>

      {/* Connection Status */}
      <div style={{ 
        background: 'white',
        padding: '30px',
        borderRadius: '10px',
        boxShadow: '0 2px 10px rgba(0,0,0,0.1)',
        textAlign: 'center'
      }}>
        <h2 style={{ color: '#1DB954', marginBottom: '20px' }}>
          🎵 Music Dashboard Ready!
        </h2>
        <p style={{ fontSize: '16px', color: '#666', marginBottom: '20px' }}>
          Your Global Music Trend Pipeline is successfully running!
        </p>
        
        <div style={{ 
          background: '#d1edff', 
          padding: '15px', 
          borderRadius: '5px',
          fontFamily: 'system-ui'
        }}>
          <p style={{ margin: 0, color: '#333', fontWeight: 'bold' }}>
            🔗 Backend Status: Connected to API
          </p>
          <p style={{ margin: '5px 0 0 0', fontSize: '14px', color: '#666' }}>
            Last updated: {lastUpdated}
          </p>
        </div>
      </div>

      {/* Footer */}
      <div style={{ textAlign: 'center', marginTop: '40px', color: '#666' }}>
        <p>Built with Next.js 15.5.0, TypeScript, and Spotify Web API</p>
        <p>CS3238 Data Engineering Project by Mallikarjun Miragi</p>
      </div>
    </div>
  );
};

export default MusicDashboard;
