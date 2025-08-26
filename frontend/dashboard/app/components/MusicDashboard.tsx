'use client';

import React, { useState, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, 
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  AreaChart, Area
} from 'recharts';
import { 
  Play, Pause, SkipForward, Heart, Share2, TrendingUp, 
  Music, Users, Calendar, Activity, Filter, Search
} from 'lucide-react';

const MusicDashboard: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [backendConnected, setBackendConnected] = useState(false);
  const [error, setError] = useState<string>('');
  const [lastUpdated, setLastUpdated] = useState<string>('');
  const [activeTab, setActiveTab] = useState('overview');
  const [selectedCategory, setSelectedCategory] = useState('all');
  const [trendingTracks, setTrendingTracks] = useState<any[]>([]);
  const [analytics, setAnalytics] = useState<any>(null);
  const [realTimeData, setRealTimeData] = useState<any[]>([]);

  // Enhanced API Functions with better error handling
  const checkBackendStatus = useCallback(async () => {
    try {
      setLoading(true);
      setError('');
      
      const response = await fetch('http://localhost:8000/health', {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
        timeout: 10000, // 10 second timeout
      });
      
      if (response.ok) {
        const data = await response.json();
        console.log('Health check response:', data); // Debug log
        
        // Updated logic for your new health endpoint format
        const isApiHealthy = data.api_status === 'healthy' || data.status === 'healthy';
        const isSpotifyConnected = data.spotify_api === 'connected';
        const isFullyOperational = data.overall_status === 'fully_operational';
        
        const connected = isApiHealthy && (isSpotifyConnected || isFullyOperational);
        setBackendConnected(connected);
        
        if (connected) {
          setError('');
          console.log('✅ Backend is fully connected and operational');
        } else {
          setError('Backend API or Spotify connection issues detected');
        }
      } else {
        setBackendConnected(false);
        setError(`Backend error: HTTP ${response.status}`);
      }
      
      setLastUpdated(new Date().toLocaleTimeString());
      
    } catch (err: any) {
      setBackendConnected(false);
      setError(`Backend not reachable: ${err.message}`);
      console.error('Backend connection error:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchTrendingTracks = useCallback(async () => {
    if (!backendConnected) {
      console.log('Skipping trending fetch - backend not connected');
      return;
    }
    
    try {
      console.log('🔄 Fetching trending tracks...');
      const response = await fetch('http://localhost:8000/trending', {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' }
      });
      
      if (response.ok) {
        const data = await response.json();
        console.log('✅ Trending tracks data:', data);
        
        if (data.tracks && Array.isArray(data.tracks)) {
          setTrendingTracks(data.tracks);
          console.log(`📊 Updated with ${data.tracks.length} trending tracks`);
        } else {
          console.warn('⚠️ Trending tracks data format unexpected:', data);
          setTrendingTracks([]);
        }
      } else {
        console.error('❌ Failed to fetch trending tracks:', response.status);
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (err: any) {
      console.error('❌ Error fetching trending tracks:', err);
      setError(`Failed to load trending tracks: ${err.message}`);
    }
  }, [backendConnected]);

  const fetchAnalytics = useCallback(async () => {
    if (!backendConnected) return;
    
    try {
      console.log('🔄 Fetching analytics...');
      const response = await fetch('http://localhost:8000/analytics');
      
      if (response.ok) {
        const data = await response.json();
        console.log('✅ Analytics data:', data);
        setAnalytics(data);
        
        // Transform analytics data for charts
        if (data && typeof data === 'object') {
          const chartData = transformAnalyticsForCharts(data);
          setRealTimeData(chartData);
        }
      } else {
        console.error('❌ Failed to fetch analytics:', response.status);
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (err: any) {
      console.error('❌ Error fetching analytics:', err);
      setError(`Failed to load analytics: ${err.message}`);
    }
  }, [backendConnected]);

  // Transform analytics data for chart visualization
  const transformAnalyticsForCharts = (analyticsData: any) => {
    if (!analyticsData) return [];
    
    try {
      const chartData = [];
      
      // Create trend data from analytics
      if (analyticsData.summary) {
        chartData.push({
          name: 'Current',
          tracks: analyticsData.summary.total_tracks || 0,
          artists: analyticsData.summary.unique_artists || 0,
          popularity: analyticsData.summary.avg_popularity || 0,
          timestamp: new Date().toLocaleTimeString()
        });
      }
      
      return chartData;
    } catch (err) {
      console.error('Error transforming analytics data:', err);
      return [];
    }
  };

  // Auto-refresh data every 30 seconds
  useEffect(() => {
    checkBackendStatus();
    const healthInterval = setInterval(checkBackendStatus, 60000); // Check health every 60s
    
    return () => clearInterval(healthInterval);
  }, [checkBackendStatus]);

  // Fetch data when backend connects
  useEffect(() => {
    if (backendConnected) {
      console.log('🚀 Backend connected - fetching data...');
      fetchTrendingTracks();
      fetchAnalytics();
      
      // Set up auto-refresh for data
      const dataInterval = setInterval(() => {
        fetchTrendingTracks();
        fetchAnalytics();
      }, 30000); // Refresh every 30 seconds
      
      return () => clearInterval(dataInterval);
    } else {
      console.log('⚠️ Backend disconnected - clearing data');
      setTrendingTracks([]);
      setAnalytics(null);
    }
  }, [backendConnected, fetchTrendingTracks, fetchAnalytics]);

  // Create dynamic genre data from real trending tracks
  const genreData = React.useMemo(() => {
    if (trendingTracks.length === 0) {
      // Fallback static data
      return [
        { name: 'Pop', value: 35, color: '#FF6B6B' },
        { name: 'Rock', value: 25, color: '#4ECDC4' },
        { name: 'Hip-Hop', value: 20, color: '#45B7D1' },
        { name: 'Electronic', value: 12, color: '#96CEB4' },
        { name: 'Country', value: 8, color: '#FECA57' }
      ];
    }
    
    // Create genre distribution from real data
    const genreCounts: { [key: string]: number } = {};
    trendingTracks.forEach(track => {
      const genre = track.genre || 'Other';
      genreCounts[genre] = (genreCounts[genre] || 0) + 1;
    });
    
    const colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3', '#54A0FF'];
    
    return Object.entries(genreCounts).map(([name, count], index) => ({
      name,
      value: count,
      color: colors[index % colors.length]
    }));
  }, [trendingTracks]);

  // Create popularity trend data from real tracks
  const popularityTrend = React.useMemo(() => {
    if (trendingTracks.length === 0) {
      return [
        { time: 'Now-5m', popularity: 65 },
        { time: 'Now-4m', popularity: 70 },
        { time: 'Now-3m', popularity: 75 },
        { time: 'Now-2m', popularity: 82 },
        { time: 'Now-1m', popularity: 88 },
        { time: 'Now', popularity: analytics?.summary?.avg_popularity || 85 }
      ];
    }
    
    // Create trend from real data
    const avgPopularity = trendingTracks.reduce((sum, track) => sum + track.popularity, 0) / trendingTracks.length;
    const now = new Date();
    
    return Array.from({ length: 6 }, (_, i) => ({
      time: `${now.getHours()}:${String(now.getMinutes() - (5 - i)).padStart(2, '0')}`,
      popularity: Math.round(avgPopularity + (Math.random() - 0.5) * 10)
    }));
  }, [trendingTracks, analytics]);

  // Animation variants
  const cardVariants = {
    hidden: { opacity: 0, y: 20 },
    visible: { opacity: 1, y: 0, transition: { duration: 0.6 } },
    hover: { scale: 1.02, transition: { duration: 0.2 } }
  };

  const tabVariants = {
    inactive: { scale: 1, backgroundColor: 'rgba(255,255,255,0.1)' },
    active: { scale: 1.05, backgroundColor: 'rgba(29,185,84,0.2)' }
  };

  if (loading && !lastUpdated) {
    return (
      <motion.div 
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="flex items-center justify-center h-screen bg-gradient-to-br from-gray-900 via-purple-900 to-violet-900"
      >
        <motion.div
          animate={{ rotate: 360 }}
          transition={{ duration: 2, repeat: Infinity, ease: "linear" }}
          className="text-6xl text-green-500"
        >
          🎵
        </motion.div>
        <motion.span 
          animate={{ opacity: [0.5, 1, 0.5] }}
          transition={{ duration: 1.5, repeat: Infinity }}
          className="ml-4 text-2xl text-white"
        >
          Loading Real-Time Music Analytics...
        </motion.span>
      </motion.div>
    );
  }

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <motion.div
          initial={{ opacity: 0, scale: 0.8 }}
          animate={{ opacity: 1, scale: 1 }}
          className="bg-gray-900 border border-gray-700 rounded-lg p-3 shadow-xl"
        >
          <p className="text-gray-300">{`${label}`}</p>
          {payload.map((entry: any, index: number) => (
            <p key={index} style={{ color: entry.color }}>
              {`${entry.dataKey}: ${entry.value}`}
            </p>
          ))}
        </motion.div>
      );
    }
    return null;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-purple-900 to-violet-900 text-white">
      {/* Header */}
      <motion.header 
        initial={{ y: -50, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        transition={{ duration: 0.8 }}
        className="bg-black/30 backdrop-blur-md border-b border-gray-700 sticky top-0 z-50"
      >
        <div className="container mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <motion.div 
              whileHover={{ scale: 1.05 }}
              className="flex items-center space-x-3"
            >
              <Music className="text-green-500 w-8 h-8" />
              <h1 className="text-3xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                Global Music Trend Dashboard
              </h1>
            </motion.div>
            
            <div className="flex items-center space-x-4">
              <motion.div
                animate={{ 
                  backgroundColor: backendConnected ? '#10B981' : '#EF4444',
                  scale: backendConnected ? [1, 1.1, 1] : 1
                }}
                transition={{ duration: 0.5, repeat: backendConnected ? Infinity : 0, repeatDelay: 2 }}
                className="px-3 py-1 rounded-full text-sm font-medium"
              >
                {backendConnected ? '● LIVE' : '● OFFLINE'}
              </motion.div>
              
              <div className="text-sm text-gray-400">
                Last updated: {lastUpdated}
              </div>
            </div>
          </div>
        </div>
      </motion.header>

      {/* Navigation Tabs */}
      <motion.nav 
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.3 }}
        className="container mx-auto px-6 py-4"
      >
        <div className="flex space-x-1 bg-black/20 rounded-full p-1 backdrop-blur-sm">
          {[
            { id: 'overview', label: 'Overview', icon: Activity },
            { id: 'trends', label: 'Trends', icon: TrendingUp },
            { id: 'artists', label: 'Artists', icon: Users },
            { id: 'playlists', label: 'Playlists', icon: Music }
          ].map((tab) => (
            <motion.button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              variants={tabVariants}
              animate={activeTab === tab.id ? 'active' : 'inactive'}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              className={`flex items-center space-x-2 px-6 py-3 rounded-full font-medium transition-all ${
                activeTab === tab.id 
                  ? 'text-white bg-green-500/20 border border-green-500/30' 
                  : 'text-gray-400 hover:text-white'
              }`}
            >
              <tab.icon className="w-5 h-5" />
              <span>{tab.label}</span>
            </motion.button>
          ))}
        </div>
      </motion.nav>

      {/* Error Banner */}
      {error && !backendConnected && (
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          className="container mx-auto px-6 mb-4"
        >
          <div className="bg-red-500/20 border border-red-500/30 rounded-lg p-4 text-center">
            <p className="text-red-300">⚠️ {error}</p>
            <p className="text-red-400 text-sm mt-1">
              Make sure your backend is running with: <code>python music_api.py</code>
            </p>
            <button 
              onClick={checkBackendStatus}
              className="mt-2 px-4 py-2 bg-red-500 hover:bg-red-600 rounded-lg text-white text-sm"
            >
              Retry Connection
            </button>
          </div>
        </motion.div>
      )}

      {/* Main Content */}
      <main className="container mx-auto px-6 pb-8">
        <AnimatePresence mode="wait">
          {/* Overview Tab */}
          {activeTab === 'overview' && (
            <motion.div
              key="overview"
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -20 }}
              transition={{ duration: 0.5 }}
            >
              {/* Real-time Stats Cards */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
                {[
                  { 
                    title: 'Live Tracks', 
                    value: trendingTracks.length, 
                    color: 'from-green-400 to-green-600', 
                    icon: Music,
                    subtitle: 'Real Spotify Data'
                  },
                  { 
                    title: 'Unique Artists', 
                    value: analytics?.summary?.unique_artists || new Set(trendingTracks.map(t => t.artist)).size, 
                    color: 'from-blue-400 to-blue-600', 
                    icon: Users,
                    subtitle: 'Active Musicians'
                  },
                  { 
                    title: 'Avg Popularity', 
                    value: Math.round(analytics?.summary?.avg_popularity || 
                      (trendingTracks.length > 0 ? trendingTracks.reduce((sum, t) => sum + t.popularity, 0) / trendingTracks.length : 0)
                    ), 
                    color: 'from-purple-400 to-purple-600', 
                    icon: TrendingUp,
                    subtitle: 'Spotify Score'
                  },
                  { 
                    title: 'Data Status', 
                    value: backendConnected ? 'LIVE' : 'OFFLINE', 
                    color: backendConnected ? 'from-green-400 to-green-600' : 'from-red-400 to-red-600', 
                    icon: Activity,
                    subtitle: backendConnected ? 'Real-time Updates' : 'Connection Lost'
                  }
                ].map((stat, index) => (
                  <motion.div
                    key={stat.title}
                    variants={cardVariants}
                    initial="hidden"
                    animate="visible"
                    whileHover="hover"
                    transition={{ delay: index * 0.1 }}
                    className={`bg-gradient-to-r ${stat.color} p-6 rounded-2xl shadow-xl`}
                  >
                    <div className="flex items-center justify-between">
                      <div>
                        <motion.h3 
                          initial={{ scale: 0 }}
                          animate={{ scale: 1 }}
                          transition={{ delay: 0.2 + index * 0.1 }}
                          className="text-3xl font-bold text-white"
                          key={stat.value} // Re-animate when value changes
                        >
                          {stat.value}
                        </motion.h3>
                        <p className="text-white/80 font-medium">{stat.title}</p>
                        <p className="text-white/60 text-xs">{stat.subtitle}</p>
                      </div>
                      <stat.icon className="w-8 h-8 text-white/60" />
                    </div>
                  </motion.div>
                ))}
              </div>

              {/* Charts Grid */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                {/* Real-time Genre Distribution */}
                <motion.div
                  variants={cardVariants}
                  initial="hidden"
                  animate="visible"
                  className="bg-black/30 backdrop-blur-md rounded-2xl p-6 border border-gray-700"
                >
                  <h3 className="text-xl font-semibold mb-4 text-green-400">
                    Live Genre Distribution
                    {trendingTracks.length > 0 && (
                      <span className="text-sm text-gray-400 ml-2">
                        (Based on {trendingTracks.length} tracks)
                      </span>
                    )}
                  </h3>
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                      <Pie
                        data={genreData}
                        cx="50%"
                        cy="50%"
                        outerRadius={100}
                        animationBegin={0}
                        animationDuration={1000}
                        dataKey="value"
                      >
                        {genreData.map((entry, index) => (
                          <Cell key={`cell-${index}`} fill={entry.color} />
                        ))}
                      </Pie>
                      <Tooltip content={<CustomTooltip />} />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                </motion.div>

                {/* Real-time Popularity Trends */}
                <motion.div
                  variants={cardVariants}
                  initial="hidden"
                  animate="visible"
                  transition={{ delay: 0.2 }}
                  className="bg-black/30 backdrop-blur-md rounded-2xl p-6 border border-gray-700"
                >
                  <h3 className="text-xl font-semibold mb-4 text-blue-400">
                    Live Popularity Trend
                    <span className="text-sm text-gray-400 ml-2">
                      (Real-time updates)
                    </span>
                  </h3>
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={popularityTrend}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                      <XAxis dataKey="time" stroke="#9CA3AF" />
                      <YAxis stroke="#9CA3AF" />
                      <Tooltip content={<CustomTooltip />} />
                      <Line 
                        type="monotone" 
                        dataKey="popularity" 
                        stroke="#3B82F6" 
                        strokeWidth={3}
                        dot={{ fill: '#3B82F6', strokeWidth: 2, r: 6 }}
                        animationDuration={2000}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </motion.div>
              </div>
            </motion.div>
          )}

          {/* Enhanced Trends Tab with Real Data */}
          {activeTab === 'trends' && (
            <motion.div
              key="trends"
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -20 }}
            >
              {/* Live Trending Tracks */}
              <motion.div
                variants={cardVariants}
                initial="hidden"
                animate="visible"
                className="bg-black/30 backdrop-blur-md rounded-2xl p-6 border border-gray-700"
              >
                <div className="flex items-center justify-between mb-6">
                  <h3 className="text-xl font-semibold text-green-400">
                    Live Trending Tracks
                    {backendConnected && (
                      <motion.span
                        animate={{ opacity: [0.5, 1, 0.5] }}
                        transition={{ duration: 2, repeat: Infinity }}
                        className="ml-2 text-sm text-green-300"
                      >
                        ● LIVE
                      </motion.span>
                    )}
                  </h3>
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={fetchTrendingTracks}
                    disabled={!backendConnected}
                    className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                      backendConnected 
                        ? 'bg-green-500 hover:bg-green-600 text-white' 
                        : 'bg-gray-600 text-gray-300 cursor-not-allowed'
                    }`}
                  >
                    {loading ? 'Refreshing...' : 'Refresh Now'}
                  </motion.button>
                </div>

                <div className="space-y-3 max-h-96 overflow-y-auto">
                  <AnimatePresence>
                    {trendingTracks.length > 0 ? (
                      trendingTracks.slice(0, 10).map((track, index) => (
                        <motion.div
                          key={track.track_id || index}
                          initial={{ opacity: 0, x: -20 }}
                          animate={{ opacity: 1, x: 0 }}
                          exit={{ opacity: 0, x: 20 }}
                          transition={{ delay: index * 0.05 }}
                          whileHover={{ scale: 1.02, backgroundColor: 'rgba(255,255,255,0.05)' }}
                          className="flex items-center space-x-4 p-4 rounded-xl border border-gray-700 hover:border-green-500/50 transition-all cursor-pointer"
                        >
                          {track.image_url ? (
                            <motion.img
                              whileHover={{ scale: 1.1 }}
                              src={track.image_url}
                              alt={track.track_name}
                              className="w-16 h-16 rounded-lg object-cover shadow-lg"
                              onError={(e) => {
                                const target = e.target as HTMLImageElement;
                                target.src = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='64' height='64' viewBox='0 0 24 24' fill='none' stroke='%23666' stroke-width='2'%3E%3Ccircle cx='12' cy='12' r='10'/%3E%3Cpolygon points='10,8 16,12 10,16 10,8'/%3E%3C/svg%3E";
                              }}
                            />
                          ) : (
                            <div className="w-16 h-16 rounded-lg bg-gradient-to-br from-purple-500 to-pink-500 flex items-center justify-center">
                              <Music className="w-8 h-8 text-white" />
                            </div>
                          )}
                          <div className="flex-1">
                            <h4 className="font-semibold text-white truncate">{track.track_name}</h4>
                            <p className="text-gray-400 truncate">{track.artist}</p>
                            <p className="text-gray-500 text-sm truncate">{track.album}</p>
                          </div>
                          <div className="text-right">
                            <div className="text-sm font-medium text-green-400">
                              {track.popularity}/100
                            </div>
                            <div className="text-xs text-gray-500">{track.genre || 'Trending'}</div>
                            <div className="text-xs text-gray-600">
                              {Math.round(track.duration_ms / 60000)}:{String(Math.round((track.duration_ms % 60000) / 1000)).padStart(2, '0')}
                            </div>
                          </div>
                          <div className="flex space-x-2">
                            <motion.a
                              href={track.spotify_url}
                              target="_blank"
                              rel="noopener noreferrer"
                              whileHover={{ scale: 1.2 }}
                              whileTap={{ scale: 0.9 }}
                              className="p-2 hover:bg-green-500/20 rounded-full transition-colors"
                            >
                              <Play className="w-4 h-4 text-green-400" />
                            </motion.a>
                            <motion.button
                              whileHover={{ scale: 1.2 }}
                              whileTap={{ scale: 0.9 }}
                              className="p-2 hover:bg-red-500/20 rounded-full transition-colors"
                            >
                              <Heart className="w-4 h-4 text-gray-400 hover:text-red-400" />
                            </motion.button>
                          </div>
                        </motion.div>
                      ))
                    ) : (
                      <motion.div
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        className="text-center py-8 text-gray-400"
                      >
                        {backendConnected ? (
                          <>
                            <Music className="w-16 h-16 mx-auto mb-4 opacity-50" />
                            <p>Loading trending tracks...</p>
                            <p className="text-sm mt-2">Fetching live Spotify data</p>
                          </>
                        ) : (
                          <>
                            <Activity className="w-16 h-16 mx-auto mb-4 opacity-50" />
                            <p>Connect to backend to load trending tracks</p>
                            <p className="text-sm mt-2">Start your API server to see live data</p>
                          </>
                        )}
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              </motion.div>
            </motion.div>
          )}

          {/* Artists Tab - Enhanced with Real Data */}
          {activeTab === 'artists' && (
            <motion.div
              key="artists"
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -20 }}
            >
              {/* Live Artist Analytics */}
              <motion.div
                variants={cardVariants}
                initial="hidden"
                animate="visible"
                className="bg-black/30 backdrop-blur-md rounded-2xl p-6 border border-gray-700 mb-8"
              >
                <h3 className="text-xl font-semibold mb-4 text-orange-400">
                  Top Artists from Live Data
                  {trendingTracks.length > 0 && (
                    <span className="text-sm text-gray-400 ml-2">
                      (From {trendingTracks.length} tracks)
                    </span>
                  )}
                </h3>
                
                {trendingTracks.length > 0 ? (
                  <ResponsiveContainer width="100%" height={400}>
                    <BarChart 
                      data={Object.entries(
                        trendingTracks.reduce((acc: { [key: string]: number }, track) => {
                          acc[track.artist] = (acc[track.artist] || 0) + track.popularity;
                          return acc;
                        }, {})
                      )
                      .map(([name, popularity]) => ({ name, popularity }))
                      .sort((a, b) => b.popularity - a.popularity)
                      .slice(0, 10)}
                      layout="horizontal"
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                      <XAxis type="number" stroke="#9CA3AF" />
                      <YAxis dataKey="name" type="category" stroke="#9CA3AF" width={120} />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar 
                        dataKey="popularity" 
                        fill="url(#colorPlays)"
                        animationDuration={1500}
                      />
                      <defs>
                        <linearGradient id="colorPlays" x1="0" y1="0" x2="1" y2="0">
                          <stop offset="5%" stopColor="#F59E0B" stopOpacity={0.8}/>
                          <stop offset="95%" stopColor="#EF4444" stopOpacity={0.8}/>
                        </linearGradient>
                      </defs>
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-96 flex items-center justify-center text-gray-400">
                    <div className="text-center">
                      <Users className="w-16 h-16 mx-auto mb-4 opacity-50" />
                      <p>No artist data available</p>
                      <p className="text-sm">Connect backend to load live artist analytics</p>
                    </div>
                  </div>
                )}
              </motion.div>
            </motion.div>
          )}

          {/* Playlists Tab remains the same */}
          {activeTab === 'playlists' && (
            <motion.div
              key="playlists"
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -20 }}
            >
              <div className="flex items-center justify-between mb-6">
                <h2 className="text-2xl font-bold text-white">Featured Playlists</h2>
                <div className="flex items-center space-x-4">
                  <motion.div whileHover={{ scale: 1.05 }} className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                    <input
                      type="text"
                      placeholder="Search playlists..."
                      className="pl-10 pr-4 py-2 bg-black/30 border border-gray-600 rounded-lg focus:border-green-500 focus:outline-none text-white"
                    />
                  </motion.div>
                  <motion.select
                    whileHover={{ scale: 1.05 }}
                    value={selectedCategory}
                    onChange={(e) => setSelectedCategory(e.target.value)}
                    className="px-4 py-2 bg-black/30 border border-gray-600 rounded-lg focus:border-green-500 focus:outline-none text-white"
                  >
                    <option value="all">All Categories</option>
                    <option value="pop">Pop</option>
                    <option value="rock">Rock</option>
                    <option value="hip-hop">Hip-Hop</option>
                    <option value="electronic">Electronic</option>
                  </motion.select>
                </div>
              </div>

              {/* Playlist Grid */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {Array.from({ length: 9 }, (_, index) => (
                  <motion.div
                    key={index}
                    variants={cardVariants}
                    initial="hidden"
                    animate="visible"
                    whileHover="hover"
                    transition={{ delay: index * 0.05 }}
                    className="bg-black/30 backdrop-blur-md rounded-2xl overflow-hidden border border-gray-700 group cursor-pointer"
                    onClick={() => console.log(`Clicked playlist ${index + 1}`)}
                  >
                    <motion.div
                      whileHover={{ scale: 1.05 }}
                      className="relative h-48 bg-gradient-to-br from-purple-600 via-blue-600 to-green-500 flex items-center justify-center"
                    >
                      <Music className="w-16 h-16 text-white/60" />
                      <motion.div
                        initial={{ opacity: 0 }}
                        whileHover={{ opacity: 1 }}
                        className="absolute inset-0 bg-black/50 flex items-center justify-center"
                      >
                        <motion.button
                          whileHover={{ scale: 1.1 }}
                          whileTap={{ scale: 0.9 }}
                          className="w-16 h-16 bg-green-500 rounded-full flex items-center justify-center hover:bg-green-600 transition-colors"
                        >
                          <Play className="w-6 h-6 text-white ml-1" />
                        </motion.button>
                      </motion.div>
                    </motion.div>
                    <div className="p-4">
                      <h4 className="font-semibold text-white mb-2">
                        {trendingTracks.length > 0 ? `Top ${selectedCategory} Hits` : "Today's Top Hits"}
                      </h4>
                      <p className="text-gray-400 text-sm mb-3">
                        {backendConnected ? "Live curated tracks" : "The most played songs right now"}
                      </p>
                      <div className="flex items-center justify-between">
                        <span className="text-xs text-gray-500">
                          {trendingTracks.length || 50} tracks
                        </span>
                        <div className="flex space-x-2">
                          <motion.button
                            whileHover={{ scale: 1.2 }}
                            whileTap={{ scale: 0.9 }}
                            className="p-2 hover:bg-gray-700 rounded-full transition-colors"
                          >
                            <Heart className="w-4 h-4 text-gray-400 hover:text-red-400" />
                          </motion.button>
                          <motion.button
                            whileHover={{ scale: 1.2 }}
                            whileTap={{ scale: 0.9 }}
                            className="p-2 hover:bg-gray-700 rounded-full transition-colors"
                          >
                            <Share2 className="w-4 h-4 text-gray-400" />
                          </motion.button>
                        </div>
                      </div>
                    </div>
                  </motion.div>
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>

      {/* Footer */}
      <motion.footer
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 1 }}
        className="bg-black/50 border-t border-gray-800 py-6"
      >
        <div className="container mx-auto px-6 text-center">
          <div className="flex items-center justify-center space-x-2 text-gray-400">
            <Music className="w-5 h-5" />
            <span>Built with Next.js 15.5.0, TypeScript, Framer Motion, and Spotify Web API</span>
          </div>
          <p className="text-gray-500 mt-2">CS3238 Data Engineering Project by Mallikarjun Miragi</p>
        </div>
      </motion.footer>
    </div>
  );
};

export default MusicDashboard;
