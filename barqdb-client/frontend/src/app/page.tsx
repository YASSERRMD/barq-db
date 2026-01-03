"use client";

import { useEffect, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

interface Stats {
  connected: boolean;
  version: string;
  barqStatus: string;
}

export default function DashboardPage() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/health`);
        if (!res.ok) throw new Error("Failed to fetch health");
        const data = await res.json();
        setStats({
          connected: data.ok,
          version: data.version,
          barqStatus: data.barq_status,
        });
      } catch (err) {
        setError("Failed to connect to backend");
        console.error(err);
      } finally {
        setLoading(false);
      }
    };
    fetchStats();
  }, []);

  return (
    <div className="animate-fade-in">
      <h1 className="text-3xl font-bold mb-8">Dashboard</h1>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 text-red-400 p-4 rounded-lg mb-6">
          {error}
        </div>
      )}

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div className="glass-card p-6">
          <div className="flex items-center justify-between mb-4">
            <span className="text-gray-400">Connection</span>
            <span className={`w-3 h-3 rounded-full ${stats?.connected ? 'bg-green-500' : 'bg-red-500'}`} />
          </div>
          <p className="text-2xl font-bold gradient-text">
            {loading ? "..." : stats?.connected ? "Online" : "Offline"}
          </p>
        </div>

        <div className="glass-card p-6">
          <div className="flex items-center justify-between mb-4">
            <span className="text-gray-400">Client Version</span>
            <span className="text-xl">🚀</span>
          </div>
          <p className="text-2xl font-bold">{stats?.version || "..."}</p>
        </div>

        <div className="glass-card p-6">
          <div className="flex items-center justify-between mb-4">
            <span className="text-gray-400">Barq Status</span>
            <span className="text-xl">⚡</span>
          </div>
          <p className="text-2xl font-bold text-cyan-400">
            {stats?.barqStatus || "..."}
          </p>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="glass-card p-6">
        <h2 className="text-xl font-semibold mb-4">Quick Actions</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <a href="/collections" className="glass-card p-4 text-center hover:scale-105 transition-transform">
            <span className="text-3xl mb-2 block">📁</span>
            <span className="text-sm text-gray-400">New Collection</span>
          </a>
          <a href="/documents" className="glass-card p-4 text-center hover:scale-105 transition-transform">
            <span className="text-3xl mb-2 block">📄</span>
            <span className="text-sm text-gray-400">Insert Document</span>
          </a>
          <a href="/search" className="glass-card p-4 text-center hover:scale-105 transition-transform">
            <span className="text-3xl mb-2 block">🔍</span>
            <span className="text-sm text-gray-400">Search Vectors</span>
          </a>
          <a href="/admin" className="glass-card p-4 text-center hover:scale-105 transition-transform">
            <span className="text-3xl mb-2 block">📊</span>
            <span className="text-sm text-gray-400">View Stats</span>
          </a>
        </div>
      </div>
    </div>
  );
}
