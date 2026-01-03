"use client";

import { useEffect, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

interface Stats {
    connected: boolean;
    grpc_addr: string;
    uptime_secs: number;
}

export default function AdminPage() {
    const [stats, setStats] = useState<Stats | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    const fetchStats = async () => {
        setLoading(true);
        try {
            const res = await fetch(`${API_BASE}/api/stats`);
            if (!res.ok) throw new Error("Failed to fetch stats");
            const data = await res.json();
            setStats(data);
            setError("");
        } catch (err) {
            setError("Failed to connect to backend");
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchStats();
    }, []);

    const formatUptime = (secs: number) => {
        const hours = Math.floor(secs / 3600);
        const mins = Math.floor((secs % 3600) / 60);
        return `${hours}h ${mins}m`;
    };

    return (
        <div className="animate-fade-in">
            <h1 className="text-3xl font-bold mb-8">Admin Panel</h1>

            {error && (
                <div className="bg-red-500/10 border border-red-500/30 text-red-400 p-4 rounded-lg mb-6">
                    {error}
                </div>
            )}

            {/* Server Stats */}
            <div className="glass-card p-6 mb-8">
                <h2 className="text-xl font-semibold mb-6">Server Statistics</h2>

                {loading ? (
                    <p className="text-gray-500">Loading...</p>
                ) : stats ? (
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                        <div className="p-4 bg-white/5 rounded-lg">
                            <p className="text-sm text-gray-400 mb-1">Connection Status</p>
                            <div className="flex items-center gap-2">
                                <span className={`w-3 h-3 rounded-full ${stats.connected ? "bg-green-500" : "bg-red-500"}`} />
                                <span className="text-xl font-bold">
                                    {stats.connected ? "Connected" : "Disconnected"}
                                </span>
                            </div>
                        </div>

                        <div className="p-4 bg-white/5 rounded-lg">
                            <p className="text-sm text-gray-400 mb-1">gRPC Address</p>
                            <p className="text-xl font-mono text-cyan-400">{stats.grpc_addr}</p>
                        </div>

                        <div className="p-4 bg-white/5 rounded-lg">
                            <p className="text-sm text-gray-400 mb-1">Uptime</p>
                            <p className="text-xl font-bold">{formatUptime(stats.uptime_secs)}</p>
                        </div>
                    </div>
                ) : null}
            </div>

            {/* Actions */}
            <div className="glass-card p-6">
                <h2 className="text-xl font-semibold mb-4">Actions</h2>

                <div className="flex flex-wrap gap-4">
                    <button onClick={fetchStats} className="glow-btn">
                        🔄 Refresh Stats
                    </button>
                </div>
            </div>
        </div>
    );
}
