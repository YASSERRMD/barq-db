"use client";

import { useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

interface SearchResult {
    id: string;
    score: number;
    payload: Record<string, unknown>;
}

export default function SearchPage() {
    const [collection, setCollection] = useState("");
    const [vectorInput, setVectorInput] = useState("");
    const [topK, setTopK] = useState(10);
    const [searching, setSearching] = useState(false);
    const [results, setResults] = useState<SearchResult[]>([]);
    const [error, setError] = useState("");

    const handleSearch = async () => {
        if (!collection || !vectorInput) {
            setError("Please enter collection name and vector");
            return;
        }

        setSearching(true);
        setError("");
        setResults([]);

        try {
            const vector = vectorInput.split(",").map((v) => parseFloat(v.trim()));

            const res = await fetch(`${API_BASE}/api/search`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ collection, vector, top_k: topK }),
            });

            if (res.ok) {
                const data = await res.json();
                setResults(data);
            } else {
                const text = await res.text();
                setError(text || "Search failed");
            }
        } catch (err) {
            setError("Network error: Could not reach backend");
        } finally {
            setSearching(false);
        }
    };

    return (
        <div className="animate-fade-in">
            <h1 className="text-3xl font-bold mb-8">Vector Search</h1>

            {/* Search Form */}
            <div className="glass-card p-6 mb-8">
                <h2 className="text-xl font-semibold mb-4">Search Parameters</h2>

                <div className="space-y-4">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div>
                            <label className="block text-sm text-gray-400 mb-2">Collection</label>
                            <input
                                type="text"
                                value={collection}
                                onChange={(e) => setCollection(e.target.value)}
                                className="input-field w-full"
                                placeholder="my_collection"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-400 mb-2">Top K</label>
                            <input
                                type="number"
                                value={topK}
                                onChange={(e) => setTopK(parseInt(e.target.value))}
                                className="input-field w-full"
                                min={1}
                                max={100}
                            />
                        </div>
                        <div className="flex items-end">
                            <button
                                onClick={handleSearch}
                                disabled={searching}
                                className="glow-btn w-full"
                            >
                                {searching ? "Searching..." : "🔍 Search"}
                            </button>
                        </div>
                    </div>

                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Query Vector (comma-separated)</label>
                        <textarea
                            value={vectorInput}
                            onChange={(e) => setVectorInput(e.target.value)}
                            className="input-field w-full h-24 font-mono text-sm"
                            placeholder="0.1, 0.2, 0.3, 0.4, 0.5..."
                        />
                    </div>
                </div>

                {error && (
                    <p className="mt-4 text-red-400">{error}</p>
                )}
            </div>

            {/* Results */}
            <div className="glass-card p-6">
                <h2 className="text-xl font-semibold mb-4">
                    Results {results.length > 0 && <span className="text-cyan-400">({results.length})</span>}
                </h2>

                {results.length === 0 ? (
                    <p className="text-gray-500">No results yet. Enter a query vector and search!</p>
                ) : (
                    <div className="space-y-4">
                        {results.map((result, index) => (
                            <div key={result.id} className="p-4 bg-white/5 rounded-lg border border-white/5">
                                <div className="flex items-center justify-between mb-2">
                                    <div className="flex items-center gap-3">
                                        <span className="text-lg font-bold text-cyan-400">#{index + 1}</span>
                                        <span className="font-medium">{result.id}</span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <span className="text-sm text-gray-400">Score:</span>
                                        <span className="text-lg font-bold text-green-400">
                                            {(result.score * 100).toFixed(1)}%
                                        </span>
                                    </div>
                                </div>
                                <div className="bg-black/20 rounded p-3 font-mono text-xs text-gray-400">
                                    {JSON.stringify(result.payload, null, 2)}
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
