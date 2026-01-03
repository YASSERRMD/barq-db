"use client";

import { useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

export default function CollectionsPage() {
    const [collections, setCollections] = useState<string[]>([]);
    const [newCollection, setNewCollection] = useState({
        name: "",
        dimension: 128,
        metric: "Cosine",
    });
    const [creating, setCreating] = useState(false);
    const [message, setMessage] = useState("");
    const [isError, setIsError] = useState(false);

    const handleCreate = async () => {
        if (!newCollection.name) {
            setMessage("Please enter a collection name");
            setIsError(true);
            return;
        }

        setCreating(true);
        setMessage("");

        try {
            const res = await fetch(`${API_BASE}/api/collections`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(newCollection),
            });

            const data = await res.json();

            if (data.success) {
                setCollections([...collections, newCollection.name]);
                setMessage(data.message);
                setIsError(false);
                setNewCollection({ ...newCollection, name: "" });
            } else {
                setMessage(data.message || "Failed to create collection");
                setIsError(true);
            }
        } catch (error) {
            setMessage("Network error: Could not reach backend");
            setIsError(true);
        } finally {
            setCreating(false);
        }
    };

    return (
        <div className="animate-fade-in">
            <h1 className="text-3xl font-bold mb-8">Collections</h1>

            {/* Create Collection Form */}
            <div className="glass-card p-6 mb-8">
                <h2 className="text-xl font-semibold mb-4">Create New Collection</h2>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Name</label>
                        <input
                            type="text"
                            value={newCollection.name}
                            onChange={(e) => setNewCollection({ ...newCollection, name: e.target.value })}
                            className="input-field w-full"
                            placeholder="my_collection"
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Dimension</label>
                        <input
                            type="number"
                            value={newCollection.dimension}
                            onChange={(e) => setNewCollection({ ...newCollection, dimension: parseInt(e.target.value) })}
                            className="input-field w-full"
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Metric</label>
                        <select
                            value={newCollection.metric}
                            onChange={(e) => setNewCollection({ ...newCollection, metric: e.target.value })}
                            className="input-field w-full"
                        >
                            <option value="Cosine">Cosine</option>
                            <option value="L2">L2 (Euclidean)</option>
                            <option value="Dot">Dot Product</option>
                        </select>
                    </div>
                    <div className="flex items-end">
                        <button
                            onClick={handleCreate}
                            disabled={creating}
                            className="glow-btn w-full"
                        >
                            {creating ? "Creating..." : "Create"}
                        </button>
                    </div>
                </div>
                {message && (
                    <p className={`mt-4 text-sm ${isError ? "text-red-400" : "text-green-400"}`}>
                        {message}
                    </p>
                )}
            </div>

            {/* Collections List */}
            <div className="glass-card p-6">
                <h2 className="text-xl font-semibold mb-4">Created Collections (this session)</h2>
                {collections.length === 0 ? (
                    <p className="text-gray-500">No collections yet. Create one above!</p>
                ) : (
                    <div className="space-y-3">
                        {collections.map((name) => (
                            <div key={name} className="flex items-center justify-between p-4 bg-white/5 rounded-lg">
                                <div className="flex items-center gap-3">
                                    <span className="text-2xl">📁</span>
                                    <span className="font-medium">{name}</span>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
