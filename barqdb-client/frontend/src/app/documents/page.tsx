"use client";

import { useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

export default function DocumentsPage() {
    const [collection, setCollection] = useState("");
    const [docId, setDocId] = useState("");
    const [vectorInput, setVectorInput] = useState("");
    const [payloadInput, setPayloadInput] = useState("{}");
    const [inserting, setInserting] = useState(false);
    const [result, setResult] = useState<{ success: boolean; message: string } | null>(null);

    const handleInsert = async () => {
        if (!collection || !docId || !vectorInput) {
            setResult({ success: false, message: "Please fill in all required fields" });
            return;
        }

        setInserting(true);
        setResult(null);

        try {
            const vector = vectorInput.split(",").map((v) => parseFloat(v.trim()));
            let payload = {};
            try {
                payload = JSON.parse(payloadInput);
            } catch {
                setResult({ success: false, message: "Invalid JSON payload" });
                setInserting(false);
                return;
            }

            const res = await fetch(`${API_BASE}/api/documents`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ collection, id: docId, vector, payload }),
            });

            if (res.ok) {
                setResult({ success: true, message: `Document '${docId}' inserted successfully!` });
                setDocId("");
                setVectorInput("");
            } else {
                const text = await res.text();
                setResult({ success: false, message: text || "Failed to insert document" });
            }
        } catch (error) {
            setResult({ success: false, message: "Network error: Could not reach backend" });
        } finally {
            setInserting(false);
        }
    };

    return (
        <div className="animate-fade-in">
            <h1 className="text-3xl font-bold mb-8">Documents</h1>

            {/* Insert Document Form */}
            <div className="glass-card p-6">
                <h2 className="text-xl font-semibold mb-6">Insert Document</h2>

                <div className="space-y-4">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                            <label className="block text-sm text-gray-400 mb-2">Collection Name *</label>
                            <input
                                type="text"
                                value={collection}
                                onChange={(e) => setCollection(e.target.value)}
                                className="input-field w-full"
                                placeholder="my_collection"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-400 mb-2">Document ID *</label>
                            <input
                                type="text"
                                value={docId}
                                onChange={(e) => setDocId(e.target.value)}
                                className="input-field w-full"
                                placeholder="doc_001"
                            />
                        </div>
                    </div>

                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Vector (comma-separated) *</label>
                        <input
                            type="text"
                            value={vectorInput}
                            onChange={(e) => setVectorInput(e.target.value)}
                            className="input-field w-full"
                            placeholder="0.1, 0.2, 0.3, 0.4, 0.5"
                        />
                        <p className="text-xs text-gray-600 mt-1">
                            Enter values separated by commas. Count: {vectorInput ? vectorInput.split(",").filter(v => v.trim()).length : 0}
                        </p>
                    </div>

                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Payload (JSON)</label>
                        <textarea
                            value={payloadInput}
                            onChange={(e) => setPayloadInput(e.target.value)}
                            className="input-field w-full h-32 font-mono text-sm"
                            placeholder='{"key": "value"}'
                        />
                    </div>

                    <button
                        onClick={handleInsert}
                        disabled={inserting}
                        className="glow-btn"
                    >
                        {inserting ? "Inserting..." : "Insert Document"}
                    </button>

                    {result && (
                        <div className={`p-4 rounded-lg ${result.success ? "bg-green-500/10 text-green-400" : "bg-red-500/10 text-red-400"}`}>
                            {result.message}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
