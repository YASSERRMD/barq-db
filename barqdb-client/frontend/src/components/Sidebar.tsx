"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";

const navItems = [
    { href: "/", label: "Dashboard", icon: "📊" },
    { href: "/collections", label: "Collections", icon: "📁" },
    { href: "/documents", label: "Documents", icon: "📄" },
    { href: "/search", label: "Search", icon: "🔍" },
    { href: "/admin", label: "Admin", icon: "⚙️" },
];

export default function Sidebar() {
    const pathname = usePathname();

    return (
        <aside className="sidebar fixed left-0 top-0 h-screen w-64 p-6 flex flex-col">
            {/* Logo */}
            <div className="flex items-center gap-3 mb-10">
                <Image
                    src="/logo.jpg"
                    alt="BarqDB Logo"
                    width={48}
                    height={48}
                    className="rounded-xl"
                />
                <div>
                    <h1 className="text-xl font-bold gradient-text">BarqDB</h1>
                    <p className="text-xs text-gray-500">Client Console</p>
                </div>
            </div>

            {/* Navigation */}
            <nav className="flex-1 space-y-2">
                {navItems.map((item) => (
                    <Link
                        key={item.href}
                        href={item.href}
                        className={`sidebar-link ${pathname === item.href ? "active" : ""}`}
                    >
                        <span className="text-xl">{item.icon}</span>
                        <span>{item.label}</span>
                    </Link>
                ))}
            </nav>

            {/* Footer */}
            <div className="pt-6 border-t border-white/5">
                <div className="text-xs text-gray-600">
                    <p>Connected to gRPC</p>
                    <p className="text-cyan-500">localhost:3001</p>
                </div>
            </div>
        </aside>
    );
}
