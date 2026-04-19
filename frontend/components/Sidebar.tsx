"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useAuth } from "@/lib/auth";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard, Users, FolderOpen, ShieldAlert, LogOut,
  Settings, Network, Bell, Map,
} from "lucide-react";

type NavItem = {
  href: string;
  label: string;
  icon: React.ComponentType<{ size?: number }>;
  /** Custom active predicate — defaults to path.startsWith(href) */
  isActive?: (path: string) => boolean;
  /** If true, this nav item renders the unread-alerts badge */
  showsBadge?: boolean;
};

const nav: NavItem[] = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  {
    href: "/providers",
    label: "Providers",
    icon: ShieldAlert,
    // Active for /providers and /providers/[npi]/... but NOT for /providers/map
    isActive: (p) => p.startsWith("/providers") && p !== "/providers/map",
  },
  {
    href: "/providers/map",
    label: "Fraud Map",
    icon: Map,
    isActive: (p) => p === "/providers/map",
  },
  { href: "/cases", label: "Cases", icon: FolderOpen },
  { href: "/network", label: "Network", icon: Network },
  { href: "/alerts", label: "Alerts", icon: Bell, showsBadge: true },
];

const adminNav: NavItem[] = [
  { href: "/admin/users", label: "Users", icon: Users },
  { href: "/admin", label: "Settings", icon: Settings },
];

interface SidebarProps {
  /** Number of unread real-time alerts — drives the bell badge */
  unreadCount?: number;
  /** Called when the user clicks the Alerts link to clear the badge */
  onClearAlerts?: () => void;
}

export default function Sidebar({ unreadCount = 0, onClearAlerts }: SidebarProps) {
  const path = usePathname();
  const { user, logout } = useAuth();

  const isActive = (item: NavItem) =>
    item.isActive ? item.isActive(path) : path.startsWith(item.href);

  return (
    <aside className="w-56 shrink-0 flex flex-col border-r border-white/[0.06] bg-white/[0.015] min-h-screen">
      {/* Logo */}
      <div className="flex items-center gap-2.5 px-5 py-5 border-b border-white/[0.06]">
        <div className="w-7 h-7 rounded-md bg-gradient-to-br from-red-500 to-orange-400 flex items-center justify-center text-[#080c16] font-black text-sm">
          V
        </div>
        <span className="text-sm font-bold tracking-widest text-slate-100">VIGIL</span>
        <span className="ml-auto">
          <span className="w-1.5 h-1.5 rounded-full bg-green-400 inline-block animate-pulse" />
        </span>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-0.5">
        {nav.map((item) => {
          const { href, label, icon: Icon, showsBadge } = item;
          const active = isActive(item);
          const badge = showsBadge && unreadCount > 0;

          return (
            <Link
              key={href}
              href={href}
              onClick={showsBadge && onClearAlerts ? onClearAlerts : undefined}
              className={cn(
                "flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition",
                active
                  ? "bg-white/[0.07] text-slate-100 font-medium"
                  : "text-slate-500 hover:text-slate-300 hover:bg-white/[0.04]"
              )}
            >
              <Icon size={15} />
              <span className="flex-1">{label}</span>
              {badge && (
                <span className="min-w-[18px] h-[18px] rounded-full bg-red-500 flex items-center justify-center text-[10px] font-bold text-white px-1 leading-none animate-pulse">
                  {unreadCount > 99 ? "99+" : unreadCount}
                </span>
              )}
            </Link>
          );
        })}

        {user?.role === "admin" && (
          <>
            <div className="pt-4 pb-1 px-3">
              <span className="text-[10px] uppercase tracking-widest text-slate-600 font-medium">Admin</span>
            </div>
            {adminNav.map((item) => {
              const { href, label, icon: Icon } = item;
              const active = isActive(item);
              return (
                <Link
                  key={href}
                  href={href}
                  className={cn(
                    "flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition",
                    active
                      ? "bg-white/[0.07] text-slate-100 font-medium"
                      : "text-slate-500 hover:text-slate-300 hover:bg-white/[0.04]"
                  )}
                >
                  <Icon size={15} />
                  {label}
                </Link>
              );
            })}
          </>
        )}
      </nav>

      {/* User */}
      <div className="border-t border-white/[0.06] px-4 py-3">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-full bg-gradient-to-br from-slate-600 to-slate-700 flex items-center justify-center text-xs font-bold text-slate-300 shrink-0">
            {user?.name?.[0] ?? "?"}
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-xs font-medium text-slate-300 truncate">{user?.name}</p>
            <p className="text-[10px] text-slate-600 uppercase tracking-wider">{user?.role}</p>
          </div>
          <button onClick={logout} className="text-slate-600 hover:text-slate-300 transition" title="Sign out">
            <LogOut size={13} />
          </button>
        </div>
      </div>
    </aside>
  );
}
