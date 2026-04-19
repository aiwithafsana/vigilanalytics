"use client";
import AppShell from "@/components/AppShell";
import Link from "next/link";
import { Users, Shield, Database, Activity } from "lucide-react";

const cards = [
  {
    href: "/admin/users",
    icon: Users,
    title: "User Management",
    desc: "Create, edit, and deactivate investigator accounts. Assign roles and state-level access.",
    color: "text-blue-400",
    bg: "bg-blue-500/10 border-blue-500/20",
  },
  {
    href: "/audit",
    icon: Activity,
    title: "Audit Log",
    desc: "Review every action taken in the system — logins, exports, case changes, and more.",
    color: "text-green-400",
    bg: "bg-green-500/10 border-green-500/20",
  },
  {
    href: "#",
    icon: Database,
    title: "Data Pipeline",
    desc: "Trigger ML pipeline runs, view last scored date, and monitor provider ingestion status.",
    color: "text-purple-400",
    bg: "bg-purple-500/10 border-purple-500/20",
    disabled: true,
  },
  {
    href: "#",
    icon: Shield,
    title: "Security Settings",
    desc: "Configure password policy, session timeouts, IP allowlists, and 2FA requirements.",
    color: "text-orange-400",
    bg: "bg-orange-500/10 border-orange-500/20",
    disabled: true,
  },
];

export default function AdminPage() {
  return (
    <AppShell>
      <div className="p-8 max-w-4xl mx-auto fade-in">
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-slate-100">Administration</h1>
          <p className="text-sm text-slate-500 mt-1">System configuration and management tools</p>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {cards.map(({ href, icon: Icon, title, desc, color, bg, disabled }) => {
            const inner = (
              <div
                className={`border rounded-xl p-6 transition ${bg} ${
                  disabled
                    ? "opacity-40 cursor-not-allowed"
                    : "hover:brightness-125 cursor-pointer"
                }`}
              >
                <Icon size={24} className={`${color} mb-3`} />
                <h2 className="text-slate-100 font-semibold mb-1">{title}</h2>
                <p className="text-slate-500 text-sm leading-relaxed">{desc}</p>
                {disabled && (
                  <span className="mt-3 inline-block text-[10px] uppercase tracking-widest text-slate-600 font-mono">
                    Coming Soon
                  </span>
                )}
              </div>
            );
            return disabled ? (
              <div key={title}>{inner}</div>
            ) : (
              <Link key={title} href={href}>{inner}</Link>
            );
          })}
        </div>
      </div>
    </AppShell>
  );
}
