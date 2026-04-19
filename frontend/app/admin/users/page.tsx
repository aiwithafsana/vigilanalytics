"use client";
import { useEffect, useState } from "react";
import AppShell from "@/components/AppShell";
import Link from "next/link";
import { getUsers, createUser } from "@/lib/api";
import type { User, Role } from "@/types";
import { ArrowLeft, UserPlus, X } from "lucide-react";

const ROLES: Role[] = ["admin", "analyst", "viewer"];
const ALL_STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
  "VA","WA","WV","WI","WY",
];

const ROLE_BADGE: Record<Role, string> = {
  admin: "bg-red-500/10 text-red-400 border border-red-500/20",
  analyst: "bg-blue-500/10 text-blue-400 border border-blue-500/20",
  viewer: "bg-slate-700 text-slate-400",
};

export default function UsersPage() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Form state
  const [form, setForm] = useState({
    email: "",
    password: "",
    name: "",
    role: "analyst" as Role,
    state_access: [] as string[],
  });

  useEffect(() => {
    getUsers()
      .then(setUsers)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  function toggleState(s: string) {
    setForm((f) => ({
      ...f,
      state_access: f.state_access.includes(s)
        ? f.state_access.filter((x) => x !== s)
        : [...f.state_access, s],
    }));
  }

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    try {
      const created = await createUser(form);
      setUsers((u) => [...u, created]);
      setShowModal(false);
      setForm({ email: "", password: "", name: "", role: "analyst", state_access: [] });
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to create user");
    } finally {
      setSaving(false);
    }
  }

  return (
    <AppShell>
      <div className="p-8 max-w-5xl mx-auto fade-in">
        <Link
          href="/admin"
          className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-300 mb-6 transition w-fit"
        >
          <ArrowLeft size={14} /> Administration
        </Link>

        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-xl font-bold text-slate-100">User Management</h1>
            <p className="text-sm text-slate-500 mt-0.5">
              {users.length} account{users.length !== 1 ? "s" : ""}
            </p>
          </div>
          <button
            onClick={() => setShowModal(true)}
            className="flex items-center gap-2 bg-blue-500/10 hover:bg-blue-500/20 border border-blue-500/20 text-blue-400 px-4 py-2 rounded-lg text-sm transition"
          >
            <UserPlus size={14} /> Create User
          </button>
        </div>

        {/* Table */}
        <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/[0.06]">
                {["Name", "Email", "Role", "State Access", "Last Login", "Status"].map((h) => (
                  <th
                    key={h}
                    className="text-left text-[10px] uppercase tracking-widest text-slate-600 font-medium px-4 py-3"
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} className="px-4 py-12 text-center text-slate-600 text-sm animate-pulse">
                    Loading…
                  </td>
                </tr>
              ) : users.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-4 py-12 text-center text-slate-600 text-sm">
                    No users found.
                  </td>
                </tr>
              ) : (
                users.map((u) => (
                  <tr key={u.id} className="border-b border-white/[0.03] hover:bg-white/[0.03] transition">
                    <td className="px-4 py-3 text-slate-200 font-medium">{u.name}</td>
                    <td className="px-4 py-3 text-slate-400 font-mono text-xs">{u.email}</td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded font-mono ${ROLE_BADGE[u.role]}`}>
                        {u.role}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-slate-500 text-xs">
                      {u.state_access?.length
                        ? u.state_access.join(", ")
                        : <span className="text-slate-600 italic">All states</span>}
                    </td>
                    <td className="px-4 py-3 text-slate-500 text-xs font-mono">
                      {u.last_login
                        ? new Date(u.last_login).toLocaleDateString()
                        : <span className="text-slate-700">Never</span>}
                    </td>
                    <td className="px-4 py-3">
                      <span
                        className={`text-xs px-2 py-0.5 rounded font-mono ${
                          u.is_active
                            ? "bg-green-500/10 text-green-400 border border-green-500/20"
                            : "bg-slate-700 text-slate-500"
                        }`}
                      >
                        {u.is_active ? "Active" : "Inactive"}
                      </span>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Create User Modal */}
        {showModal && (
          <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
            <div className="bg-[#0f1623] border border-white/[0.1] rounded-2xl w-full max-w-lg shadow-2xl">
              <div className="flex items-center justify-between px-6 py-4 border-b border-white/[0.06]">
                <h2 className="text-slate-100 font-semibold">Create New User</h2>
                <button
                  onClick={() => { setShowModal(false); setError(null); }}
                  className="text-slate-500 hover:text-slate-300 transition"
                >
                  <X size={18} />
                </button>
              </div>

              <form onSubmit={handleCreate} className="px-6 py-5 space-y-4">
                {error && (
                  <div className="bg-red-500/10 border border-red-500/20 text-red-400 text-sm rounded-lg px-4 py-3">
                    {error}
                  </div>
                )}

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="text-xs text-slate-500 mb-1 block">Full Name</label>
                    <input
                      required
                      value={form.name}
                      onChange={(e) => setForm({ ...form, name: e.target.value })}
                      className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500/50"
                    />
                  </div>
                  <div>
                    <label className="text-xs text-slate-500 mb-1 block">Email</label>
                    <input
                      required
                      type="email"
                      value={form.email}
                      onChange={(e) => setForm({ ...form, email: e.target.value })}
                      className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500/50"
                    />
                  </div>
                </div>

                <div>
                  <label className="text-xs text-slate-500 mb-1 block">
                    Password <span className="text-slate-600">(min 12 chars, upper, lower, digit, special)</span>
                  </label>
                  <input
                    required
                    type="password"
                    value={form.password}
                    onChange={(e) => setForm({ ...form, password: e.target.value })}
                    className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500/50"
                  />
                </div>

                <div>
                  <label className="text-xs text-slate-500 mb-1 block">Role</label>
                  <select
                    value={form.role}
                    onChange={(e) => setForm({ ...form, role: e.target.value as Role })}
                    className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none"
                  >
                    {ROLES.map((r) => (
                      <option key={r} value={r} className="bg-[#0f1623]">
                        {r.charAt(0).toUpperCase() + r.slice(1)}
                      </option>
                    ))}
                  </select>
                  <p className="text-[10px] text-slate-600 mt-1">
                    Admin: full access · Analyst: create/edit cases · Viewer: read-only
                  </p>
                </div>

                <div>
                  <label className="text-xs text-slate-500 mb-2 block">
                    State Access{" "}
                    <span className="text-slate-600">(leave empty for all states)</span>
                  </label>
                  <div className="flex flex-wrap gap-1.5 max-h-32 overflow-y-auto p-1">
                    {ALL_STATES.map((s) => (
                      <button
                        key={s}
                        type="button"
                        onClick={() => toggleState(s)}
                        className={`text-xs px-2 py-0.5 rounded font-mono transition border ${
                          form.state_access.includes(s)
                            ? "bg-blue-500/20 text-blue-300 border-blue-500/40"
                            : "bg-white/[0.03] text-slate-500 border-white/[0.07] hover:border-white/[0.15]"
                        }`}
                      >
                        {s}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="flex gap-3 pt-2">
                  <button
                    type="button"
                    onClick={() => { setShowModal(false); setError(null); }}
                    className="flex-1 border border-white/[0.08] text-slate-400 hover:text-slate-200 px-4 py-2 rounded-lg text-sm transition"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={saving}
                    className="flex-1 bg-blue-500/20 hover:bg-blue-500/30 border border-blue-500/30 text-blue-300 px-4 py-2 rounded-lg text-sm transition disabled:opacity-50"
                  >
                    {saving ? "Creating…" : "Create User"}
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}
      </div>
    </AppShell>
  );
}
