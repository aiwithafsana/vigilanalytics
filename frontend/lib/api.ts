import type {
  CaseListResponse,
  CaseNote,
  DashboardResponse,
  ProviderDetail,
  ProviderListResponse,
  User,
} from "@/types";

const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem("vigil_token");
}

// Prevent multiple simultaneous refresh calls
let _refreshPromise: Promise<string | null> | null = null;

async function _attemptRefresh(): Promise<string | null> {
  if (_refreshPromise) return _refreshPromise;
  _refreshPromise = (async () => {
    const refreshToken = localStorage.getItem("vigil_refresh_token");
    if (!refreshToken) return null;
    try {
      const res = await fetch(`${BASE}/api/users/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: refreshToken }),
      });
      if (!res.ok) return null;
      const data = await res.json();
      localStorage.setItem("vigil_token", data.access_token);
      localStorage.setItem("vigil_refresh_token", data.refresh_token);
      return data.access_token as string;
    } catch {
      return null;
    } finally {
      _refreshPromise = null;
    }
  })();
  return _refreshPromise;
}

function _clearAuth() {
  localStorage.removeItem("vigil_token");
  localStorage.removeItem("vigil_refresh_token");
  window.location.href = "/login";
}

async function request<T>(path: string, options: RequestInit = {}, _retry = true): Promise<T> {
  const token = getToken();
  const res = await fetch(`${BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...options.headers,
    },
  });

  if (res.status === 401) {
    if (_retry) {
      // Try to silently refresh the access token, then retry once
      const newToken = await _attemptRefresh();
      if (newToken) return request<T>(path, options, false);
    }
    _clearAuth();
    throw new Error("Not authenticated");
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail ?? "Request failed");
  }

  return res.json();
}

// ── Auth ──────────────────────────────────────────────────────────────────────

export async function login(email: string, password: string) {
  const data = await request<{ access_token: string; refresh_token: string }>(
    "/api/users/login",
    { method: "POST", body: JSON.stringify({ email, password }) }
  );
  localStorage.setItem("vigil_token", data.access_token);
  localStorage.setItem("vigil_refresh_token", data.refresh_token);
  return data;
}

export function logout() {
  localStorage.removeItem("vigil_token");
  localStorage.removeItem("vigil_refresh_token");
  window.location.href = "/login";
}

export const getMe = () => request<User>("/api/users/me");

// ── Dashboard ─────────────────────────────────────────────────────────────────

export const getDashboard = () => request<DashboardResponse>("/api/dashboard");

// ── Providers ─────────────────────────────────────────────────────────────────

export function getProviders(params: Record<string, string | number | boolean | undefined> = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") qs.set(k, String(v));
  }
  return request<ProviderListResponse>(`/api/providers?${qs}`);
}

export const getProvider = (npi: string) =>
  request<ProviderDetail>(`/api/providers/${npi}`);

export const getProviderPdfUrl = (npi: string) =>
  `${BASE}/api/providers/${npi}/report/pdf`;

export const getProvidersCsvUrl = (params: Record<string, string> = {}) => {
  const qs = new URLSearchParams(params);
  return `${BASE}/api/providers/export/csv?${qs}`;
};

// ── Cases ─────────────────────────────────────────────────────────────────────

export function getCases(params: Record<string, string | number | boolean | undefined> = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") qs.set(k, String(v));
  }
  return request<CaseListResponse>(`/api/cases?${qs}`);
}

export const getCase = (id: number) => request<import("@/types").Case>(`/api/cases/${id}`);

export function createCase(body: {
  provider_npi: string;
  title: string;
  state?: string;
  estimated_loss?: number;
  notes?: string;
}) {
  return request<import("@/types").Case>("/api/cases", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function updateCase(
  id: number,
  body: Partial<{ title: string; status: string; notes: string; estimated_loss: number }>
) {
  return request<import("@/types").Case>(`/api/cases/${id}`, {
    method: "PATCH",
    body: JSON.stringify(body),
  });
}

export function addCaseNote(caseId: number, content: string) {
  return request<CaseNote>(`/api/cases/${caseId}/notes`, {
    method: "POST",
    body: JSON.stringify({ content }),
  });
}

// ── Network ───────────────────────────────────────────────────────────────────

export const getProviderNetwork = (npi: string) =>
  request<import("@/types").NetworkGraph>(`/api/network/${npi}`);

export const getProviderNetwork2Hop = (npi: string, maxNodes = 100) =>
  request<import("@/types").NetworkGraph>(`/api/network/${npi}/2hop?max_nodes=${maxNodes}`);

export const searchNetworkProviders = (q: string) =>
  request<import("@/types").NetworkNode[]>(`/api/network/search?q=${encodeURIComponent(q)}`);

// ── Alerts ────────────────────────────────────────────────────────────────────

export const getAlerts = (severity?: number) => {
  const qs = severity != null ? `?severity=${severity}` : "";
  return request<import("@/types").AlertResponse>(`/api/alerts${qs}`);
};

// ── Provider map ──────────────────────────────────────────────────────────────

export const getProviderMap = (state?: string) => {
  const qs = state ? `?state=${encodeURIComponent(state)}` : "";
  return request<import("@/types").ProviderMapPoint[]>(`/api/providers/map${qs}`);
};

// ── Provider billing breakdown ────────────────────────────────────────────────

export const getProviderBilling = (npi: string, year?: number) => {
  const qs = year != null ? `?year=${year}` : "";
  return request<import("@/types").BillingRecord[]>(`/api/providers/${npi}/billing${qs}`);
};

// ── Provider fraud flags (normalized) ────────────────────────────────────────

export const getProviderFlags = (npi: string) =>
  request<import("@/types").FraudFlag[]>(`/api/providers/${npi}/flags`);

// ── AI Investigative Brief ────────────────────────────────────────────────────

export const getProviderAnalysis = (npi: string) =>
  request<import("@/types").ProviderAnalysis>(`/api/providers/${npi}/analysis`);

// ── Users ─────────────────────────────────────────────────────────────────────

export const getUsers = () => request<User[]>("/api/users");

export function createUser(body: {
  email: string;
  password: string;
  name: string;
  role: string;
  state_access: string[];
}) {
  return request<User>("/api/users", { method: "POST", body: JSON.stringify(body) });
}
