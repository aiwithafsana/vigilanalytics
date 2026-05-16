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

export interface MfaChallenge {
  mfa_required: true;
  mfa_token: string;
  expires_in: number;
}

export interface TokenPair {
  access_token: string;
  refresh_token: string;
  token_type?: string;
}

export type LoginResult =
  | { kind: "tokens"; tokens: TokenPair }
  | { kind: "mfa_required"; challenge: MfaChallenge };

/**
 * Two-step login.  If the user has MFA enabled, the server returns an
 * `mfa_required` challenge instead of tokens; the caller must follow up
 * with `loginMfa(...)` after collecting the user's TOTP / backup code.
 */
export async function login(email: string, password: string): Promise<LoginResult> {
  // The /login endpoint returns either TokenPair OR MfaChallenge — both are
  // 200 OK so we discriminate on the body shape.
  const data = await request<TokenPair | MfaChallenge>(
    "/api/users/login",
    { method: "POST", body: JSON.stringify({ email, password }) }
  );

  if ("mfa_required" in data && data.mfa_required) {
    return { kind: "mfa_required", challenge: data };
  }
  const tokens = data as TokenPair;
  localStorage.setItem("vigil_token", tokens.access_token);
  localStorage.setItem("vigil_refresh_token", tokens.refresh_token);
  return { kind: "tokens", tokens };
}

/**
 * Step 2 of login — verify TOTP or backup code.  On success, persists the
 * tokens to localStorage just like a no-MFA login would.
 */
export async function loginMfa(mfa_token: string, code: string): Promise<TokenPair> {
  const tokens = await request<TokenPair>("/api/users/login/mfa", {
    method: "POST",
    body: JSON.stringify({ mfa_token, code }),
  });
  localStorage.setItem("vigil_token", tokens.access_token);
  localStorage.setItem("vigil_refresh_token", tokens.refresh_token);
  return tokens;
}

// ── MFA enrollment ────────────────────────────────────────────────────────────
export interface MfaSetup {
  secret: string;
  provisioning_uri: string;
  issuer: string;
}

export interface MfaActivateResult {
  mfa_enabled: boolean;
  backup_codes: string[];
}

export const mfaSetup = () =>
  request<MfaSetup>("/api/users/mfa/setup", { method: "POST" });

export const mfaActivate = (code: string) =>
  request<MfaActivateResult>("/api/users/mfa/activate", {
    method: "POST",
    body: JSON.stringify({ code }),
  });

export const mfaDisable = (code: string) =>
  request<void>("/api/users/mfa/disable", {
    method: "POST",
    body: JSON.stringify({ code }),
  });

export const mfaRegenerateBackupCodes = (code: string) =>
  request<MfaActivateResult>("/api/users/mfa/regenerate-backup-codes", {
    method: "POST",
    body: JSON.stringify({ code }),
  });

// ── Audit timeline (per-target chain of custody) ──────────────────────────────
export interface AuditLogItem {
  id:           number;
  user_id:      string | null;
  user_name:    string | null;
  action:       string;
  target_type:  string | null;
  target_id:    string | null;
  details:      Record<string, unknown>;
  ip_address:   string | null;
  created_at:   string;
}

export interface AuditTimeline {
  items:     AuditLogItem[];
  total:     number;
  page:      number;
  page_size: number;
}

export const getAuditTimeline = (
  target_type: "provider" | "case" | "user",
  target_id: string,
  limit = 50,
) => {
  const qs = new URLSearchParams({ target_type, target_id, limit: String(limit) });
  return request<AuditTimeline>(`/api/audit/timeline?${qs}`);
};

// ── Agent runs ────────────────────────────────────────────────────────────────
// Agentic workflows (Public Records aggregator, etc.) — see backend/app/agents/

export type AgentSeverity = "critical" | "high" | "medium" | "low" | "info";
export type AgentRunStatus = "running" | "succeeded" | "partial" | "failed";

export interface AgentFinding {
  source:   string;
  severity: AgentSeverity;
  title:    string;
  summary:  string;
  url:      string | null;
  date:     string | null;
}

export interface AgentToolResult {
  tool_name:   string;
  success:     boolean;
  n_findings:  number;
  duration_ms: number;
  error:       string | null;
}

export interface AgentRunResult {
  workflow:           string;
  target_type:        string;
  target_id:          string;
  started_at:         string;
  completed_at:       string;
  duration_ms:        number;
  success:            boolean;
  n_tools_run:        number;
  n_tools_succeeded:  number;
  n_findings:         number;
  max_severity:       AgentSeverity;
  tool_results:       AgentToolResult[];
  findings:           AgentFinding[];
}

export interface AgentRun {
  id:                       number;
  workflow:                 string;
  target_type:              string;
  target_id:                string;
  status:                   AgentRunStatus;
  started_at:               string | null;
  completed_at:             string | null;
  duration_ms:              number | null;
  n_findings:               number | null;
  max_severity:             AgentSeverity | null;
  triggered_by_user_id:     string | null;
  result:                   AgentRunResult | null;
  error:                    string | null;
}

export const triggerAgentRun = (workflow: string, npi: string) =>
  request<{ agent_run_id: number; workflow: string; npi: string; status: string; poll_url: string }>(
    `/api/agents/${workflow}/run?npi=${encodeURIComponent(npi)}`,
    { method: "POST" },
  );

export const getAgentRun = (runId: number) =>
  request<AgentRun>(`/api/agents/runs/${runId}`);

export const listAgentRuns = (target_id: string, target_type = "provider", limit = 20) =>
  request<AgentRun[]>(
    `/api/agents/runs?target_type=${target_type}&target_id=${encodeURIComponent(target_id)}&limit=${limit}`,
  );

export function logout() {
  localStorage.removeItem("vigil_token");
  localStorage.removeItem("vigil_refresh_token");
  window.location.href = "/login";
}

export const getMe = () => request<User>("/api/users/me");

// ── System / data vintage ─────────────────────────────────────────────────────
// Surfaces how fresh the underlying scoring + LEIE data is.  Required for
// legal-defensibility — every score shown in the UI must be accompanied by
// the data-as-of date so investigators / attorneys know what they're seeing.
export interface DataVintage {
  model_version:            string;
  scoring_data_year:        number;
  scoring_data_through:     string;            // YYYY-12-31
  providers_last_scored_at: string | null;
  leie_last_refreshed_at:   string | null;
  leie_active_count:        number;
  as_of:                    string;
}

export const getDataVintage = () => request<DataVintage>("/api/system/data-vintage");

export const triggerLeieRefresh = () =>
  request<{ status: string; message: string; triggered_at: string; triggered_by: string }>(
    "/api/system/leie-refresh",
    { method: "POST" },
  );

// ── Attestations ──────────────────────────────────────────────────────────────
// Recorded before sensitive actions (PDF export, marking case substantiated, etc.)
// to create an audit trail that the user reviewed the methodology limitations.
export type AttestationAction =
  | "pdf_export"
  | "csv_export"
  | "case_outcome_substantiated"
  | "case_referral";

export interface AttestationResponse {
  attestation_id: number;
  for_action:     AttestationAction;
  attested_at:    string;
  expires_in:     number;     // seconds
}

export const recordAttestation = (body: {
  action:      AttestationAction;
  target_id?:  string;
  target_type?: "provider" | "case";
  methodology_version?: string;
}) =>
  request<AttestationResponse>("/api/audit/attestation", {
    method: "POST",
    body: JSON.stringify({ methodology_version: "2.1.0", ...body }),
  });

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

export interface ProviderActiveCase {
  id:                 number;
  case_number:        string;
  title:              string;
  status:             "open" | "under_review";
  state:              string | null;
  created_at:         string | null;
  assigned_to_name:   string | null;
  created_by_name:    string | null;
}

export const getProviderActiveCases = (npi: string) =>
  request<ProviderActiveCase[]>(`/api/providers/${npi}/active-cases`);

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

export function recordOutcome(
  caseId: number,
  outcome: import("@/types").CaseOutcome,
  outcome_note?: string
) {
  return request<import("@/types").Case>(`/api/cases/${caseId}/outcome`, {
    method: "PATCH",
    body: JSON.stringify({ outcome, outcome_note }),
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
