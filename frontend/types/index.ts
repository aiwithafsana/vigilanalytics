export type Role = "admin" | "analyst" | "viewer";

export interface User {
  id: string;
  email: string;
  name: string;
  role: Role;
  state_access: string[];
  is_active: boolean;
  created_at: string;
  last_login: string | null;
}

export interface Flag {
  type: string;
  severity: "critical" | "high" | "medium" | "low";
  text: string;
}

export interface ProviderSummary {
  npi: string;
  name_last: string | null;
  name_first: string | null;
  specialty: string | null;
  state: string | null;
  city: string | null;
  total_payment: number | null;
  risk_score: number | null;
  is_excluded: boolean;
  flags: Flag[];
  flag_count: number | null;
  data_year: number | null;
}

export interface ProviderDetail extends ProviderSummary {
  total_services: number | null;
  total_beneficiaries: number | null;
  num_procedure_types: number | null;
  peer_median_payment: number | null;
  peer_median_services: number | null;
  peer_median_benes: number | null;
  payment_vs_peer: number | null;
  services_vs_peer: number | null;
  benes_vs_peer: number | null;
  payment_zscore: number | null;
  services_per_bene: number | null;
  payment_per_bene: number | null;
  billing_entropy: number | null;
  em_upcoding_ratio: number | null;
  xgboost_score: number | null;
  isolation_score: number | null;
  autoencoder_score: number | null;
  leie_date: string | null;
  leie_reason: string | null;
  scored_at: string | null;
}

export interface ProviderListResponse {
  items: ProviderSummary[];
  total: number;
  page: number;
  page_size: number;
}

export interface CaseNote {
  id: number;
  case_id: number;
  user_id: string;
  content: string;
  created_at: string;
  user_name: string | null;
}

export interface CaseDocument {
  id: number;
  case_id: number;
  filename: string;
  file_size: number | null;
  uploaded_by: string;
  created_at: string;
}

export interface Case {
  id: number;
  case_number: string;
  provider_npi: string;
  title: string;
  status: "open" | "under_review" | "closed" | "referred";
  assigned_to: string | null;
  state: string | null;
  estimated_loss: number | null;
  notes: string | null;
  created_by: string;
  created_at: string;
  updated_at: string;
  provider: ProviderSummary | null;
  case_notes: CaseNote[];
  documents: CaseDocument[];
}

export interface CaseListResponse {
  items: Case[];
  total: number;
  page: number;
  page_size: number;
}

export interface DashboardStats {
  total_providers: number;
  total_payment: number;
  leie_matches: number;
  open_cases: number;
  high_risk_providers: number;
  states_covered: number;
  new_leads: number;
}

export interface RiskDistribution {
  critical: number;
  high: number;
  medium: number;
  low: number;
}

export interface LeadItem {
  flag_id: number;
  npi: string;
  name: string;
  specialty: string | null;
  state: string | null;
  city: string | null;
  is_excluded: boolean;
  severity: number;       // 1=critical, 2=high, 3=medium
  flag_type: string;
  explanation: string | null;
  estimated_overpayment: number | null;
  flag_value: number | null;
  peer_value: number | null;
  hcpcs_code: string | null;
  total_payment: number | null;
  risk_score: number | null;
}

export interface DashboardResponse {
  stats: DashboardStats;
  risk_distribution: RiskDistribution;
  top_providers: ProviderSummary[];
  recent_cases: Case[];
  top_leads: LeadItem[];
}

// ── Fraud Flags (normalized) ──────────────────────────────────────────────────

export interface FraudFlag {
  id: number;
  npi: string;
  flag_type: string;
  layer: number | null;
  severity: number | null;    // 1=critical, 2=high, 3=medium
  confidence: number | null;
  year: number | null;
  flag_value: number | null;
  peer_value: number | null;
  explanation: string | null;
  estimated_overpayment: number | null;
  hcpcs_code: string | null;
  is_active: boolean;
  created_at: string;
}

// ── Billing Records ───────────────────────────────────────────────────────────

export interface BillingRecord {
  id: number;
  npi: string;
  year: number;
  hcpcs_code: string | null;
  hcpcs_description: string | null;
  place_of_service: string | null;
  total_beneficiaries: number | null;
  total_services: number | null;
  total_claims: number | null;
  avg_submitted_charge: number | null;
  avg_medicare_allowed: number | null;
  avg_medicare_payment: number | null;
  total_medicare_payment: number | null;
}

// ── Map / Geographic ──────────────────────────────────────────────────────────

export interface ProviderMapPoint {
  state: string;
  total_providers: number;
  high_risk_count: number;
  avg_risk_score: number | null;
  total_estimated_loss: number | null;
  excluded_count: number;
}

// ── Alerts ────────────────────────────────────────────────────────────────────

export interface AlertItem {
  flag_id: number;
  npi: string;
  provider_name: string | null;
  specialty: string | null;
  state: string | null;
  risk_score: number | null;
  flag_type: string;
  severity: number;           // 1=critical, 2=high, 3=medium
  explanation: string | null;
  estimated_overpayment: number | null;
  created_at: string;
}

export interface AlertResponse {
  items: AlertItem[];
  total: number;
  since: string | null;
}

// ── Network graph ─────────────────────────────────────────────────────────────

export interface NetworkNode {
  npi: string;
  name: string;
  specialty: string;
  state: string;
  risk_score: number;
  is_excluded: boolean;
  total_payment: number;
  flag_count: number;
  is_center: boolean;
}

export interface NetworkEdge {
  id: number;
  source: string;
  target: string;
  referral_count: number;
  shared_patients: number;
  total_payment: number;
  referral_percentage: number;
  is_suspicious: boolean;
}

export interface NetworkStats {
  total_nodes: number;
  total_edges: number;
  suspicious_edges: number;
  hop1_count?: number;
  hop2_count?: number;
}

export interface NetworkGraph {
  center_npi: string;
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  stats: NetworkStats;
}

// ── AI Investigative Brief ─────────────────────────────────────────────────────

export interface AnalysisKeyFinding {
  label: string;
  value: string;
  detail: string;
}

export interface AnalysisBillingAnomaly {
  hcpcs: string | null;
  description: string | null;
  total_paid: number;
  services: number;
  beneficiaries: number;
  services_per_bene: number;
  anomaly_text: string;
}

export interface AnalysisNetworkSuspect {
  npi: string;
  name: string;
  specialty: string | null;
  state: string | null;
  risk_score: number;
  is_excluded: boolean;
  shared_patients: number;
  direction: string;
  reason: string;
  action: string;
}

export interface AnalysisAction {
  step: number;
  category: string;
  action: string;
  detail: string;
}

export interface ProviderAnalysis {
  npi: string;
  provider_name: string;
  specialty: string | null;
  state: string | null;
  risk_score: number;
  priority: number;
  priority_label: string;
  scheme_type: string;
  narrative: string;
  key_findings: AnalysisKeyFinding[];
  billing_anomalies: AnalysisBillingAnomaly[];
  network_suspects: AnalysisNetworkSuspect[];
  recommended_actions: AnalysisAction[];
  estimated_exposure: number | null;
  active_signals: number;
  suspicious_edges: number;
  generated_at: string;
  data_source: string;
}
