-- =============================================================================
-- Vigil Architecture v2 Migration
-- Apply to an existing database that has the v1 schema (providers, cases, etc.)
-- Safe to run multiple times — all statements use IF NOT EXISTS / IF EXISTS guards.
-- =============================================================================

-- ── 1. Add new columns to providers ──────────────────────────────────────────

ALTER TABLE providers
  ADD COLUMN IF NOT EXISTS entity_type       CHAR(1),
  ADD COLUMN IF NOT EXISTS taxonomy_code     VARCHAR(10),
  ADD COLUMN IF NOT EXISTS credential        VARCHAR(20),
  ADD COLUMN IF NOT EXISTS enrollment_date   DATE,
  ADD COLUMN IF NOT EXISTS is_opt_out        BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS risk_tier         SMALLINT,
  ADD COLUMN IF NOT EXISTS flag_count        SMALLINT DEFAULT 0;

-- Backfill risk_tier from existing risk_score
UPDATE providers SET risk_tier =
  CASE
    WHEN risk_score >= 90 THEN 1
    WHEN risk_score >= 70 THEN 2
    WHEN risk_score >= 50 THEN 3
    ELSE 4
  END
WHERE risk_tier IS NULL AND risk_score IS NOT NULL;

-- Backfill flag_count from JSONB flags array
UPDATE providers
  SET flag_count = jsonb_array_length(flags)
WHERE flag_count = 0 AND flags IS NOT NULL AND jsonb_typeof(flags) = 'array';

-- ── 2. Create fraud_flags table ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fraud_flags (
  id                    BIGSERIAL PRIMARY KEY,
  npi                   VARCHAR(10) REFERENCES providers(npi) ON DELETE CASCADE,
  flag_type             VARCHAR(30) NOT NULL,
  layer                 SMALLINT,
  severity              SMALLINT,          -- 1=critical, 2=high, 3=medium
  confidence            NUMERIC(4,3),      -- 0.000–1.000
  year                  SMALLINT,
  flag_value            NUMERIC,
  peer_value            NUMERIC,
  explanation           TEXT,
  estimated_overpayment NUMERIC(14,2),
  hcpcs_code            VARCHAR(10),
  is_active             BOOLEAN DEFAULT TRUE,
  reviewed_by           UUID,
  reviewed_at           TIMESTAMP WITH TIME ZONE,
  created_at            TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_fraud_flags_npi_active    ON fraud_flags(npi, is_active);
CREATE INDEX IF NOT EXISTS ix_fraud_flags_sev_created   ON fraud_flags(severity, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_fraud_flags_created_at    ON fraud_flags(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_fraud_flags_flag_type     ON fraud_flags(flag_type);

-- ── 3. Create billing_records table ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS billing_records (
  id                      BIGSERIAL PRIMARY KEY,
  npi                     VARCHAR(10) REFERENCES providers(npi) ON DELETE CASCADE,
  year                    SMALLINT NOT NULL,
  hcpcs_code              VARCHAR(10),
  hcpcs_description       TEXT,
  place_of_service        VARCHAR(2),
  total_beneficiaries     INTEGER,
  total_services          INTEGER,
  total_claims            INTEGER,
  avg_submitted_charge    NUMERIC(12,2),
  avg_medicare_allowed    NUMERIC(12,2),
  avg_medicare_payment    NUMERIC(12,2),
  total_medicare_payment  NUMERIC(14,2)
);

CREATE INDEX IF NOT EXISTS ix_billing_records_npi_year  ON billing_records(npi, year);
CREATE INDEX IF NOT EXISTS ix_billing_records_year      ON billing_records(year);
CREATE INDEX IF NOT EXISTS ix_billing_records_hcpcs     ON billing_records(hcpcs_code);

-- ── 4. Create peer_benchmarks table ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS peer_benchmarks (
  id                        BIGSERIAL PRIMARY KEY,
  year                      SMALLINT NOT NULL,
  taxonomy_code             VARCHAR(10),
  state                     CHAR(2),
  hcpcs_code                VARCHAR(10),   -- NULL = provider-level
  peer_count                INTEGER,
  median_total_payment      NUMERIC(14,2),
  p90_total_payment         NUMERIC(14,2),
  p99_total_payment         NUMERIC(14,2),
  median_services_per_ben   NUMERIC(8,2),
  median_charge_per_service NUMERIC(10,2),
  UNIQUE (year, taxonomy_code, state, hcpcs_code)
);

CREATE INDEX IF NOT EXISTS ix_peer_benchmarks_lookup ON peer_benchmarks(taxonomy_code, state, year, hcpcs_code);

-- ── 5. Add full-text search vector to providers (async-safe) ─────────────────
-- Run CONCURRENTLY outside a transaction block if the table is large.

ALTER TABLE providers ADD COLUMN IF NOT EXISTS search_vector TSVECTOR;

-- Update existing rows (batch-safe, run once after migration)
UPDATE providers
SET search_vector = to_tsvector('english',
  coalesce(npi, '') || ' ' ||
  coalesce(name_last, '') || ' ' ||
  coalesce(name_first, '') || ' ' ||
  coalesce(specialty, '') || ' ' ||
  coalesce(city, '') || ' ' ||
  coalesce(state, '')
)
WHERE search_vector IS NULL;

-- GIN index for fast full-text lookups (run separately — CONCURRENTLY cannot be in a transaction)
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_providers_search ON providers USING GIN(search_vector);

-- Trigger to keep search_vector up to date on insert/update
CREATE OR REPLACE FUNCTION providers_search_vector_update() RETURNS TRIGGER AS $$
BEGIN
  NEW.search_vector := to_tsvector('english',
    coalesce(NEW.npi, '') || ' ' ||
    coalesce(NEW.name_last, '') || ' ' ||
    coalesce(NEW.name_first, '') || ' ' ||
    coalesce(NEW.specialty, '') || ' ' ||
    coalesce(NEW.city, '') || ' ' ||
    coalesce(NEW.state, '')
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS providers_search_vector_trigger ON providers;
CREATE TRIGGER providers_search_vector_trigger
  BEFORE INSERT OR UPDATE ON providers
  FOR EACH ROW EXECUTE FUNCTION providers_search_vector_update();

-- ── 6. Migrate JSONB flags → fraud_flags (one-time backfill) ─────────────────
-- Uncomment and run manually after verifying the migration above is correct.
-- This converts existing p.flags JSONB rows to normalized fraud_flags rows.
--
-- INSERT INTO fraud_flags (npi, flag_type, layer, severity, explanation, is_active, created_at)
-- SELECT
--   p.npi,
--   flag->>'type'       AS flag_type,
--   1                   AS layer,
--   CASE flag->>'severity'
--     WHEN 'critical' THEN 1
--     WHEN 'high'     THEN 2
--     WHEN 'medium'   THEN 3
--     ELSE 3
--   END                 AS severity,
--   flag->>'text'       AS explanation,
--   TRUE                AS is_active,
--   NOW()               AS created_at
-- FROM providers p,
--   jsonb_array_elements(CASE WHEN jsonb_typeof(p.flags) = 'array' THEN p.flags ELSE '[]'::jsonb END) AS flag
-- WHERE jsonb_typeof(p.flags) = 'array' AND jsonb_array_length(p.flags) > 0
-- ON CONFLICT DO NOTHING;

-- ── Done ──────────────────────────────────────────────────────────────────────
-- After running this migration:
--   1. Restart the FastAPI server to pick up new routes
--   2. Run: CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_providers_search ON providers USING GIN(search_vector);
--   3. Optionally run the JSONB flags backfill above
--   4. Run load_billing.py to populate billing_records
--   5. Run detect_layer1.py to populate fraud_flags
