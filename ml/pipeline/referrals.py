"""
referrals.py — Seed synthetic referral edges for demo/analysis purposes.

Methodology:
  - Take top 2,000 providers by risk_score
  - Within each state, randomly connect providers (simulating referral patterns)
  - Higher-risk providers get more connections
  - Edges between two high-risk providers (risk > 70) are flagged is_suspicious=True

In production this would be replaced with CMS Shared Patient data or
claims-level co-occurrence analysis.
"""

import os
import sys
import random
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / "backend" / ".env")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://vigil:vigil@localhost:5432/vigil"
).replace("postgresql+asyncpg://", "postgresql://")

PROC_DIR = Path(__file__).parent.parent / "data" / "processed"

BATCH_SIZE = 2_000
TOP_N = 2_000       # providers to include in the network
SEED = 42


def _conn():
    return psycopg2.connect(DATABASE_URL)


def generate_edges(df: pd.DataFrame, rng: np.random.Generator) -> list[dict]:
    """
    For each provider, connect them to 2-6 others in the same state.
    Weight connection probability by risk_score so high-risk providers
    cluster together.
    """
    edges = []
    seen = set()

    by_state = df.groupby("state")

    for state, group in by_state:
        if len(group) < 2:
            continue

        npis = group["npi"].values
        risks = group["risk_score"].fillna(0).values
        # Normalise to probability weights
        weights = risks + 1.0
        weights = weights / weights.sum()

        for i, row in group.iterrows():
            npi = row["npi"]
            risk = row["risk_score"] or 0
            # Higher-risk providers get more connections (3–8)
            n_connections = int(3 + min(5, risk / 20))
            n_connections = min(n_connections, len(npis) - 1)

            # Sample targets (weighted by risk — fraudsters refer to fraudsters)
            candidates = npis[npis != npi]
            cand_weights = weights[npis != npi]
            cand_weights = cand_weights / cand_weights.sum()

            targets = rng.choice(
                candidates,
                size=min(n_connections, len(candidates)),
                replace=False,
                p=cand_weights,
            )

            for target_npi in targets:
                key = tuple(sorted([npi, target_npi]))
                if key in seen:
                    continue
                seen.add(key)

                target_risk = group.loc[group["npi"] == target_npi, "risk_score"]
                target_risk_val = float(target_risk.values[0]) if len(target_risk) else 0.0

                referral_count = int(rng.integers(5, 120))
                shared_patients = int(referral_count * rng.uniform(0.4, 0.9))
                total_payment = round(float(shared_patients * rng.uniform(800, 4500)), 2)
                referral_pct = round(float(rng.uniform(5, 45)), 2)
                # Suspicious when both are in the top 20% of this high-risk cohort
                suspicious_threshold = float(df["risk_score"].quantile(0.80)) if "risk_score" in df.columns else 90.0
                is_suspicious = (risk >= suspicious_threshold and target_risk_val >= suspicious_threshold)

                edges.append({
                    "source_npi": str(npi),
                    "target_npi": str(target_npi),
                    "referral_count": referral_count,
                    "shared_patients": shared_patients,
                    "total_payment": total_payment,
                    "referral_percentage": referral_pct,
                    "is_suspicious": is_suspicious,
                })

    return edges


def run():
    print("\n=== REFERRALS ===")

    # Load scored providers
    df = pd.read_parquet(PROC_DIR / "scored_with_flags.parquet")
    df = df[["npi", "state", "specialty", "risk_score"]].copy()
    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0)

    # Take top N by risk
    top = df.nlargest(TOP_N, "risk_score").reset_index(drop=True)
    print(f"  Building network for top {len(top):,} providers…")
    print(f"  States covered: {top['state'].nunique()}")

    rng = np.random.default_rng(SEED)
    edges = generate_edges(top, rng)
    print(f"  Generated {len(edges):,} referral edges")
    suspicious = sum(1 for e in edges if e["is_suspicious"])
    print(f"  Suspicious edges: {suspicious:,}")

    # Upsert into DB
    conn = _conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE referral_edges RESTART IDENTITY")

    sql = """
    INSERT INTO referral_edges
        (source_npi, target_npi, referral_count, shared_patients,
         total_payment, referral_percentage, is_suspicious)
    VALUES %s
    """
    rows = [
        (
            e["source_npi"], e["target_npi"], e["referral_count"],
            e["shared_patients"], e["total_payment"],
            e["referral_percentage"], e["is_suspicious"],
        )
        for e in edges
    ]

    for i in range(0, len(rows), BATCH_SIZE):
        psycopg2.extras.execute_values(cur, sql, rows[i:i + BATCH_SIZE], page_size=BATCH_SIZE)
        conn.commit()

    print(f"  [db] Inserted {len(rows):,} edges")
    cur.close()
    conn.close()

    return edges


if __name__ == "__main__":
    run()
