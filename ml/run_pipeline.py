"""
run_pipeline.py — Full pipeline orchestrator.

Usage
-----
    # Full run (downloads everything, trains models, scores, loads DB)
    python run_pipeline.py

    # Skip the large HCPCS file (no entropy/E&M features — faster)
    python run_pipeline.py --skip-hcpcs

    # Skip download + ingest if parquet files already exist
    python run_pipeline.py --skip-ingest

    # Skip model training (use saved models)
    python run_pipeline.py --skip-train

    # Dry run — stop after scoring, don't write to DB
    python run_pipeline.py --dry-run

Steps
-----
1. ingest   — download CMS Part B + LEIE, aggregate to one row per NPI
2. features — compute peer medians, z-scores, entropy, E&M ratio
3. train    — train Isolation Forest, XGBoost, Autoencoder
4. score    — score all providers, compute composite risk_score 0-100
5. flags    — generate human-readable anomaly flag objects
6. load_db  — upsert everything into PostgreSQL
"""

import sys
import time
from pathlib import Path

# Add ml/ to path so pipeline imports work
sys.path.insert(0, str(Path(__file__).parent))

import argparse


def parse_args():
    p = argparse.ArgumentParser(description="Vigil ML pipeline")
    p.add_argument("--skip-hcpcs",   action="store_true", help="Skip HCPCS download (no entropy/E&M features)")
    p.add_argument("--skip-ingest",  action="store_true", help="Skip ingest step (use cached parquet)")
    p.add_argument("--skip-train",   action="store_true", help="Skip training (use saved models)")
    p.add_argument("--skip-yoy",     action="store_true", help="Skip year-over-year surge detection")
    p.add_argument("--leie-only",    action="store_true", help="Only run LEIE refresh (fastest — no retraining)")
    p.add_argument("--dry-run",      action="store_true", help="Stop before loading DB")
    return p.parse_args()


def step(name: str, fn, *args, **kwargs):
    t0 = time.time()
    print(f"\n{'='*50}")
    result = fn(*args, **kwargs)
    elapsed = time.time() - t0
    print(f"  ✓ {name} completed in {elapsed:.1f}s")
    return result


def main():
    args = parse_args()
    t_start = time.time()

    print("╔══════════════════════════════════════════╗")
    print("║    VIGIL — Medicare Fraud ML Pipeline    ║")
    print("╚══════════════════════════════════════════╝")

    # ── LEIE-only fast path ────────────────────────────────────────────────────
    if args.leie_only:
        from pipeline.refresh_leie import run as refresh_leie
        delta = step("LEIE Refresh", refresh_leie, dry_run=args.dry_run)
        print(f"\n  LEIE refresh complete in {time.time() - t_start:.1f}s")
        print(f"  New exclusions: {len(delta.get('newly_excluded', []))}")
        print(f"  Reinstatements: {len(delta.get('newly_reinstated', []))}")
        return

    from pipeline.ingest      import run as ingest
    from pipeline.features    import build as features
    from pipeline.train       import run as train
    from pipeline.score       import run as score
    from pipeline.flags       import run as flags
    from pipeline.load_db     import run as load_db
    from pipeline.refresh_leie import run as refresh_leie
    from pipeline.detect_yoy  import run as detect_yoy

    # 1. Ingest
    PROC_DIR = Path(__file__).parent / "data" / "processed"
    HISTORICAL_YEARS = [2021, 2020, 2019, 2018]

    if args.skip_ingest and (PROC_DIR / "providers_aggregated.parquet").exists():
        print("\n[skip] ingest — using cached parquet files")
        import pandas as pd
        providers = pd.read_parquet(PROC_DIR / "providers_aggregated.parquet")
        leie      = pd.read_parquet(PROC_DIR / "leie.parquet")
    else:
        providers, leie = step("Ingest", ingest, skip_hcpcs=args.skip_hcpcs)

    import pandas as pd
    historical_providers = {}
    for yr in HISTORICAL_YEARS:
        path = PROC_DIR / f"providers_aggregated_{yr}.parquet"
        if path.exists():
            historical_providers[yr] = pd.read_parquet(path)

    # 2. Features (2022 — production scoring data)
    df = step("Features 2022", features, providers)

    # 2b. Features for each historical year — separate peer groups per year
    if historical_providers:
        for yr, yr_provs in historical_providers.items():
            step(f"Features {yr}", features, yr_provs,
                 PROC_DIR / f"features_{yr}.parquet")
    else:
        print("\n[info] No historical provider data found — training on 2022 only")

    # 3. Train
    MODELS_DIR = Path(__file__).parent / "models"
    models_exist = all(
        (MODELS_DIR / f).exists()
        for f in ["isolation_forest.joblib", "xgboost.joblib", "autoencoder.joblib"]
    )
    if args.skip_train and models_exist:
        print("\n[skip] train — using saved models")
    else:
        step("Train", train)

    # 4. Score
    df_scored = step("Score", score, df)

    # 5. Flags
    df_flagged = step("Flags", flags, df_scored)

    # 6. Load DB
    if args.dry_run:
        print("\n[dry-run] Skipping DB load.")
        out = PROC_DIR / "scored_with_flags.parquet"
        print(f"  Results at: {out}")
    else:
        step("Load DB", load_db)

        # 6.5. LEIE refresh — run after load_db so newly loaded providers get the flag
        step("LEIE Refresh", refresh_leie)

        # 6.6. Year-over-year surge detection (runs against billing_records in DB)
        if not args.skip_yoy:
            step("YoY Surge Detection", detect_yoy)
        else:
            print("\n[skip] YoY surge detection")

    elapsed = time.time() - t_start
    print(f"\n{'='*50}")
    print(f"  Pipeline complete in {elapsed/60:.1f} minutes")
    print(f"  Providers scored: {len(df_flagged):,}")
    print(f"  High risk (70+):  {(df_flagged['risk_score'] >= 70).sum():,}")
    print(f"  Critical (90+):   {(df_flagged['risk_score'] >= 90).sum():,}")


if __name__ == "__main__":
    main()
