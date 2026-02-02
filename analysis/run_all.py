# analysis/run_all.py
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings

import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.tools.sm_exceptions import PerfectSeparationWarning


# --- Cluster-robust helper utilities ---
def _cluster_codes(s: pd.Series) -> np.ndarray:
    """Convert arbitrary group labels (e.g., slugs) into stable integer codes for cluster-robust SE."""
    s = s.astype("string")
    return pd.Categorical(s).codes


def fit_binary_with_cluster_fallback(formula: str, df: pd.DataFrame, cluster_col: str = "slug"):
    """Try Binomial GLM with cluster-robust SE; if it fails (rare events / FE / singular), fall back to OLS LPM."""

    # --- Try Binomial GLM
    try:
        with warnings.catch_warnings():
            # Perfect separation often happens with rare events + lots of FE.
            # Treat as failure so we fall back to OLS LPM (draft-safe, stable).
            warnings.filterwarnings("error", category=PerfectSeparationWarning)

            glm = smf.glm(formula=formula, data=df, family=sm.families.Binomial())
            used = glm.data.frame  # rows actually used after Patsy drops NA
            groups = _cluster_codes(used[cluster_col])
            res = glm.fit(cov_type="cluster", cov_kwds={"groups": groups})

        out = pd.DataFrame({
            "term": res.params.index,
            "coef": res.params.values,
            "se_cluster_slug": res.bse.values,
            "stat": res.tvalues.values,  # z-stat for GLM
            "p": res.pvalues.values,
            "model": "glm_binomial_cluster",
        })
        return out
    except Exception:
        # --- Fall back to OLS LPM
        ols = smf.ols(formula=formula, data=df)
        used = ols.data.frame
        groups = _cluster_codes(used[cluster_col])
        res = ols.fit(cov_type="cluster", cov_kwds={"groups": groups})

        out = pd.DataFrame({
            "term": res.params.index,
            "coef": res.params.values,
            "se_cluster_slug": res.bse.values,
            "stat": res.tvalues.values,  # t-stat for OLS
            "p": res.pvalues.values,
            "model": "ols_lpm_cluster",
        })
        return out

PANEL_PATH = "data_clean/panel_protocol_year.csv"
M1_PATH = "data_clean/m1_exploit_audit.csv"

OUT_TABLES = "outputs/tables"
OUT_FIGS = "outputs/figures"

def ensure_dirs():
    os.makedirs(OUT_TABLES, exist_ok=True)
    os.makedirs(OUT_FIGS, exist_ok=True)

def parse_dt(series):
    return pd.to_datetime(series, utc=True, errors="coerce")

def save_table(df, name, index=False):
    csv_path = os.path.join(OUT_TABLES, f"{name}.csv")
    tex_path = os.path.join(OUT_TABLES, f"{name}.tex")
    df.to_csv(csv_path, index=index)
    # LaTeX (simple, draft-friendly)
    try:
        df.to_latex(tex_path, index=index, float_format="%.4f")
    except Exception:
        # If LaTeX export fails for some dtype, still keep CSV
        pass
    print("Saved table:", csv_path)

def save_fig(name):
    path = os.path.join(OUT_FIGS, f"{name}.png")
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()
    print("Saved figure:", path)

def load_panel():
    # Read as strings where needed to avoid mixed dtype warnings
    p = pd.read_csv(PANEL_PATH, low_memory=False, dtype={"last_audit_date_dt": "string"})
    # Parse dates
    p["last_audit_date_dt"] = parse_dt(p["last_audit_date_dt"])
    p["year_end"] = parse_dt(p["year_end"])
    # Basic transforms
    tvl_num = pd.to_numeric(p["tvl"], errors="coerce")
    # TVL should not be negative; clip to avoid invalid log1p and keep scale interpretable
    tvl_num = tvl_num.clip(lower=0).fillna(0.0)
    p["log_tvl"] = np.log1p(tvl_num)
    p["audit_firm_count"] = pd.to_numeric(p.get("audit_firm_count", 0), errors="coerce").fillna(0)
    p["any_top_firm"] = pd.to_numeric(p.get("any_top_firm", 0), errors="coerce").fillna(0).astype(int)
    p["has_audit"] = pd.to_numeric(p.get("has_audit", 0), errors="coerce").fillna(0).astype(int)
    p["audited_by_year_end"] = pd.to_numeric(p.get("audited_by_year_end", 0), errors="coerce").fillna(0).astype(int)

    # time_since_last_audit_days can be float because NA
    p["time_since_last_audit_days"] = pd.to_numeric(p["time_since_last_audit_days"], errors="coerce")
    # Negative values (audit after year_end or bad merges) are not meaningful for "time since"; set to NA
    p.loc[p["time_since_last_audit_days"] < 0, "time_since_last_audit_days"] = np.nan

    # Exploit outcomes
    p["exploited_this_year"] = pd.to_numeric(p["exploited_this_year"], errors="coerce").fillna(0).astype(int)
    p["exploit_count"] = pd.to_numeric(p["exploit_count"], errors="coerce").fillna(0).astype(int)
    p["total_loss_usd"] = pd.to_numeric(p["total_loss_usd"], errors="coerce").fillna(0.0)
    p["max_loss_usd"] = pd.to_numeric(p["max_loss_usd"], errors="coerce").fillna(0.0)

    # Audit score: keep numeric; DO NOT use in full-sample baseline because it's almost all NA
    if "audit_score" in p.columns:
        p["audit_score"] = pd.to_numeric(p["audit_score"], errors="coerce")
        p["audit_score_available"] = p["audit_score"].notna().astype(int)
    else:
        p["audit_score"] = np.nan
        p["audit_score_available"] = 0

    # A simple chain proxy (first chain token) for quick FE if needed later
    p["chain_main"] = p["chains_llama"].astype(str).str.split(";").str[0].replace("nan", np.nan)

    # Category clean
    p["category_llama"] = p["category_llama"].astype(str).replace("nan", np.nan)

    return p

def load_m1():
    m1 = pd.read_csv(M1_PATH, low_memory=False, dtype={"last_audit_date_dt":"string", "exploit_dt":"string"})
    m1["exploit_dt"] = parse_dt(m1.get("exploit_dt", m1.get("exploit_date")))
    m1["last_audit_date_dt"] = parse_dt(m1.get("last_audit_date_dt", m1.get("last_audit_date")))
    m1["loss_usd"] = pd.to_numeric(m1["loss_usd"], errors="coerce")
    # Keep DeFi-mapped only (slug notna)
    if "slug" in m1.columns:
        m1 = m1[m1["slug"].notna()].copy()

    # Days since last audit (only meaningful if audited)
    m1["days_since_last_audit"] = (m1["exploit_dt"] - m1["last_audit_date_dt"]).dt.days
    m1.loc[m1["days_since_last_audit"] < 0, "days_since_last_audit"] = np.nan

    # Controls
    for col in ["has_audit", "any_top_firm", "audit_firm_count"]:
        if col in m1.columns:
            m1[col] = pd.to_numeric(m1[col], errors="coerce").fillna(0)
    if "audit_score" in m1.columns:
        m1["audit_score"] = pd.to_numeric(m1["audit_score"], errors="coerce")

    # Normalize soft/strict audit indicators if present
    for col in ["has_full_audit", "has_certik_badge"]:
        if col in m1.columns:
            m1[col] = pd.to_numeric(m1[col], errors="coerce").fillna(0).astype(int)

    m1["log_loss"] = np.log1p(m1["loss_usd"].fillna(0.0))
    return m1

def table_summary_stats(panel):
    # Summary stats audited vs unaudited (by year-end)
    grp = panel.groupby("audited_by_year_end", dropna=False)
    out = grp.agg(
        n=("slug", "size"),
        protocols=("slug", "nunique"),
        exploited_rate=("exploited_this_year", "mean"),
        avg_log_tvl=("log_tvl", "mean"),
        avg_exploit_count=("exploit_count", "mean"),
        avg_total_loss_usd=("total_loss_usd", "mean"),
    ).reset_index()
    save_table(out, "table_A_summary_stats_audited_by_year_end", index=False)

def fig_exploits_over_time(panel):
    # Exploit counts per year (protocol-year cells with exploit)
    by_year = panel.groupby("year").agg(
        exploited_cells=("exploited_this_year", "sum"),
        total_loss=("total_loss_usd", "sum"),
    ).reset_index()

    plt.figure()
    plt.plot(by_year["year"], by_year["exploited_cells"])
    plt.xlabel("Year")
    plt.ylabel("Exploited protocol-year cells")
    plt.title("Exploits over time (DeFi-mapped)")
    save_fig("fig_exploits_cells_over_time")

    plt.figure()
    plt.plot(by_year["year"], by_year["total_loss"])
    plt.xlabel("Year")
    plt.ylabel("Total loss (USD, sum over protocol-year)")
    plt.title("Total loss over time (DeFi-mapped)")
    save_fig("fig_total_loss_over_time")

def fig_audited_vs_unaudited_rates(panel):
    # Exploit rate by year and audited status
    tmp = panel.groupby(["year", "audited_by_year_end"]).agg(
        exploited_rate=("exploited_this_year", "mean"),
        n=("slug", "size"),
    ).reset_index()

    plt.figure()
    for k, dfk in tmp.groupby("audited_by_year_end"):
        plt.plot(dfk["year"], dfk["exploited_rate"], label=f"audited_by_year_end={k}")
    plt.xlabel("Year")
    plt.ylabel("Exploit rate (mean over protocol-year)")
    plt.title("Exploit rate: audited vs unaudited")
    plt.legend()
    save_fig("fig_exploit_rate_audited_vs_unaudited")

def panel_regression_baseline(panel):
    # Baseline probability model:
    # exploited_this_year ~ audited_by_year_end + log_tvl + any_top_firm + audit_firm_count + year FE + category FE
    # (Avoid audit_score here due to missingness)
    df = panel.copy()
    df = df.dropna(subset=["category_llama"])  # simple FE handling

    formula = "exploited_this_year ~ audited_by_year_end + log_tvl + any_top_firm + audit_firm_count + C(year) + C(category_llama)"

    coefs = fit_binary_with_cluster_fallback(formula, df, cluster_col="slug")
    save_table(coefs, "table_B_panel_baseline_coefs", index=False)

def panel_regression_timing_audited_only(panel):
    # Timing model only among audited_by_year_end==1 (so days since audit is interpretable)
    df = panel[(panel["audited_by_year_end"] == 1)].copy()
    df = df.dropna(subset=["time_since_last_audit_days", "category_llama"])
    # log(1+days)
    df["log_days_since_audit"] = np.log1p(df["time_since_last_audit_days"])
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["log_days_since_audit"])

    formula = "exploited_this_year ~ log_days_since_audit + log_tvl + any_top_firm + audit_firm_count + C(year) + C(category_llama)"

    coefs = fit_binary_with_cluster_fallback(formula, df, cluster_col="slug")
    save_table(coefs, "table_B2_panel_timing_audited_only_coefs", index=False)


def event_severity_regression(m1: pd.DataFrame):
    """Event-level severity regressions.

    Why this exists:
    - Including timing terms (days since last audit) can drop almost all rows because last_audit_date is often missing.
    - If Patsy drops all rows, statsmodels can crash with a zero-size design matrix.

    We therefore run:
      (C1) Base severity model (no timing) on the largest usable sample.
      (C2) Timing model only on the subset where `days_since_last_audit` is available.
      (C3) Optional soft/strict audit proxy model if those columns exist.

    Output tables are saved under outputs/tables.
    """

    df = m1.copy()

    # Keep only rows with a mapped protocol and a defined loss
    if "slug" in df.columns:
        df = df[df["slug"].notna()].copy()
    df = df[df["loss_usd"].notna()].copy()

    if len(df) == 0:
        diag = pd.DataFrame([{
            "note": "No event rows available for severity regression after filtering (slug/loss).",
            "rows": 0,
        }])
        save_table(diag, "table_C0_event_severity_diagnostics", index=False)
        return

    # Ensure covariates exist and are numeric
    for col in ["has_audit", "any_top_firm", "audit_firm_count"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Ensure log_loss exists
    if "log_loss" not in df.columns:
        df["log_loss"] = np.log1p(pd.to_numeric(df["loss_usd"], errors="coerce").fillna(0.0))

    # -------------------------
    # (C1) Base severity model (largest sample): no timing term
    # -------------------------
    formula_base = "log_loss ~ has_audit + any_top_firm + audit_firm_count"
    model_base = smf.ols(formula=formula_base, data=df)
    used_base = model_base.data.frame

    if len(used_base) == 0:
        diag = pd.DataFrame([{
            "note": "Base severity model has 0 usable rows after Patsy handling.",
            "rows": 0,
        }])
        save_table(diag, "table_C0_event_severity_diagnostics", index=False)
        return

    groups_base = _cluster_codes(used_base["slug"])
    res_base = model_base.fit(cov_type="cluster", cov_kwds={"groups": groups_base})

    coefs_base = pd.DataFrame({
        "term": res_base.params.index,
        "coef": res_base.params.values,
        "se_cluster_slug": res_base.bse.values,
        "stat": res_base.tvalues.values,
        "p": res_base.pvalues.values,
        "model": "ols_base_cluster",
        "n": len(used_base),
    })
    save_table(coefs_base, "table_C1_ols_event_severity_base_coefs", index=False)

    # -------------------------
    # (C2) Timing severity model: only where days_since_last_audit exists
    # -------------------------
    if "days_since_last_audit" in df.columns:
        df_t = df[df["days_since_last_audit"].notna()].copy()

        if len(df_t) > 0:
            df_t["log_days_since_audit"] = np.log1p(
                pd.to_numeric(df_t["days_since_last_audit"], errors="coerce")
            )
            df_t = df_t.replace([np.inf, -np.inf], np.nan).dropna(subset=["log_days_since_audit"])

            if len(df_t) > 0:
                formula_timing = "log_loss ~ has_audit + any_top_firm + audit_firm_count + log_days_since_audit"
                model_timing = smf.ols(formula=formula_timing, data=df_t)
                used_timing = model_timing.data.frame

                if len(used_timing) > 0:
                    groups_timing = _cluster_codes(used_timing["slug"])
                    res_timing = model_timing.fit(cov_type="cluster", cov_kwds={"groups": groups_timing})

                    coefs_timing = pd.DataFrame({
                        "term": res_timing.params.index,
                        "coef": res_timing.params.values,
                        "se_cluster_slug": res_timing.bse.values,
                        "stat": res_timing.tvalues.values,
                        "p": res_timing.pvalues.values,
                        "model": "ols_timing_cluster",
                        "n": len(used_timing),
                    })
                    save_table(coefs_timing, "table_C2_ols_event_severity_timing_coefs", index=False)
                else:
                    diag = pd.DataFrame([{
                        "note": "Timing severity model has 0 usable rows after Patsy handling.",
                        "rows": 0,
                    }])
                    save_table(diag, "table_C2_event_severity_timing_diagnostics", index=False)
            else:
                diag = pd.DataFrame([{
                    "note": "Timing severity model: no usable rows after computing log_days_since_audit.",
                    "rows": int(len(df_t)),
                }])
                save_table(diag, "table_C2_event_severity_timing_diagnostics", index=False)
        else:
            diag = pd.DataFrame([{
                "note": "Timing severity model skipped: days_since_last_audit missing for all rows.",
                "rows": 0,
            }])
            save_table(diag, "table_C2_event_severity_timing_diagnostics", index=False)
    else:
        diag = pd.DataFrame([{
            "note": "Timing severity model skipped: days_since_last_audit column not found.",
            "rows": int(len(df)),
        }])
        save_table(diag, "table_C2_event_severity_timing_diagnostics", index=False)

    # -------------------------
    # (C3) Soft vs strict audit proxies (optional)
    # -------------------------
    has_soft_strict = ("has_full_audit" in df.columns) or ("has_certik_badge" in df.columns)
    if has_soft_strict:
        df_ss = df.copy()
        if "has_full_audit" not in df_ss.columns:
            df_ss["has_full_audit"] = 0
        if "has_certik_badge" not in df_ss.columns:
            df_ss["has_certik_badge"] = 0

        df_ss["has_full_audit"] = pd.to_numeric(df_ss["has_full_audit"], errors="coerce").fillna(0).astype(int)
        df_ss["has_certik_badge"] = pd.to_numeric(df_ss["has_certik_badge"], errors="coerce").fillna(0).astype(int)

        formula_ss = "log_loss ~ has_audit + has_full_audit + has_certik_badge + any_top_firm + audit_firm_count"
        model_ss = smf.ols(formula=formula_ss, data=df_ss)
        used_ss = model_ss.data.frame

        if len(used_ss) > 0:
            groups_ss = _cluster_codes(used_ss["slug"])
            res_ss = model_ss.fit(cov_type="cluster", cov_kwds={"groups": groups_ss})

            coefs_ss = pd.DataFrame({
                "term": res_ss.params.index,
                "coef": res_ss.params.values,
                "se_cluster_slug": res_ss.bse.values,
                "stat": res_ss.tvalues.values,
                "p": res_ss.pvalues.values,
                "model": "ols_soft_strict_cluster",
                "n": len(used_ss),
            })
            save_table(coefs_ss, "table_C3_ols_event_severity_soft_strict_coefs", index=False)
        else:
            diag = pd.DataFrame([{
                "note": "Soft/strict severity model has 0 usable rows after Patsy handling.",
                "rows": 0,
            }])
            save_table(diag, "table_C3_event_severity_soft_strict_diagnostics", index=False)

def main():
    ensure_dirs()
    panel = load_panel()
    m1 = load_m1()

    # --- Tables (draft-ready)
    table_summary_stats(panel)

    # --- Figures (draft-ready)
    fig_exploits_over_time(panel)
    fig_audited_vs_unaudited_rates(panel)

    # --- Regressions (core, defensible even for first draft)
    panel_regression_baseline(panel)
    panel_regression_timing_audited_only(panel)
    event_severity_regression(m1)

    # --- Quick diagnostics saved as a small table
    diag = pd.DataFrame([{
        "panel_rows": len(panel),
        "protocols": panel["slug"].nunique(),
        "years_min": int(panel["year"].min()),
        "years_max": int(panel["year"].max()),
        "exploit_rate": float(panel["exploited_this_year"].mean()),
        "audit_score_available_rate": float(panel["audit_score_available"].mean()),
    }])
    save_table(diag, "table_0_diagnostics", index=False)

    print("\nDONE. Check outputs/figures and outputs/tables\n")

if __name__ == "__main__":
    main()