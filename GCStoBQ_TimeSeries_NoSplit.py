# GCStoBQ_TimeSeries_YYYYMM_FINAL_FIXED.py
#
# FULL REFRESH loader:
# - One BigQuery table per Metric_Submetric (TitleCase)
# - Skip first sheet in every Excel file
# - FEATURES are rows, PERIODS are columns
# - Output grain: (company_id, yyyymm)
# - BigQuery-safe: lowercase columns + uniqueness
#
# Install:
#   pip install pandas openpyxl google-cloud-storage google-cloud-bigquery pyarrow

import io
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import storage
from google.cloud import bigquery


# ============================================================
# CONFIG
# ============================================================
GCP_PROJECT_ID = "gcp-sfpl-etl-demo-project6"
BQ_DATASET_ID = "Shravas_Lab"

GCS_BUCKET = "shravas-lab"
GCS_PREFIX = "DATA/"

HEADER_ROW_1BASED = 6
HEADER_ROW_0BASED = HEADER_ROW_1BASED - 1

SKIP_FIRST_SHEET = True
MAX_ROWS_PER_LOAD = 500_000


# ============================================================
# EXCLUDE FILTERS
# ============================================================
EXCLUDE_MAINMETRICS = {
    'EquityMetrics',
     #"RatiosMetrics",
     #'StockMetrics',
     'GeneralMetrics',
    # 'FinanceMetrics'
     
}

EXCLUDE_SUBMETRICS = {
#'FinancialRatios',
#'ValuationRatios',
#'BanksRBI',
#'CashFlowRatios',
#'DuPontAnalysis',
'BalanceSheet',
#'CashFlow',
#'CSR',
#'ExpenditureR&D',
#'FinancialHighlights',
'Forex',
'FundFlow',
'Investments',
'MaturityPattern',
'OperationalData',
'ProfitAndLoss',
'RPD',
'RPT',
'SectorExposure',
'SegmentFinance',
'Subsidiaries',
'Averages',
'SharePrice',
'BlockDeals',
'DeliverableVolume',
'HighsAndLows',
'StockReturn',
'BulkDeals',
#'StockCues'
}

EXCLUDE_METRIC_SUBMETRIC = {
    # ("FinanceMetrics", "CashFlow"),
}


# ============================================================
# HELPERS
# ============================================================
MONTH_MAP = {
    "jan": "01", "january": "01",
    "feb": "02", "february": "02",
    "mar": "03", "march": "03",
    "apr": "04", "april": "04",
    "may": "05",
    "jun": "06", "june": "06",
    "jul": "07", "july": "07",
    "aug": "08", "august": "08",
    "sep": "09", "sept": "09", "september": "09",
    "oct": "10", "october": "10",
    "nov": "11", "november": "11",
    "dec": "12", "december": "12",
}


def sanitize_column(s: str) -> str:
    """BigQuery-safe, lowercase column names (case-insensitive duplicate safe)."""
    s = re.sub(r"[^A-Za-z0-9_]", "_", str(s))
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "unknown"
    if s[0].isdigit():
        s = f"t_{s}"
    return s.lower()


def make_unique(cols: List[str]) -> List[str]:
    """Ensure uniqueness after lowercasing (BigQuery treats field names case-insensitively)."""
    seen = {}
    out = []
    for c in cols:
        c = str(c).lower()
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}__{seen[c]}")
    return out


def title_case_table_part(x: str) -> str:
    """FinanceMetrics, CashFlow style for table names."""
    s = re.sub(r"[^A-Za-z0-9]", "", str(x))
    return s[0].upper() + s[1:] if s else "Unknown"


def period_to_yyyymm(val) -> str:
    """Mar-2021 -> 202103, 2021-03-31 -> 202103, 202103 -> 202103."""
    s = str(val).strip()

    # 202103 / 20210331
    m = re.search(r"\b(19|20)\d{2}(0[1-9]|1[0-2])(\d{2})?\b", s)
    if m:
        raw = m.group(0)
        return raw[:6]

    # 2021-03 / 2021/03 / 2021.03
    m = re.search(r"\b((19|20)\d{2})\s*[-/\.]\s*(0?[1-9]|1[0-2])\b", s)
    if m:
        return f"{m.group(1)}{int(m.group(3)):02d}"

    # month-name with year: Mar-2021, March 2021, 2021 Mar
    t = s.lower()
    y = re.search(r"(19|20)\d{2}", t)
    if y:
        yyyy = y.group(0)
        for mon, mm in MONTH_MAP.items():
            if re.search(rf"\b{re.escape(mon)}\b", t):
                return f"{yyyy}{mm}"

    # year only
    y = re.search(r"(19|20)\d{2}", s)
    if y:
        return f"{y.group(0)}01"

    # fallback (stable)
    return sanitize_column(s)


def parse_metric(blob_name: str) -> Optional[Tuple[str, str]]:
    parts = blob_name.replace("\\", "/").split("/")
    if len(parts) < 4 or parts[0] != "DATA":
        return None
    if not parts[-1].lower().endswith(".xlsx"):
        return None
    mainmetric = parts[1]
    submetric = parts[-1].rsplit(".", 1)[0]
    return mainmetric, submetric


def force_all_to_string(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        df[c] = df[c].astype("string")
    return df


def chunk_df(df: pd.DataFrame, max_rows: int):
    for start in range(0, len(df), max_rows):
        yield df.iloc[start:start + max_rows].copy()


# ============================================================
# TRANSFORM
# ============================================================
def transform_sheet(company_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    df:
      col0 = feature names
      col1.. = period columns
    output:
      company_id, yyyymm, feature_cols...
    """
    df = df.copy()
    df = df.dropna(how="all").dropna(axis=1, how="all")
    if df.shape[1] < 2:
        return pd.DataFrame()

    feature_col = df.columns[0]
    time_cols = list(df.columns[1:])

    # normalize feature names
    df[feature_col] = (
        df[feature_col]
        .astype(str)
        .str.replace(r"[\r\n]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    df = df[df[feature_col] != ""]
    if df.empty:
        return pd.DataFrame()

    # make feature names unique BEFORE pivot (keeps meaning)
    dup = df.groupby(feature_col).cumcount()
    df[feature_col] = df[feature_col] + dup.map(lambda x: f"__{x+1}" if x else "")

    # melt -> long
    long_df = df.melt(
        id_vars=[feature_col],
        value_vars=time_cols,
        var_name="period",
        value_name="value"
    )
    long_df["yyyymm"] = long_df["period"].map(period_to_yyyymm)

    # pivot -> wide
    pivot = long_df.pivot_table(
        index="yyyymm",
        columns=feature_col,
        values="value",
        aggfunc="first"
    ).reset_index()

    pivot.insert(0, "company_id", company_id)

    # BigQuery-safe columns
    pivot.columns = [sanitize_column(c) for c in pivot.columns]
    pivot.columns = make_unique(list(pivot.columns))

    pivot = force_all_to_string(pivot)

    # Drop columns that are entirely NA (helps concat + avoids warnings)
    pivot = pivot.dropna(axis=1, how="all")

    return pivot


def union_concat(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Union schema concat without pandas FutureWarning:
    - drop empty frames
    - drop all-NA columns before concat
    """
    cleaned = []
    for d in dfs:
        if d is None or d.empty:
            continue
        d = d.dropna(axis=1, how="all")
        if d.empty:
            continue
        cleaned.append(d)

    if not cleaned:
        return pd.DataFrame()

    # union columns
    all_cols = []
    seen = set()
    for d in cleaned:
        for c in d.columns:
            if c not in seen:
                seen.add(c)
                all_cols.append(c)

    normalized = []
    for d in cleaned:
        d = d.copy()
        for c in all_cols:
            if c not in d.columns:
                d[c] = pd.NA
        d = d[all_cols]
        normalized.append(d)

    out = pd.concat(normalized, ignore_index=True)
    out.columns = make_unique([str(c) for c in out.columns])
    return out


# ============================================================
# LOAD
# ============================================================
def load_df_to_bq(
    bq_client: bigquery.Client,
    table_fqdn: str,
    df: pd.DataFrame,
    write_disposition: str
) -> None:
    if df.empty:
        return

    df = df.copy()
    df.columns = [sanitize_column(c) for c in df.columns]
    df.columns = make_unique(list(df.columns))

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )

    # CRITICAL FIX:
    # schema_update_options ONLY allowed with WRITE_APPEND (or truncate on partition table)
    if write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
        job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    bq_client.load_table_from_dataframe(df, table_fqdn, job_config=job_config).result()


# ============================================================
# MAIN
# ============================================================
def main():
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    grouped: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for blob in storage_client.list_blobs(GCS_BUCKET, prefix=GCS_PREFIX):
        parsed = parse_metric(blob.name)
        if not parsed:
            continue

        mainmetric, submetric = parsed

        # excludes
        if mainmetric in EXCLUDE_MAINMETRICS:
            continue
        if submetric in EXCLUDE_SUBMETRICS:
            continue
        if (mainmetric, submetric) in EXCLUDE_METRIC_SUBMETRIC:
            continue

        grouped[(mainmetric, submetric)].append(blob.name)

    if not grouped:
        print("No eligible .xlsx files found under prefix (or everything excluded).")
        return

    bucket = storage_client.bucket(GCS_BUCKET)

    for (mainmetric, submetric), files in sorted(grouped.items()):
        table_name = f"{title_case_table_part(mainmetric)}_{title_case_table_part(submetric)}"
        table_fqdn = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"

        print(f"\n=== FULL REFRESH: {table_fqdn} ===")
        print(f"Files: {len(files)}")

        first_load_for_table = True

        for path in sorted(files):
            excel_bytes = bucket.blob(path).download_as_bytes()
            xls = pd.ExcelFile(io.BytesIO(excel_bytes))

            sheets = xls.sheet_names
            if SKIP_FIRST_SHEET and sheets:
                sheets = sheets[1:]

            per_company: List[pd.DataFrame] = []

            for sheet in sheets:
                company_id = str(sheet).strip()
                try:
                    df = pd.read_excel(xls, sheet_name=sheet, header=HEADER_ROW_0BASED, dtype=object)
                except Exception:
                    continue

                transformed = transform_sheet(company_id, df)
                if not transformed.empty:
                    per_company.append(transformed)

            file_df = union_concat(per_company)
            if file_df.empty:
                print(f"  - SKIP (no usable transformed data): {path}")
                continue

            for piece in chunk_df(file_df, MAX_ROWS_PER_LOAD):
                mode = (
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if first_load_for_table
                    else bigquery.WriteDisposition.WRITE_APPEND
                )
                load_df_to_bq(bq_client, table_fqdn, piece, mode)
                print(f"  -> LOADED file={path} rows={len(piece):,} mode={mode}")
                first_load_for_table = False

    print("\nDONE")


if __name__ == "__main__":
    main()
