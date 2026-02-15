# GCStoBQ_TimeSeries_Subheading_AnchoredSearch.py
#
# Robust split when subheading rows shift across companies:
# - Uses anchor rows + window scan to find true subheading position per sheet
# - Splits strictly UNDER heading until next heading
# - No extra columns
# - Table names: <MainMetric>_<Submetric>_<Subheading>
# - Prevents many tables by locking label per heading index (first found wins)
#
# Install:
#   pip install pandas openpyxl google-cloud-storage google-cloud-bigquery pyarrow
#
# Auth:
#   gcloud auth application-default login

import io
import re
import traceback
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import storage, bigquery


# ================= CONFIG =================
PROJECT_ID = "gcp-sfpl-etl-demo-project6"
DATASET_ID = "Shravas_Lab"
BUCKET_NAME = "shravas-lab"
GCS_PREFIX = "DATA/"

HEADER_ROW_1BASED = 6
SKIP_FIRST_SHEET = True
MAX_ROWS_PER_LOAD = 500_000
FORCE_YYYYMM = True

# For each (MainMetric, Submetric), define:
# - SUBHEADINGS: ordered list of subheading titles (as seen in the sheet)
# - ANCHOR_ROWS: approximate Excel row numbers where those headings appear
# - SEARCH_WINDOW: scan +/- this many rows around each anchor to locate the heading on each sheet
SUBHEADING_CONFIG: Dict[Tuple[str, str], Dict[str, object]] = {
    # Example for BalanceSheet:
    ("FinanceMetrics", "BalanceSheet"): {
        "SUBHEADINGS": ["EQUITY AND LIABILITIES", "ASSETS"],
        "ANCHOR_ROWS": [8, 29],
        "SEARCH_WINDOW": 8,
    },
    
        ("FinanceMetrics","FundFlow"):{
            "SUBHEADINGS":["SOURCES OF FUNDS","APPLICATION OF FUNDS"],
            "ANCHOR_ROWS":[7,19],
            "SEARCH_WINDOW":2,
        },
        ("FinanceMetrics", "ProfitAndLoss"): {
        "SUBHEADINGS": ["INCOME :", "EXPENDITURE :"],
        "ANCHOR_ROWS"
        : [9, 15],
        "SEARCH_WINDOW": 4,},}
    


PROCESS_ONLY_CONFIGURED = True

EXCLUDE_MAINMETRICS = {
    # "FinanceMetrics",
     "GeneralMetrics",
     "StockMetrics",
     'RatiosMetrics',
     'EquityMetrics'
}  # DO NOT exclude EquityMetrics if you want it
EXCLUDE_SUBMETRICS = {
#'BalanceSheet'
'CashFlow',
'CSR',
'ExpenditureR&D',
'FinancialHighlights',
'Forex',
#'FundFlow',
'Investments',
'MaturityPattern',
'OperationalData',
#'ProfitAndLoss',
'RPD',
'RPT',
'SectorExposure',
'SegmentFinance', #still needs multiple splits
'Subsidiaries',
}

EXCLUDE_METRIC_SUBMETRIC = set()

MONTH_MAP = {
    "jan": "01","january":"01","feb":"02","february":"02","mar":"03","march":"03",
    "apr":"04","april":"04","may":"05","jun":"06","june":"06","jul":"07","july":"07",
    "aug":"08","august":"08","sep":"09","sept":"09","september":"09",
    "oct":"10","october":"10","nov":"11","november":"11","dec":"12","december":"12",
}

# ================ HELPERS =================
def log(msg: str) -> None:
    print(msg, flush=True)

def sanitize_column(s: str) -> str:
    s = "" if s is None else str(s)

    # remove newlines and extra spaces
    s = s.replace("\n", " ").replace("\r", " ").strip()

    # replace special chars with space
    s = re.sub(r"[^A-Za-z0-9]", " ", s)

    # split into words
    parts = s.split()

    if not parts:
        return "unknown"

    # camelCase conversion
    camel = parts[0].lower() + "".join(p.capitalize() for p in parts[1:])

    # BigQuery column cannot start with number
    if camel[0].isdigit():
        camel = f"c{camel}"

    return camel


def make_unique(cols: List[str]) -> List[str]:
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



def normalize_text(s: object) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\((.*?)\)", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()

def ensure_dataset(bq: bigquery.Client, project_id: str, dataset_id: str, location: str = "asia-south1") -> None:
    ds = bigquery.Dataset(f"{project_id}.{dataset_id}")
    try:
        bq.get_dataset(ds)
    except Exception:
        ds.location = location
        bq.create_dataset(ds, exists_ok=True)

def parse_metric(blob_name: str) -> Optional[Tuple[str, str]]:
    path = blob_name.replace("\\", "/").strip("/")
    parts = path.split("/")
    if len(parts) < 4 or parts[0] != "DATA":
        return None
    if not parts[-1].lower().endswith(".xlsx"):
        return None
    return parts[1].strip(), parts[-1].rsplit(".", 1)[0].strip()

def period_to_yyyymm(val) -> str:
    s = "" if val is None else str(val).strip()
    m = re.search(r"\b(19|20)\d{2}(0[1-9]|1[0-2])(\d{2})?\b", s)
    if m:
        raw = m.group(0)
        return raw[:6] if FORCE_YYYYMM else raw[2:6]
    m = re.search(r"\b((19|20)\d{2})\s*[-/\.]\s*(0?[1-9]|1[0-2])\b", s)
    if m:
        yyyy = m.group(1); mm = f"{int(m.group(3)):02d}"
        return f"{yyyy}{mm}" if FORCE_YYYYMM else f"{yyyy[2:]}{mm}"
    t = s.lower()
    y = re.search(r"(19|20)\d{2}", t)
    if y:
        yyyy = y.group(0)
        for mon, mm in MONTH_MAP.items():
            if re.search(rf"\b{re.escape(mon)}\b", t):
                return f"{yyyy}{mm}" if FORCE_YYYYMM else f"{yyyy[2:]}{mm}"
    y = re.search(r"(19|20)\d{2}", s)
    if y:
        return f"{y.group(0)}01" if FORCE_YYYYMM else f"{y.group(0)[2:]}01"
    return sanitize_column(s)

# ============== RAW READ ==================
def read_sheet_raw(xls: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)

def build_data_df_from_raw(raw: pd.DataFrame) -> pd.DataFrame:
    hdr_idx = HEADER_ROW_1BASED - 1
    if raw is None or raw.empty or len(raw) <= hdr_idx:
        return pd.DataFrame()
    header = ["" if x is None else str(x) for x in raw.iloc[hdr_idx].tolist()]
    df = raw.iloc[hdr_idx + 1 :].copy()
    df.columns = header
    return df

def excel_row_to_raw_index(r: int) -> int:
    return r - 1

def excel_row_to_data_index(r: int) -> int:
    return r - (HEADER_ROW_1BASED + 1)

def find_heading_row_near_anchor(raw: pd.DataFrame, heading_text: str, anchor_excel_row: int, window: int) -> Optional[int]:
    """
    Returns the Excel row number where heading_text is found near anchor (±window), else None.
    Scans each candidate row left->right for any cell containing heading_text (normalized).
    """
    if raw is None or raw.empty:
        return None
    target = normalize_text(heading_text)
    start = max(1, anchor_excel_row - window)
    end = min(len(raw), anchor_excel_row + window)
    for excel_r in range(start, end + 1):
        ri = excel_row_to_raw_index(excel_r)
        row_vals = raw.iloc[ri].tolist()
        for v in row_vals:
            if pd.notna(v):
                txt = normalize_text(v)
                if txt == target:
                    return excel_r
    return None

def slice_data_df_by_excel_range(data_df: pd.DataFrame, start_excel_row: int, end_excel_row: Optional[int]) -> pd.DataFrame:
    if data_df is None or data_df.empty:
        return pd.DataFrame()
    start_i = max(0, excel_row_to_data_index(start_excel_row))
    end_i = excel_row_to_data_index(end_excel_row) if end_excel_row is not None else len(data_df) - 1
    end_i = min(len(data_df) - 1, end_i)
    if start_i > end_i:
        return pd.DataFrame()
    return data_df.iloc[start_i:end_i+1].copy()

# ============== TRANSFORM ==================
def transform_timeseries(company_id: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.dropna(how="all").dropna(axis=1, how="all")
    if df.shape[1] < 2:
        return pd.DataFrame()
    feature_col = df.columns[0]
    time_cols = list(df.columns[1:])

    # Keep only rows that actually have values in any period column
    has_value = df[time_cols].notna().any(axis=1)
    df = df[has_value].copy()
    if df.empty:
        return pd.DataFrame()

    df[feature_col] = (
        df[feature_col].astype(str)
        .str.replace(r"[\r\n]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    df = df[df[feature_col] != ""].copy()
    if df.empty:
        return pd.DataFrame()

    dup = df.groupby(feature_col).cumcount()
    df[feature_col] = df[feature_col] + dup.map(lambda x: f"__{x+1}" if x else "")

    long_df = df.melt(id_vars=[feature_col], value_vars=time_cols, var_name="period", value_name="value")
    long_df["yyyymm"] = long_df["period"].map(period_to_yyyymm)

    pivot = long_df.pivot_table(index="yyyymm", columns=feature_col, values="value", aggfunc="first").reset_index()
    pivot.insert(0, "company_id", company_id)
    pivot.columns = make_unique([sanitize_column(c) for c in pivot.columns])
    for c in pivot.columns:
        pivot[c] = pivot[c].astype("string")
    return pivot.dropna(axis=1, how="all")

def union_concat(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    dfs = [d.dropna(axis=1, how="all") for d in dfs if d is not None and not d.empty]
    if not dfs:
        return pd.DataFrame()
    all_cols, seen = [], set()
    for d in dfs:
        for c in d.columns:
            if c not in seen:
                seen.add(c)
                all_cols.append(c)
    norm = []
    for d in dfs:
        d = d.copy()
        for c in all_cols:
            if c not in d.columns:
                d[c] = pd.NA
        norm.append(d[all_cols])
    out = pd.concat(norm, ignore_index=True)
    out.columns = make_unique([str(c) for c in out.columns])
    return out

def chunk_df(df: pd.DataFrame, max_rows: int):
    for i in range(0, len(df), max_rows):
        yield df.iloc[i:i+max_rows].copy()

def load_df_to_bq(bq_client: bigquery.Client, table_fqdn: str, df: pd.DataFrame, write_disposition: str) -> None:
    if df.empty:
        return
    df = df.copy()
    df.columns = make_unique([sanitize_column(c) for c in df.columns])
    for c in df.columns:
        df[c] = df[c].astype("string")
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
    if write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
        job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    bq_client.load_table_from_dataframe(df, table_fqdn, job_config=job_config).result()

# ================= MAIN ===================
def main():
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    ensure_dataset(bq_client, PROJECT_ID, DATASET_ID)

    grouped: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for blob in storage_client.list_blobs(BUCKET_NAME, prefix=GCS_PREFIX):
        parsed = parse_metric(blob.name)
        if not parsed:
            continue
        mainmetric, submetric = parsed
        if mainmetric in EXCLUDE_MAINMETRICS: continue
        if submetric in EXCLUDE_SUBMETRICS: continue
        if (mainmetric, submetric) in EXCLUDE_METRIC_SUBMETRIC: continue
        if PROCESS_ONLY_CONFIGURED and (mainmetric, submetric) not in SUBHEADING_CONFIG: continue
        grouped[(mainmetric, submetric)].append(blob.name)

    if not grouped:
        log("No eligible files (or nothing configured in SUBHEADING_CONFIG).")
        return

    bucket = storage_client.bucket(BUCKET_NAME)
    first_load_for_table: Dict[str, bool] = {}
    canonical_label_by_index: Dict[Tuple[str, str, int], str] = {}

    for (mainmetric, submetric), files in sorted(grouped.items()):
        cfg = SUBHEADING_CONFIG[(mainmetric, submetric)]
        headings: List[str] = list(cfg["SUBHEADINGS"])
        anchors: List[int] = list(cfg["ANCHOR_ROWS"])
        window: int = int(cfg["SEARCH_WINDOW"])

        log(f"\n=== PROCESS: {mainmetric}.{submetric} | files={len(files)} ===")
        log(f"Headings={headings} | anchors={anchors} | window=±{window}")

        for path in sorted(files):
            try:
                excel_bytes = bucket.blob(path).download_as_bytes()
                xls = pd.ExcelFile(io.BytesIO(excel_bytes), engine="openpyxl")
                sheets = xls.sheet_names
                if SKIP_FIRST_SHEET and sheets:
                    sheets = sheets[1:]

                per_table_frames: Dict[str, List[pd.DataFrame]] = defaultdict(list)

                for sheet in sheets:
                    company_id = str(sheet).strip()
                    raw = read_sheet_raw(xls, sheet)
                    if raw is None or raw.empty:
                        continue
                    data_df = build_data_df_from_raw(raw)
                    if data_df.empty:
                        continue

                    # Find actual heading rows for THIS sheet
                    found_rows: List[Optional[int]] = []
                    for h, a in zip(headings, anchors):
                        found_rows.append(find_heading_row_near_anchor(raw, h, a, window))

                    # If any heading not found, skip this sheet (prevents wrong splits)
                    if any(r is None for r in found_rows):
                        continue

                    found_rows = [int(r) for r in found_rows]  # type: ignore
                    # Ensure increasing order
                    found_rows_sorted = sorted(found_rows)

                    # Build segments: (heading_i rows_under_this_heading)
                    for i, heading_row in enumerate(found_rows_sorted):
                        start = heading_row + 1
                        end = found_rows_sorted[i+1] - 1 if i+1 < len(found_rows_sorted) else None

                        seg_df = slice_data_df_by_excel_range(data_df, start, end)
                        if seg_df.empty:
                            continue

                        t = transform_timeseries(company_id, seg_df)
                        if t.empty:
                            continue

                        # Stable table label by heading index (locks first seen text)
                        key = (mainmetric, submetric, i+1)
                        if key not in canonical_label_by_index:
                            canonical_label_by_index[key] = headings[i]
                        label = canonical_label_by_index[key]

                        table_name = f"{sanitize_table_part(mainmetric)}_{sanitize_table_part(submetric)}_{sanitize_table_part(label)}"
                        table_fqdn = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
                        per_table_frames[table_fqdn].append(t)

                # load per derived table for this file
                for table_fqdn, frames in per_table_frames.items():
                    file_df = union_concat(frames)
                    if file_df.empty:
                        continue
                    for piece in chunk_df(file_df, MAX_ROWS_PER_LOAD):
                        if table_fqdn not in first_load_for_table:
                            first_load_for_table[table_fqdn] = True
                        mode = bigquery.WriteDisposition.WRITE_TRUNCATE if first_load_for_table[table_fqdn] else bigquery.WriteDisposition.WRITE_APPEND
                        load_df_to_bq(bq_client, table_fqdn, piece, mode)
                        log(f"  -> LOADED {table_fqdn.split('.')[-1]} file={path.split('/')[-1]} rows={len(piece):,} mode={mode}")
                        first_load_for_table[table_fqdn] = False

            except KeyboardInterrupt:
                log("Stopped by user.")
                return
            except Exception as e:
                log(f"❌ ERROR blob={path}: {e}")
                traceback.print_exc()

    log("\n✅ Done.")

if __name__ == "__main__":
    main()
