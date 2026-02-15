import io
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import storage
from google.cloud import bigquery


# =========================
# CONFIG (EDIT THESE)
# =========================
GCP_PROJECT_ID = "gcp-sfpl-etl-demo-project6"
BQ_DATASET_ID = "Shravas_Lab"

GCS_BUCKET = "shravas-lab"      # <-- your bucket
GCS_PREFIX = "DATA/"          # folder prefix inside bucket

HEADER_ROW_1BASED = 6
HEADER_ROW_0BASED = HEADER_ROW_1BASED - 1
SKIP_FIRST_SHEET = True

# If file-level concat becomes too large, split into multiple loads (still "file at a time")
MAX_ROWS_PER_LOAD = 500_000   # tune: 200k to 2M depending on RAM

# Switch OFF entire main metrics (DATA/<MainMetric>/...)
EXCLUDE_MAINMETRICS = {
    "RatiosMetrics",
     "GeneralMetrics",
     "StockMetrics",
    # "FinancialMetrics",
    "EquityMetrics",
}

# Exclude submetrics (filename WITHOUT .xlsx)
EXCLUDE_SUBMETRICS = {
     'BalanceSheet',
'CashFlow',
'CSR',
'ExpenditureR&D',
'FinancialHighlights',
'Forex',
'FundFlow',
'Investments',
#'MaturityPattern',
'OperationalData',
'ProfitAndLoss',
'RPD',
'RPT',
'SectorExposure',
'SegmentFinance',
'Subsidiaries',
}

# Optional: exclude only specific (MainMetric, Submetric)
EXCLUDE_METRIC_SUBMETRIC = {
    # ("FinancialMetrics", "ProfitAndLoss"),
}

# Metadata columns (all STRING)
ADD_SOURCE_PATH =False
ADD_SOURCE_FILE = False
ADD_MAINMETRIC = False
ADD_SUBMETRIC = False


# =========================
# HELPERS
# =========================
def sanitize_identifier(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_]", "_", str(s))
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "unknown"
    if re.match(r"^\d", s):
        s = f"t_{s}"
    return s


def parse_mainmetric_submetric(blob_name: str) -> Optional[Tuple[str, str]]:
    """
    Expect: DATA/<MainMetric>/<A-H>/<Submetric>.xlsx
    Example: DATA/EquityMetrics/A/BonusHistory.xlsx
    """
    path = blob_name.replace("\\", "/").strip("/")
    parts = path.split("/")
    if len(parts) < 4 or parts[0] != "DATA":
        return None

    filename = parts[-1]
    if not filename.lower().endswith(".xlsx"):
        return None

    mainmetric = parts[1]
    submetric = re.sub(r"\.xlsx$", "", filename, flags=re.IGNORECASE)
    return mainmetric, submetric


def ensure_dataset(bq: bigquery.Client, project_id: str, dataset_id: str) -> None:
    ds_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    try:
        bq.get_dataset(ds_ref)
    except Exception:
        # If your dataset is NOT asia-south1, change this or create dataset manually in correct location.
        ds_ref.location = "asia-south1"
        bq.create_dataset(ds_ref, exists_ok=True)


def force_all_to_string(df: pd.DataFrame) -> pd.DataFrame:
    # keep NULLs as <NA>, avoid dtype conflicts across companies
    for c in df.columns:
        df[c] = df[c].astype("string")
    return df


def read_company_sheets(excel_bytes: bytes) -> List[Tuple[str, pd.DataFrame]]:
    """
    Reads all company sheets (skipping first sheet).
    Uses header row 6 (1-based) when available.
    If a sheet doesn't have enough rows to reach header row, it is skipped safely.
    """
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))
    sheet_names = xls.sheet_names

    if SKIP_FIRST_SHEET and sheet_names:
        sheet_names = sheet_names[1:]

    out: List[Tuple[str, pd.DataFrame]] = []

    for sheet in sheet_names:
        company_id = str(sheet).strip()

        # --- quick pre-check: does this sheet have enough rows to even reach header row?
        try:
            preview = pd.read_excel(
                xls,
                sheet_name=sheet,
                header=None,
                nrows=HEADER_ROW_0BASED + 1,  # need at least rows up to header
                dtype=object,
            )
            if preview.shape[0] <= HEADER_ROW_0BASED:
                # Not enough rows to contain the header row
                # (example: only 2 lines in sheet)
                continue
        except Exception:
            # Any weird sheet parsing issue -> skip sheet
            continue

        # --- now read real data
        try:
            df = pd.read_excel(
                xls,
                sheet_name=sheet,
                header=HEADER_ROW_0BASED,
                dtype=object,
            )
        except Exception:
            # If pandas still complains, skip the sheet
            continue

        # Drop empty rows/cols
        df = df.dropna(how="all")
        df = df.dropna(axis=1, how="all")
        if df.empty:
            continue

        # Clean header names
        df.columns = [str(c).strip() for c in df.columns]

        # Add company_id
        df.insert(0, "company_id", company_id)

        out.append((company_id, df))

    return out



def load_df_to_bq(bq: bigquery.Client, table_fqdn: str, df: pd.DataFrame, write_disposition: str) -> None:
    if df.empty:
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )

    # schema_update_options only allowed with WRITE_APPEND (unless partitioned)
    if write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
        job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    job = bq.load_table_from_dataframe(df, table_fqdn, job_config=job_config)
    job.result()


def chunk_df(df: pd.DataFrame, max_rows: int):
    for start in range(0, len(df), max_rows):
        yield df.iloc[start:start + max_rows].copy()


# =========================
# MAIN
# =========================
def main():
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    ensure_dataset(bq_client, GCP_PROJECT_ID, BQ_DATASET_ID)

    grouped: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    # Discover and group files by (mainmetric, submetric)
    for blob in storage_client.list_blobs(GCS_BUCKET, prefix=GCS_PREFIX):
        if not blob.name.lower().endswith(".xlsx"):
            continue

        parsed = parse_mainmetric_submetric(blob.name)
        if not parsed:
            continue

        mainmetric, submetric = parsed

        if mainmetric in EXCLUDE_MAINMETRICS:
            continue
        if submetric in EXCLUDE_SUBMETRICS or (mainmetric, submetric) in EXCLUDE_METRIC_SUBMETRIC:
            continue

        grouped[(mainmetric, submetric)].append(blob.name)

    if not grouped:
        print("No eligible .xlsx files found under prefix (or everything excluded).")
        return

    bucket = storage_client.bucket(GCS_BUCKET)

    created_tables = []
    total_tables = 0
    total_files_loaded = 0

    for (mainmetric, submetric), blob_names in sorted(grouped.items()):
        total_tables += 1

        table_name = f"{sanitize_identifier(mainmetric)}_{sanitize_identifier(submetric)}"
        table_fqdn = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
        created_tables.append(table_fqdn)

        print(f"\n=== FULL REFRESH (PER-FILE LOAD): {table_fqdn} ===")
        print(f"Files: {len(blob_names)}")

        first_load_for_table = True

        for blob_name in sorted(blob_names):
            gcs_path = f"gs://{GCS_BUCKET}/{blob_name}"
            filename_only = blob_name.split("/")[-1]

            excel_bytes = bucket.blob(blob_name).download_as_bytes()
            company_sheets = read_company_sheets(excel_bytes)

            if not company_sheets:
                print(f"  - {gcs_path} -> no usable company sheets (skipped).")
                continue

            # Build ONE dataframe for the entire file (all company sheets)
            dfs = []
            for company_id, df in company_sheets:
                # metadata
                if ADD_SOURCE_PATH:
                    df["source_path"] = gcs_path
                if ADD_SOURCE_FILE:
                    df["source_file"] = filename_only
                if ADD_MAINMETRIC:
                    df["main_metric"] = mainmetric
                if ADD_SUBMETRIC:
                    df["sub_metric"] = submetric

                # sanitize columns + force string
                df.columns = [sanitize_identifier(c) for c in df.columns]
                df = force_all_to_string(df)
                dfs.append(df)

            file_df = pd.concat(dfs, ignore_index=True)

            # Load per file, but chunk if too large
            first_chunk_of_this_file = True
            for piece in chunk_df(file_df, MAX_ROWS_PER_LOAD):
                if first_load_for_table:
                    write_mode = bigquery.WriteDisposition.WRITE_TRUNCATE
                else:
                    write_mode = bigquery.WriteDisposition.WRITE_APPEND

                # After first piece ever loaded into table, subsequent pieces append
                load_df_to_bq(bq_client, table_fqdn, piece, write_mode)

                rows = len(piece)
                total_files_loaded += 1 if first_chunk_of_this_file else 0
                print(f"  -> LOADED file={filename_only} rows={rows:,} mode={write_mode}")

                first_load_for_table = False
                first_chunk_of_this_file = False

    print("\nDONE")
    print(f"Tables refreshed: {total_tables}")
    print(f"Files loaded (at least 1 chunk each): {total_files_loaded}")

    # Print where to look in BigQuery UI + list tables via API (helps when UI is confusing)
    print("\nWhere your tables are:")
    print(f"  Project : {GCP_PROJECT_ID}")
    print(f"  Dataset : {BQ_DATASET_ID}")
    print("  Example : <Project>.<Dataset>.<Table>")

    print("\nTables this run (first 30):")
    for t in created_tables[:30]:
        print(" ", t)

    # Extra: list tables that exist in dataset (authoritative)
    try:
        print("\nBigQuery says these tables exist in the dataset (first 200):")
        ds_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET_ID)
        for i, table in enumerate(bq_client.list_tables(ds_ref)):
            if i >= 200:
                print("  ... (truncated)")
                break
            print(" ", f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{table.table_id}")
    except Exception as e:
        print("\nCould not list tables from dataset (permissions/UI issue). Error:", str(e))


if __name__ == "__main__":
    main()
