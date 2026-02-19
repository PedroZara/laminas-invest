import os
import re
import sys
import unicodedata
import logging
from io import BytesIO
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest


# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("laminas-fechamento")


# -----------------------
# Config defaults
# -----------------------
DEFAULTS = {
    "GCS_BUCKET": "laminas_fechamento",
    "GCS_PATH_PREFIX": "",  # ex: "raw/" (opcional)
    "FILE_PREFIX": "lamina_",  # base filename prefix
    "FILE_SUFFIX": "_fechamento.xlsx",  # pode deixar vazio: ""
    "SHEET": "0",  # índice (0) ou nome
    "HEADER_ROW": "0",  # linha do header do excel (0 = primeira)
    "DROP_EMPTY_COLUMNS": "true",
    "FORCE_STRING": "true",
    "STAGING_PREFIX": "_staging/laminas_fechamento/",
    "CLEANUP_STAGING": "false",
    "BQ_PROJECT": "",  # vazio => usa GOOGLE_CLOUD_PROJECT
    "BQ_DATASET": "OCUPACAO_REFINED_ZONE",
    "BQ_TABLE": "laminas_fechamento",
    "LOAD_MODE": "overwrite_partition",  # overwrite_partition | append | overwrite_table
    "DRY_RUN": "false",
    "MAX_FILES": "0",  # 0 = sem limite
    "FAIL_ON_FILE_ERROR": "false",  # true => qualquer erro em arquivo aborta job
    "LOG_COLUMNS": "false",  # true => loga lista de colunas após normalização
}


PT_MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,  # março -> marco (sem acento)
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


# -----------------------
# Utils
# -----------------------
def env(name: str) -> str:
    return os.getenv(name, DEFAULTS[name])


def env_bool(name: str) -> bool:
    return env(name).strip().lower() == "true"


def env_int(name: str) -> int:
    try:
        return int(env(name).strip())
    except Exception:
        return 0


def strip_accents(s: str) -> str:
    s = "" if s is None else str(s)
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def normalize_column_name(raw: str) -> str:
    s = strip_accents(raw).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return ""
    if re.match(r"^[0-9]", s):
        s = f"col_{s}"
    return s[:300]


def dedupe_columns(cols: List[str]) -> List[str]:
    out: List[str] = []
    seen: Dict[str, int] = {}
    empty_count = 0

    for c in cols:
        base = c or ""
        if not base:
            empty_count += 1
            base = f"col_{empty_count}"

        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")

    return out


def normalize_columns(original_cols: List[str]) -> List[str]:
    normalized = [normalize_column_name(c) for c in original_cols]
    normalized = dedupe_columns(normalized)

    fixed: List[str] = []
    for c in normalized:
        if not c:
            fixed.append("col")
        elif not re.match(r"^[a-z_][a-z0-9_]*$", c):
            c2 = re.sub(r"[^a-z0-9_]+", "_", c)
            if re.match(r"^[0-9]", c2):
                c2 = f"col_{c2}"
            fixed.append(c2[:300])
        else:
            fixed.append(c)

    return dedupe_columns(fixed)


def parse_year_month_from_filename(filename: str) -> Tuple[int, int]:
    """
    Espera algo como: lamina_2024_abril_fechamento.xlsx
    """
    name = strip_accents(filename.lower())

    m_year = re.search(r"(19\d{2}|20\d{2})", name)
    if not m_year:
        raise ValueError(f"Não achei ano no filename: {filename}")
    year = int(m_year.group(1))

    for k, v in PT_MONTHS.items():
        if re.search(rf"\b{k}\b", name):
            return year, v

    m_num = re.search(r"(?:_|-)(0?[1-9]|1[0-2])(?:_|-)", name)
    if m_num:
        return year, int(m_num.group(1))

    raise ValueError(f"Não achei mês (nome pt ou número) no filename: {filename}")


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    def is_empty_col(s: pd.Series) -> bool:
        if s.isna().all():
            return True
        if s.dtype == object:
            return s.fillna("").astype(str).str.strip().eq("").all()
        return False

    keep = [c for c in df.columns if not is_empty_col(df[c])]
    return df[keep]


def read_xlsx_to_df(xlsx_bytes: bytes, sheet: str, header_row: int) -> pd.DataFrame:
    sheet_arg = int(sheet) if str(sheet).isdigit() else sheet
    bio = BytesIO(xlsx_bytes)
    return pd.read_excel(
        bio, sheet_name=sheet_arg, header=header_row, engine="openpyxl"
    )


def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    out = BytesIO()
    pq.write_table(table, out, compression="snappy")
    return out.getvalue()


# -----------------------
# BigQuery helpers
# -----------------------
def resolve_project() -> str:
    p = env("BQ_PROJECT").strip()
    if p:
        return p
    p = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    if p:
        return p
    raise RuntimeError("Defina BQ_PROJECT ou GOOGLE_CLOUD_PROJECT.")


def ensure_dataset_exists(
    bq: bigquery.Client, project: str, dataset: str
) -> bigquery.Dataset:
    ds_id = f"{project}.{dataset}"
    try:
        return bq.get_dataset(ds_id)
    except NotFound:
        raise RuntimeError(f"Dataset não encontrado: {ds_id}")


def ensure_partitioned_table(
    bq: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
) -> bigquery.Table:
    table_id = f"{project}.{dataset}.{table}"
    try:
        return bq.get_table(table_id)
    except NotFound:
        # schema mínimo (load job adiciona colunas do parquet via ALLOW_FIELD_ADDITION)
        schema = [
            bigquery.SchemaField("ano", "INT64"),
            bigquery.SchemaField("mes", "INT64"),
            bigquery.SchemaField("competencia", "DATE"),
            bigquery.SchemaField("arquivo_origem", "STRING"),
            bigquery.SchemaField("gcs_uri", "STRING"),
            bigquery.SchemaField("data_ingestao", "TIMESTAMP"),
        ]
        tbl = bigquery.Table(table_id, schema=schema)
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="competencia",
        )
        created = bq.create_table(tbl)
        log.info(
            "Tabela criada: %s (particionada por competencia)", created.full_table_id
        )
        return created


def load_parquet_to_bq(
    bq: bigquery.Client,
    source_uri: str,
    destination_table: str,
    write_disposition: str,
):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
    )
    job = bq.load_table_from_uri(source_uri, destination_table, job_config=job_config)
    job.result()
    log.info("Load OK: %s <= %s", destination_table, source_uri)


# -----------------------
# Location guard (bucket vs dataset)
# -----------------------
def check_location_compat(
    storage_client: storage.Client,
    bq_client: bigquery.Client,
    bucket_name: str,
    dataset_obj: bigquery.Dataset,
):
    bucket = storage_client.get_bucket(bucket_name)
    bucket_loc = (bucket.location or "").upper()
    dataset_loc = (dataset_obj.location or "").upper()

    # BigQuery aceita US/EU multi-region; bucket pode ser US/EU ou região específica.
    # Se dataset for US, bucket deve ser US; se dataset for região, bucket deve ser igual.
    if dataset_loc in ("US", "EU"):
        if bucket_loc != dataset_loc:
            raise RuntimeError(
                f"Incompatibilidade de location: dataset={dataset_loc} vs bucket={bucket_loc}. "
                f"Ajuste para coincidir."
            )
    else:
        # dataset é região específica
        if bucket_loc != dataset_loc:
            raise RuntimeError(
                f"Incompatibilidade de location: dataset={dataset_loc} vs bucket={bucket_loc}. "
                f"Ajuste para coincidir."
            )


# -----------------------
# GCS listing
# -----------------------
def list_target_blobs(
    storage_client: storage.Client, bucket_name: str
) -> List[storage.Blob]:
    gcs_path_prefix = env("GCS_PATH_PREFIX").lstrip("/")
    file_prefix = env("FILE_PREFIX")
    file_suffix = env("FILE_SUFFIX")

    blobs = storage_client.list_blobs(bucket_name, prefix=gcs_path_prefix or None)

    selected: List[storage.Blob] = []
    for blob in blobs:
        base = blob.name.split("/")[-1]
        if base.startswith("~$"):
            continue
        if not base.endswith(".xlsx"):
            continue
        if file_prefix and not base.startswith(file_prefix):
            continue
        if file_suffix and not base.endswith(file_suffix):
            continue
        selected.append(blob)

    selected.sort(key=lambda b: b.name)
    max_files = env_int("MAX_FILES")
    if max_files > 0:
        selected = selected[:max_files]

    return selected


# -----------------------
# Main
# -----------------------
def main():
    dry_run = env_bool("DRY_RUN")
    drop_empty = env_bool("DROP_EMPTY_COLUMNS")
    force_string = env_bool("FORCE_STRING")
    cleanup_staging = env_bool("CLEANUP_STAGING")
    fail_on_file_error = env_bool("FAIL_ON_FILE_ERROR")
    log_columns = env_bool("LOG_COLUMNS")

    bucket_name = env("GCS_BUCKET")
    staging_prefix = env("STAGING_PREFIX").rstrip("/") + "/"

    dataset = env("BQ_DATASET")
    table = env("BQ_TABLE")
    load_mode = env("LOAD_MODE").strip().lower()

    sheet = env("SHEET").strip()
    header_row = env_int("HEADER_ROW")

    project = resolve_project()

    log.info("Projeto: %s", project)
    log.info(
        "Bucket: %s | GCS_PATH_PREFIX=%s | FILE_PREFIX=%s | FILE_SUFFIX=%s",
        bucket_name,
        env("GCS_PATH_PREFIX"),
        env("FILE_PREFIX"),
        env("FILE_SUFFIX"),
    )
    log.info("BQ destino: %s.%s.%s | load_mode=%s", project, dataset, table, load_mode)
    log.info(
        "sheet=%s | header_row=%s | force_string=%s | drop_empty=%s | dry_run=%s",
        sheet,
        header_row,
        force_string,
        drop_empty,
        dry_run,
    )

    storage_client = storage.Client(project=project)
    bq_client = bigquery.Client(project=project)

    ds_obj = ensure_dataset_exists(bq_client, project, dataset)
    check_location_compat(storage_client, bq_client, bucket_name, ds_obj)

    if load_mode == "overwrite_table":
        ensure_partitioned_table(bq_client, project, dataset, table)

    blobs = list_target_blobs(storage_client, bucket_name)
    if not blobs:
        log.warning("Nenhum arquivo XLSX encontrado com o padrão informado.")
        return

    log.info("Arquivos selecionados: %d", len(blobs))

    bucket = storage_client.bucket(bucket_name)
    base_table_id = f"{project}.{dataset}.{table}"

    processed = 0
    failed = 0

    for idx, blob in enumerate(blobs, start=1):
        filename = blob.name.split("/")[-1]
        gcs_source_uri = f"gs://{bucket_name}/{blob.name}"
        log.info("(%d/%d) Processando: %s", idx, len(blobs), gcs_source_uri)

        try:
            year, month = parse_year_month_from_filename(filename)
            competencia_dt = date(year, month, 1)
            partition_id = competencia_dt.strftime("%Y%m%d")

            # download
            xlsx_bytes = blob.download_as_bytes()

            # read excel
            df = read_xlsx_to_df(xlsx_bytes, sheet=sheet, header_row=header_row)
            if df is None or df.empty:
                log.warning("Sem dados: %s", filename)
                continue

            if drop_empty:
                df = drop_empty_columns(df)

            # normalize columns
            original_cols = [str(c) for c in df.columns.tolist()]
            df.columns = normalize_columns(original_cols)

            if log_columns:
                log.info("Colunas (%s): %s", filename, df.columns.tolist())

            # force string (robusto p/ variação de tipos no Excel)
            if force_string:
                for c in df.columns:
                    df[c] = df[c].map(lambda x: None if pd.isna(x) else str(x).strip())

            # metadata
            df["ano"] = int(year)
            df["mes"] = int(month)
            df["competencia"] = pd.to_datetime(competencia_dt)  # DATE no BQ
            df["arquivo_origem"] = filename
            df["gcs_uri"] = gcs_source_uri
            df["data_ingestao"] = pd.Timestamp(datetime.now(timezone.utc))

            if dry_run:
                log.info(
                    "[DRY_RUN] Linhas=%d | colunas=%d | competencia=%s",
                    len(df),
                    len(df.columns),
                    competencia_dt,
                )
                processed += 1
                continue

            # parquet bytes
            parquet_bytes = dataframe_to_parquet_bytes(df)

            safe_base = re.sub(r"[^a-zA-Z0-9_.-]+", "_", filename).replace(
                ".xlsx", ".parquet"
            )
            staging_path = f"{staging_prefix}ano={year}/mes={month:02d}/{safe_base}"
            staging_uri = f"gs://{bucket_name}/{staging_path}"

            # upload parquet
            staging_blob = bucket.blob(staging_path)
            staging_blob.upload_from_string(
                parquet_bytes, content_type="application/octet-stream"
            )
            log.info("Staging Parquet: %s", staging_uri)

            # load to BQ
            if load_mode == "overwrite_partition":
                destination = f"{base_table_id}${partition_id}"
                load_parquet_to_bq(
                    bq_client,
                    staging_uri,
                    destination,
                    bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
            elif load_mode == "append":
                load_parquet_to_bq(
                    bq_client,
                    staging_uri,
                    base_table_id,
                    bigquery.WriteDisposition.WRITE_APPEND,
                )
            elif load_mode == "overwrite_table":
                load_parquet_to_bq(
                    bq_client,
                    staging_uri,
                    base_table_id,
                    bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
            else:
                raise ValueError(
                    "LOAD_MODE inválido. Use overwrite_partition|append|overwrite_table"
                )

            if cleanup_staging:
                staging_blob.delete()
                log.info("Staging removido: %s", staging_uri)

            processed += 1

        except (BadRequest, ValueError, RuntimeError) as e:
            failed += 1
            log.error("Falha em %s: %s", filename, e)
            if fail_on_file_error:
                raise
        except Exception as e:
            failed += 1
            log.exception("Erro inesperado em %s: %s", filename, e)
            if fail_on_file_error:
                raise

    log.info("Finalizado. OK=%d | Falhas=%d | Total=%d", processed, failed, len(blobs))

    if failed > 0 and fail_on_file_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
