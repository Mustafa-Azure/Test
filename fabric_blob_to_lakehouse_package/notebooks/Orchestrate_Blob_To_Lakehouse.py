# Orchestrate_Blob_To_Lakehouse.py
# Fabric PySpark notebook: copies files recursively from multiple Blob sources into Lakehouse Files,
# then logs metadata to a Delta table.

from pyspark.sql import functions as F, types as T
from datetime import datetime

try:
    from notebookutils import mssparkutils
except Exception as e:
    mssparkutils = None

LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"

def enable_managed_identity_for_abfss(account_name: str):
    """Configure Spark to use Managed Identity for the given storage account."""
    if not account_name:
        return
    host = f"{account_name}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{host}",
                   "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")

def abfss_uri(account_name: str, container: str, prefix: str = "") -> str:
    prefix = (prefix or "").strip("/")
    base = f"abfss://{container}@{account_name}.dfs.core.windows.net"
    return f"{base}/{prefix}" if prefix else f"{base}/"

def resolve_dest_folder(target_path: str) -> str:
    """
    Accepts:
      - 'lakehouse://Files/...'
      - '/lakehouse/default/Files/...'
      - 'Files/...'
      - '...'(relative under Files)
    Returns absolute Lakehouse Files path.
    """
    if not target_path:
        raise ValueError("target_path is required in storage_config")
    p = target_path.strip()

    if p.startswith("lakehouse://"):
        p = p.replace("lakehouse://", "")
    if p.startswith("/lakehouse/"):
        return p
    if p.startswith("Files/"):
        return f"{LAKEHOUSE_FILES_ROOT}/{p[len('Files/'):]}"

    # default: relative under Files
    return f"{LAKEHOUSE_FILES_ROOT}/{p.lstrip('/')}"

def list_files_recursive(root_path: str):
    """
    Use mssparkutils.fs.ls to recursively list files under a Lakehouse Files path.
    Returns a list of dicts with path, name, size, last_modified.
    """
    if mssparkutils is None:
        raise RuntimeError("mssparkutils is unavailable in this runtime. Please run in Fabric or use a Pipeline Copy activity.")

    stack = [root_path.rstrip("/")]
    results = []

    while stack:
        current = stack.pop()
        try:
            entries = mssparkutils.fs.ls(current)
        except Exception as e:
            print(f"Warn: cannot list {current}: {e}")
            continue

        for it in entries:
            # it has .name (full path), .isDir, .size, .modificationTime
            if getattr(it, "isDir", False):
                stack.append(it.name.rstrip("/"))
            else:
                results.append({
                    "path": it.name,
                    "name": it.name.rstrip("/").split("/")[-1],
                    "size": int(getattr(it, "size", 0) or 0),
                    "last_modified": getattr(it, "modificationTime", None)
                })
    return results

def main():
    # 1) Read config rows
    cfg = spark.table("storage_config")
    rows = [r.asDict() for r in cfg.collect()]
    if not rows:
        print("No rows found in storage_config. Add sources and re-run.")
        return

    all_log_rows = []
    now_ts = datetime.utcnow()

    for src in rows:
        account = (src.get("account_name") or "").strip().lower()
        container = (src.get("container_name") or "").strip().lower()
        prefix = (src.get("directory_prefix") or "").strip()
        target_path = src.get("target_path")

        if not account or not container:
            print(f"Skipping row with missing account/container: {src}")
            continue

        enable_managed_identity_for_abfss(account)

        source_uri = abfss_uri(account, container, prefix)
        dest_root = resolve_dest_folder(target_path)

        print(f"\n=== Ingest: {account}/{container} prefix='{prefix}' ===")
        print(f"Source: {source_uri}")
        print(f"Dest  : {dest_root}")

        if mssparkutils is None:
            raise RuntimeError("mssparkutils is required for file copy in this notebook runtime.")

        # 2) Copy everything recursively
        try:
            mssparkutils.fs.mkdirs(dest_root)  # ensure root
        except Exception:
            pass

        try:
            mssparkutils.fs.cp(source_uri, dest_root, recurse=True)
            status = "ingested"
            err = None
        except Exception as e:
            status = "error"
            err = str(e)
            print(f"Copy error: {e}")

        # 3) Build metadata rows from the destination view (post-copy)
        try:
            files = list_files_recursive(dest_root)
            for f in files:
                rel = f["path"].replace(dest_root.rstrip('/') + '/', "")
                all_log_rows.append((
                    account,
                    container,
                    prefix,
                    source_uri,
                    dest_root,
                    rel,
                    f["name"],
                    int(f["size"]),
                    f["last_modified"],
                    status if status != "error" else "error",
                    err,
                    now_ts
                ))
        except Exception as e:
            all_log_rows.append((
                account, container, prefix, source_uri, dest_root,
                None, None, None, None, "error", f"metadata: {e}", now_ts
            ))

    # 4) Write log table
    if all_log_rows:
        schema = T.StructType([
            T.StructField("source_account",   T.StringType()),
            T.StructField("source_container", T.StringType()),
            T.StructField("source_prefix",    T.StringType()),
            T.StructField("source_uri",       T.StringType()),
            T.StructField("dest_root",        T.StringType()),
            T.StructField("relative_path",    T.StringType()),
            T.StructField("file_name",        T.StringType()),
            T.StructField("file_size",        T.LongType()),
            T.StructField("last_modified",    T.TimestampType()),
            T.StructField("status",           T.StringType()),
            T.StructField("error_message",    T.StringType()),
            T.StructField("ingestion_time",   T.TimestampType()),
        ])
        df = spark.createDataFrame(all_log_rows, schema=schema)
        df.write.mode("append").format("delta").saveAsTable("file_ingestion_log")
        print(f"Wrote {df.count()} log rows to file_ingestion_log")
    else:
        print("No log rows generated.")

if __name__ == "__main__":
    main()