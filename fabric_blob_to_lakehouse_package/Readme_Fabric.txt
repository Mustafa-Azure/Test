from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
import json

# Attempt to use mssparkutils for file copy (binary). Available in Fabric runtime.
try:
    from notebookutils import mssparkutils
except Exception:
    mssparkutils = None

LAKEHOUSE_FILES_ROOT = "/lakehouse/default/Files"  # Adjust if not default

# ---- Managed Identity ABFS setup ----
def enable_msi_for_account(account_name: str):
    if not account_name:
        return
    host = f"{account_name}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{host}", \
                   "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
    # Optional: if your environment needs explicit MSI endpoint/tenant, uncomment and set
    # spark.conf.set(f"fs.azure.account.oauth2.msi.tenant", "<TENANT-ID>")
    # spark.conf.set(f"fs.azure.account.oauth2.msi.endpoint", "http://169.254.169.254/metadata/identity/oauth2/token")


def abfss_path(account_name: str, container: str, prefix: str = "") -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"abfss://{container}@{account_name}.dfs.core.windows.net/{prefix}"
    return f"abfss://{container}@{account_name}.dfs.core.windows.net/"


def list_candidate_files_df(src_df: 'DataFrame'):
    """Return DF of files with path/size/mtime/etag. Uses binaryFile for robust listing including nested folders."""
    return (spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .load(src_df))


def already_ingested_paths(source_id: str):
    if not spark._jsparkSession.catalog().tableExists("file_ingestion_log"):
        return set()
    df = spark.table("file_ingestion_log").where(F.col("source_id") == source_id)
    return set(r[0] for r in df.select("path").distinct().collect())


def upsert_ingestion_log(rows):
    if not rows:
        return
    schema = T.StructType([
        T.StructField("source_id", T.StringType()),
        T.StructField("path", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("size", T.LongType()),
        T.StructField("etag", T.StringType()),
        T.StructField("last_modified", T.TimestampType()),
        T.StructField("outcome", T.StringType()),
        T.StructField("error_message", T.StringType()),
        T.StructField("ingestion_time", T.TimestampType()),
    ])
    log_df = spark.createDataFrame(rows, schema=schema)
    (log_df.write.mode("append").format("delta").saveAsTable("file_ingestion_log"))
	

config = spark.table("storage_config").collect()
print(f"Loaded {len(config)} source rows from storage_config")


log_rows = []
now_ts = datetime.utcnow()

for row in config:
    src = row.asDict()
    source_id = src.get("source_id")
    print(f"\n=== Processing {source_id}: {src.get('description')} ===")

    use_shortcuts = bool(src.get("use_shortcuts"))
    shortcut_path  = src.get("shortcut_path") or ""
    account_name   = src.get("account_name")
    container_name = src.get("container_name")
    prefix         = src.get("directory_prefix") or ""
    fmt            = (src.get("file_format") or "binary").lower()
    target_table   = src.get("target_table")
    target_folder  = src.get("target_folder") or f"Files/bronze/{source_id}"
    schema_mode    = (src.get("schema_mode") or "infer").lower()
    schema_json    = src.get("schema_json")

    # Resolve source URI
    if use_shortcuts:
        # Read from Lakehouse shortcut path
        base_path = f"{LAKEHOUSE_FILES_ROOT}/{shortcut_path.strip('/')}/"
        if prefix:
            src_uri = base_path + prefix.strip('/') + "/"
        else:
            src_uri = base_path
    else:
        enable_msi_for_account(account_name)
        src_uri = abfss_path(account_name, container_name, prefix)

    print(f"Source URI: {src_uri}")

    # List candidate files recursively
    try:
        files_df = (spark.read.format("binaryFile")
                    .option("recursiveFileLookup", "true")
                    .load(src_uri)
                    .select(F.col("path"), F.col("modificationTime").alias("last_modified"),
                            F.col("length").alias("size"), F.col("content")))
        # Derive name and a best-effort ETag surrogate (hash of content)
        files_df = files_df.withColumn("name", F.regexp_extract(F.col("path"), r"([^/]+)$", 1))
        files_df = files_df.withColumn("etag", F.sha2(F.col("content"), 256))
        files_df = files_df.drop("content")
    except Exception as e:
        print(f"Error listing: {e}")
        continue

    # Incremental: left-anti against existing log with same source_id & path & etag
    if spark._jsparkSession.catalog().tableExists("file_ingestion_log"):
        existing = (spark.table("file_ingestion_log")
                    .where(F.col("source_id") == source_id)
                    .select("path", "etag"))
        to_process = (files_df.join(existing, on=["path", "etag"], how="left_anti"))
    else:
        to_process = files_df

    count = to_process.count()
    print(f"{count} file(s) to process for {source_id}")

    # Branch by format
    if fmt in ("csv", "json", "parquet"):
        # Build a reader on the source URI, not on the binaryFile DF
        reader = spark.read.option("recursiveFileLookup", "true")
        if fmt == "csv":
            if src.get("csv_header"): reader = reader.option("header", "true")
            if src.get("csv_delimiter"): reader = reader.option("delimiter", src.get("csv_delimiter"))
            if schema_mode == "infer":
                reader = reader.option("inferSchema", "true")
        elif fmt == "json":
            if schema_mode == "infer":
                reader = reader.option("inferSchema", "true")
            reader = reader.option("multiLine", "true")
        elif fmt == "parquet":
            pass  # Parquet carries schema

        if schema_mode == "provided" and schema_json:
            spark_schema = T.StructType.fromJson(json.loads(schema_json))
            df = reader.format(fmt).schema(spark_schema).load(src_uri)
        else:
            df = reader.format(fmt).load(src_uri)

        # Optional: add provenance columns
        df = df.withColumn("_ingest_source_id", F.lit(source_id)) \
               .withColumn("_ingest_time", F.current_timestamp())

        # Write to Delta table
        if not target_table:
            raise ValueError(f"target_table is required for structured format")
=================================================================================			

-- Configuration table listing sources to ingest
CREATE TABLE IF NOT EXISTS storage_config (
  source_id STRING,                       -- unique ID per source row
  description STRING,
  use_shortcuts BOOLEAN,                  -- true to use Lakehouse Shortcuts
  shortcut_path STRING,                   -- e.g. 'Files/Shortcuts/mycontainer' (when use_shortcuts=true)
  account_name STRING,                    -- e.g. mystorageacct
  container_name STRING,                  -- e.g. mycontainer
  directory_prefix STRING,                -- optional subfolder within the container ('' for all)
  file_format STRING,                     -- csv | json | parquet | binary
  csv_header BOOLEAN,                     -- for CSV
  csv_delimiter STRING,                   -- for CSV
  target_table STRING,                    -- Delta table name for structured; ignored for binary
  target_folder STRING,                   -- Files subfolder for binary, e.g. 'Files/bronze/media/mycontainer'
  schema_mode STRING,                     -- infer | provided (if provided, see schema_json)
  schema_json STRING                      -- optional JSON schema for structured
);

-- Ingestion log to track incrementality and auditing
CREATE TABLE IF NOT EXISTS file_ingestion_log (
  source_id STRING,
  path STRING,                            -- full path to the file in source
  name STRING,
  size BIGINT,
  etag STRING,
  last_modified TIMESTAMP,
  outcome STRING,                         -- ingested | skipped | error
  error_message STRING,
  ingestion_time TIMESTAMP
) USING DELTA;

========================================================================================

INSERT INTO storage_config VALUES
('src01','Main media bin',false,NULL,'mystorageacct1','media','', 'binary', NULL, NULL, NULL, 'Files/bronze/media/mystorageacct1', 'infer', NULL),
('src02','Telemetry CSVs', false,NULL,'mystorageacct2','telemetry','2025/','csv', true, ',', 'telemetry_bronze', NULL, 'infer', NULL),
('src03','JSON events via Shortcut', true,'Files/Shortcuts/events', NULL, NULL, NULL, 'json', NULL,NULL, 'events_bronze', NULL, 'infer', NULL);

CREATE TABLE storage_config (
    account_name STRING,
    container_name STRING,
    account_url STRING,
    auth_method STRING,
    file_format STRING,
    target_path STRING
);

INSERT INTO storage_config VALUES
('VendorMak', 'Wavedata', 'https://vendormak.blob.core.windows.net', 'managed_identity', 'binary', 'lakehouse://Files/VendorMak/Wavedata'),
('VendorEli', 'Wavedata', 'https://vendoreli.blob.core.windows.net', 'managed_identity', 'binary', 'lakehouse://Files/VendorEli/Wavedata');
==============================================================================================================

from pyspark.sql import SparkSession

# Parameters passed from pipeline
account_url = dbutils.widgets.get("account_url")
container_name = dbutils.widgets.get("container_name")
target_path = dbutils.widgets.get("target_path")

# Read all files recursively from nested folders
df = spark.read.format("binaryFile") \
    .option("recursiveFileLookup", "true") \
    .load(f"{account_url}/{container_name}")

# Save to Lakehouse path
df.write.mode("append").format("delta").save(target_path)

===============================================================================================

from notebookutils import mssparkutils
from pyspark.sql.functions import input_file_name, col
from datetime import datetime

# ---- Pipeline parameters ----
account_name   = dbutils.widgets.get("account_name")    # VendorMak / VendorEli
account_url    = dbutils.widgets.get("account_url")     # https://vendormak.blob.core.windows.net
container_name = dbutils.widgets.get("container_name")  # Wavedata
target_path    = dbutils.widgets.get("target_path")     # lakehouse://Files/VendorMak/Wavedata

# ---- Paths ----
abfss_url = f"abfss://{container_name}@{account_name}.blob.core.windows.net/"
lakehouse_files_path = target_path.replace("lakehouse://", "Files/")  # Files/VendorMak/Wavedata

print(f"Copying from {abfss_url} to {lakehouse_files_path} ...")

# ---- Step 1: Copy files recursively into Lakehouse Files ----
mssparkutils.fs.cp(abfss_url, lakehouse_files_path, recurse=True)

print("File copy completed.")

# ---- Step 2: Read files back to collect metadata ----
df_meta = spark.read.format("binaryFile") \
    .option("recursiveFileLookup", "true") \
    .load(lakehouse_files_path) \
    .withColumn("source_account", col(lit(account_name))) \
    .withColumn("source_container", col(lit(container_name))) \
    .withColumn("ingestion_timestamp", col(lit(datetime.utcnow().isoformat())))

# ---- Step 3: Save metadata to Delta table ----
delta_table_path = "Tables/file_ingestion_log"

df_meta.select(
    col("path").alias("file_path"),
    col("modificationTime").alias("last_modified"),
    col("length").alias("file_size"),
    "source_account",
    "source_container",
    "ingestion_timestamp"
).write.mode("append").format("delta").save(delta_table_path)

print("Metadata logging completed.")

=====================================================================================================================

{
    "name": "MultiAccount_BlobToLakehouse",
    "properties": {
        "activities": [
            {
                "name": "GetStorageConfig",
                "type": "Lookup",
                "dependsOn": [],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0
                },
                "typeProperties": {
                    "source": {
                        "type": "SqlSource",
                        "sqlReaderQuery": "SELECT account_name, account_url, container_name, target_path FROM storage_config"
                    },
                    "dataset": {
                        "referenceName": "Lakehouse_sql_dataset",
                        "type": "DatasetReference"
                    },
                    "firstRowOnly": false
                }
            },
            {
                "name": "ForEachStorageAccount",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "GetStorageConfig",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "items": {
                        "value": "@activity('GetStorageConfig').output.value",
                        "type": "Expression"
                    },
                    "isSequential": false,
                    "activities": [
                        {
                            "name": "IngestFromBlob",
                            "type": "SynapseNotebook",
                            "dependsOn": [],
                            "typeProperties": {
                                "notebook": {
                                    "referenceName": "BlobToLakehouse_Notebook",
                                    "type": "NotebookReference"
                                },
                                "parameters": {
                                    "account_name": {
                                        "value": "@item().account_name",
                                        "type": "Expression"
                                    },
                                    "account_url": {
                                        "value": "@item().account_url",
                                        "type": "Expression"
                                    },
                                    "container_name": {
                                        "value": "@item().container_name",
                                        "type": "Expression"
                                    },
                                    "target_path": {
                                        "value": "@item().target_path",
                                        "type": "Expression"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        ],
        "annotations": []
    }
}
=================================================================================================================