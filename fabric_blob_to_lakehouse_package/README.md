# Fabric: Multi-Account Blob → Lakehouse (One-Page Playbook)

This package gives you a **single, simple path** to ingest **all files (recursive)** from multiple Azure Blob Storage accounts/containers into **Lakehouse Files**, and log basic metadata.

## What’s Inside
- `sql/01_ddl.sql` — Creates `storage_config` + `file_ingestion_log` tables
- `sql/02_seed.sql` — Adds your two sources (VendorMak & VendorEli) with `wavedata` container
- `notebooks/Orchestrate_Blob_To_Lakehouse.py` — Notebook that **copies files** and **logs metadata**
- `pipelines/pipeline_minimal.json` — A minimal Fabric Pipeline with a single Notebook activity

---

## Step-by-Step

### 0) Prereqs
1. Create (or pick) a **Lakehouse** in your Fabric workspace.
2. Grant the **Fabric workspace managed identity** the role **Storage Blob Data Reader** on:
   - Storage account **VendorMak**
   - Storage account **VendorEli**
3. Confirm your container is named **`wavedata`** (Azure requires lowercase). If it is `Wavedata` in your notes, use lowercase in config.

### 1) Create the Tables
Open your Lakehouse → SQL endpoint → run `sql/01_ddl.sql`.

### 2) Seed Your Sources
Run `sql/02_seed.sql` to add rows for both sources. Adjust names if needed.

### 3) Import the Notebook
Create a Notebook in Fabric named **`Orchestrate_Blob_To_Lakehouse`** and paste the contents of `notebooks/Orchestrate_Blob_To_Lakehouse.py`. Attach it to your Lakehouse.

### 4) Test the Ingestion
Run the notebook. It will:
- For each row in `storage_config`, **copy all files recursively** from Blob to **Lakehouse Files**
- Log file metadata to `file_ingestion_log`

### 5) (Optional) Orchestrate via Pipeline
Import `pipelines/pipeline_minimal.json` into Fabric Pipelines and point it at your notebook. Schedule as needed.

### 6) Add More Sources Later
Just insert a new row into `storage_config` with the new account/container and the Lakehouse target folder. Make sure to grant RBAC on that storage account.

---

## Troubleshooting
- **403 / auth errors**: Re-check RBAC on the source storage account(s) for the Fabric workspace managed identity.
- **Container not found**: Ensure container is lowercase (`wavedata`).
- **No files copied**: Verify there are files in nested folders and that the account/container names in `storage_config` are correct.
- **Performance**: This approach copies whole containers. For extremely large estates, split by prefixes (add multiple rows with `directory_prefix` values) and run in parallel.

Generated: 2025-08-18T08:22:46.018274Z