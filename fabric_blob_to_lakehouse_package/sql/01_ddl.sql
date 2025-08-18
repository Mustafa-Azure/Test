-- 01_ddl.sql
-- Lakehouse tables for config + logging

CREATE TABLE IF NOT EXISTS storage_config (
  account_name      STRING,   -- e.g., VendorMak (case-insensitive; will be lowercased at runtime)
  container_name    STRING,   -- e.g., wavedata (must be lowercase in Azure)
  directory_prefix  STRING,   -- optional subfolder within the container; '' for all
  target_path       STRING    -- Lakehouse target, e.g., 'lakehouse://Files/VendorMak/Wavedata'
);

CREATE TABLE IF NOT EXISTS file_ingestion_log (
  source_account      STRING,
  source_container    STRING,
  source_prefix       STRING,
  source_uri          STRING,
  dest_root           STRING,
  relative_path       STRING,
  file_name           STRING,
  file_size           LONG,
  last_modified       TIMESTAMP,
  status              STRING,       -- ingested | skipped | error
  error_message       STRING,
  ingestion_time      TIMESTAMP
) USING DELTA;