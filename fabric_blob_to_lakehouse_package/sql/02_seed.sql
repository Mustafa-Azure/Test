-- 02_seed.sql
-- Seed rows for your two sources (copy entire container recursively)

-- Adjust the target_path if you want a different folder under Lakehouse Files
DELETE FROM storage_config;

INSERT INTO storage_config VALUES
('VendorMak', 'wavedata', '', 'lakehouse://Files/VendorMak/Wavedata'),
('VendorEli', 'wavedata', '', 'lakehouse://Files/VendorEli/Wavedata');