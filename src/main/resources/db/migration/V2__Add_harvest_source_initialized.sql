-- Track whether a harvest source has completed its first (forced) harvest.
-- Existing rows are treated as initialized so we do not re-force them.
ALTER TABLE harvest_source ADD COLUMN initialized BOOLEAN NOT NULL DEFAULT true;
