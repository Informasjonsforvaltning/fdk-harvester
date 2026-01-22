-- Initial schema for FDK Harvester

-- Harvest source table
CREATE TABLE IF NOT EXISTS harvest_source (
    id BIGSERIAL PRIMARY KEY,
    uri VARCHAR(2048) NOT NULL UNIQUE,
    checksum VARCHAR(64) NOT NULL,
    issued TIMESTAMP NOT NULL
);

-- Resources table
CREATE TABLE IF NOT EXISTS resources (
    uri VARCHAR(2048) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    fdk_id VARCHAR(255) NOT NULL,
    removed BOOLEAN NOT NULL DEFAULT FALSE,
    issued TIMESTAMP NOT NULL,
    modified TIMESTAMP NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    harvest_source_id BIGINT NOT NULL,
    CONSTRAINT fk_resources_harvest_source FOREIGN KEY (harvest_source_id) REFERENCES harvest_source(id)
);

-- Indexes for resources
CREATE INDEX IF NOT EXISTS idx_resources_type ON resources(type);
CREATE INDEX IF NOT EXISTS idx_resources_fdk_id ON resources(fdk_id);
CREATE INDEX IF NOT EXISTS idx_resources_removed ON resources(removed);
CREATE INDEX IF NOT EXISTS idx_resources_harvest_source_id ON resources(harvest_source_id);
