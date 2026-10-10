-- Medallion layers. Each layer is a Postgres schema so permissions, lineage
-- and "which layer is this table in?" are obvious from the qualified name.
CREATE SCHEMA IF NOT EXISTS raw;      -- file contents as-is + load metadata; append-only
CREATE SCHEMA IF NOT EXISTS staging;  -- typed/parsed rows + rejects
CREATE SCHEMA IF NOT EXISTS clean;    -- deduplicated, validated, flagged
CREATE SCHEMA IF NOT EXISTS marts;    -- aggregates for consumers
CREATE SCHEMA IF NOT EXISTS audit;    -- pipeline bookkeeping: data-quality results per run
