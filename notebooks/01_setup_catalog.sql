-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · Unity Catalog objects
-- MAGIC Creates the medallion schemas and the landing volume (safe to re-run).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.raw COMMENT 'Landing zone for raw files';
CREATE VOLUME IF NOT EXISTS workspace.raw.landing COMMENT 'Raw Olist CSV files copied from Azure ADLS';
CREATE SCHEMA IF NOT EXISTS workspace.bronze COMMENT 'Raw data as Delta, all strings, append-only';
CREATE SCHEMA IF NOT EXISTS workspace.silver COMMENT 'Cleaned, typed, deduplicated, validated';
CREATE SCHEMA IF NOT EXISTS workspace.gold COMMENT 'Star schema and business marts';
