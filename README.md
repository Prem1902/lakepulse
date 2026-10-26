# LakePulse: Enterprise FinTech Customer SCD Type 2 Engine on Databricks

## Overview
LakePulse is a metadata-driven Lakehouse ingestion and historical tracking framework built natively on **Databricks**, **Unity Catalog**, and **Delta Lake**. It extracts high-frequency customer KYC and credit risk updates from a paginated REST API, lands raw payloads into **Unity Catalog Managed Volumes**, streams incrementally into Bronze using **Auto Loader (`cloudFiles`)**, and maintains full historical point-in-time auditability in Silver using an atomic **PySpark SCD Type 2 (`MERGE INTO`)** pattern.

## Key Architectural Highlights
- **Decoupled REST API Ingestion:** Uses `requests.Session` with exponential backoff (`HTTP 429/5xx` resilience) to land NDJSON batches into Unity Catalog Managed Volumes (`/Volumes/{catalog}/landing/raw_api_json`), avoiding fragile Spark UDF HTTP anti-patterns.
- **Cost-Optimized Auto Loader (`trigger(availableNow=True)`):** Combines Structured Streaming state management, RocksDB checkpointing, and automatic schema evolution (`addNewColumns`) with batch Job Compute pricing.
- **Single-Pass Atomic SCD Type 2 Merge:** Computes a null-safe `SHA-256` hash (`row_hash`) across mutable business attributes and uses a staged `merge_key` union pattern to expire outdated records (`is_current = false`) and insert new active versions (`is_current = true`) in a single atomic Delta transaction.
- **Audit, Balance, and Control (ABC) Framework:** Validates record counts between API landing, Bronze ingestion, and Silver Delta `operationMetrics` (`numTargetRowsInserted`, `numTargetRowsUpdated`), halting downstream processing on reconciliation discrepancies.
- **Unity Catalog Governance & Liquid Clustering:** Enforces 3-level namespace isolation (`{env}_fin_catalog`), dynamic PII column masking (`mask_email`), and replaces legacy Hive partitioning with **Delta Liquid Clustering (`CLUSTER BY (customer_id, is_current)`)** for sub-second point lookups.
- **FinOps-Driven Compute Strategy via DABs:** Deployed via **Databricks Asset Bundles (`databricks.yml`)**, running Single-Node Standard clusters in `dev` and Photon-accelerated, Spot-worker autoscaling **Job Clusters** in `prod`.
