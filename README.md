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

# LakePulse: Enterprise FinTech Customer SCD Type 2 Engine on Databricks

![Databricks](https://img.shields.io/badge/Databricks-Runtime_15.4_LTS-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Liquid_Clustering-00ADD8?logo=databricks&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Governance-Unity_Catalog-1B3139)
![CI/CD](https://img.shields.io/badge/Deployment-Databricks_Asset_Bundles-2088FF)
![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)

## 1. Executive Summary
**LakePulse** is a production-grade, metadata-driven Lakehouse ingestion and historical tracking framework built natively on **Databricks**, **Unity Catalog**, and **Delta Lake**. 

Designed for high-compliance FinTech environments, it extracts paginated customer KYC and credit risk updates from an external REST API, lands raw payloads into **Unity Catalog Managed Volumes**, streams incrementally into Bronze using **Auto Loader (`cloudFiles`)**, and maintains full point-in-time historical auditability in Silver using a **single-pass atomic PySpark SCD Type 2 (`MERGE INTO`)** engine. Every stage is wrapped in an automated **Audit, Balance, and Control (ABC)** reconciliation framework.

---

## 2. End-to-End System Architecture

```mermaid
flowchart TB
    subgraph External["External Source Systems"]
        API["FinTech KYC & Credit Risk REST API<br/>(Paginated JSON + Rate Limits)"]
    end

    subgraph UC["Databricks Unity Catalog ({env}_fin_catalog)"]
        direction TB
        
        subgraph Landing["Schema: landing"]
            VOL[("Managed Volume: raw_api_json<br/>/payloads/dt=YYYY-MM-DD/<br/>/schemas/ & /checkpoints/")]
        end

        subgraph Bronze["Schema: bronze"]
            BRZ[("Delta Table: customer_events_raw<br/>(Append-Only + _rescued_data)")]
        end

        subgraph Silver["Schema: silver"]
            SLV[("Delta Table: dim_customer_scd2<br/>Liquid Clustered: (customer_id, is_current)<br/>Dynamic PII Masking: mask_email()")]
        end

        subgraph Audit["Schema: audit"]
            ABC[("Delta Table: pipeline_audit_log<br/>(ABC Telemetry & Row Reconciliation)")]
        end
    end

    subgraph Compute["Orchestration: Databricks Workflows (Job Compute)"]
        T1["Task 1: t1_api_to_volume.py<br/>(Requests + Exponential Backoff)"]
        T2["Task 2: t2_volume_to_bronze.py<br/>(Auto Loader + Trigger.AvailableNow)"]
        T3["Task 3: t3_bronze_to_silver.py<br/>(SHA-256 Diff + Atomic SCD2 Merge)"]
        
        T1 -->|"dbutils.jobs.taskValues<br/>(batch_id, api_landed_count)"| T2
        T2 -->|"Schema Evolution<br/>(addNewColumns)"| T3
    end

    API ==>|"HTTPS GET (Retry 429/5xx)"| T1
    T1 ==>|"Writes NDJSON"| VOL
    VOL ==>|"cloudFiles Stream"| T2
    T2 ==>|"Delta Append"| BRZ
    BRZ ==>|"Deduplicated Batch"| T3
    T3 ==>|"Atomic MERGE INTO"| SLV

    T2 -.->|"Validates API == Bronze Count"| ABC
    T3 -.->|"Logs Delta operationMetrics"| ABC