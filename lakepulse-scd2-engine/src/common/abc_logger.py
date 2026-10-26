from pyspark.sql import SparkSession

def log_abc_metrics(
    spark: SparkSession,
    catalog: str,
    batch_id: str,
    layer: str,
    source_count: int,
    inserted_count: int,
    updated_count: int = 0,
    enforce_equality: bool = False
) -> None:
    """Logs Audit, Balance, and Control (ABC) telemetry to Unity Catalog audit table."""
    status = "PASS"
    if enforce_equality and source_count != inserted_count:
        status = "FAIL"

    spark.sql(f"""
        INSERT INTO {catalog}.audit.pipeline_audit_log
        VALUES (
            '{batch_id}',
            '{layer}',
            {source_count},
            {inserted_count},
            {updated_count},
            '{status}',
            current_timestamp()
        )
    """)

    if status == "FAIL":
        raise RuntimeError(
            f"[ABC RECONCILIATION FAILURE] Layer={layer}, Batch={batch_id}: "
            f"Expected {source_count} rows, but wrote {inserted_count} rows."
        )
