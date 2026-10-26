from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils

TRACKED_COLUMNS = [
    "full_name",
    "email",
    "credit_tier",
    "credit_limit",
    "risk_score",
    "address_city"
]

def run_scd2_merge(spark: SparkSession, catalog: str, batch_id: str):
    bronze_table = f"{catalog}.bronze.customer_events_raw"
    silver_table = f"{catalog}.silver.dim_customer_scd2"

    window_spec = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

    incoming_df = (
        spark.read.table(bronze_table)
        .filter(F.col("_batch_id") == batch_id)
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("credit_limit", F.col("credit_limit").cast("decimal(18,2)"))
        .withColumn("risk_score", F.col("risk_score").cast("int"))
        .withColumn("effective_start_ts", F.to_timestamp("updated_at"))
        .withColumn(
            "row_hash",
            F.sha2(
                F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in TRACKED_COLUMNS]),
                256
            )
        )
    )

    silver_delta = DeltaTable.forName(spark, silver_table)
    active_silver_df = silver_delta.toDF().filter(F.col("is_current") == True)

    changed_records_df = (
        incoming_df.alias("inc")
        .join(
            active_silver_df.select("customer_id", "row_hash").alias("sil"),
            on="customer_id",
            how="inner"
        )
        .filter(F.col("inc.row_hash") != F.col("sil.row_hash"))
        .select("inc.*")
    )

    staged_part_a = incoming_df.withColumn("merge_key", F.col("customer_id"))
    staged_part_b = changed_records_df.withColumn("merge_key", F.lit(None).cast("string"))

    staged_updates_df = (
        staged_part_a.unionByName(staged_part_b)
        .withColumn(
            "customer_sk",
            F.sha2(F.concat_ws("||", F.col("customer_id"), F.col("effective_start_ts").cast("string")), 256)
        )
        .withColumn("effective_end_ts", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
        .withColumn("is_current", F.lit(True))
        .withColumn("batch_id", F.lit(batch_id))
    )

    (
        silver_delta.alias("target")
        .merge(
            source=staged_updates_df.alias("source"),
            condition="target.customer_id = source.merge_key AND target.is_current = true"
        )
        .whenMatchedUpdate(
            condition="target.row_hash <> source.row_hash",
            set={
                "is_current": F.lit(False),
                "effective_end_ts": "source.effective_start_ts"
            }
        )
        .whenNotMatchedInsert(
            values={
                "customer_sk": "source.customer_sk",
                "customer_id": "source.customer_id",
                "full_name": "source.full_name",
                "email": "source.email",
                "credit_tier": "source.credit_tier",
                "credit_limit": "source.credit_limit",
                "risk_score": "source.risk_score",
                "address_city": "source.address_city",
                "row_hash": "source.row_hash",
                "effective_start_ts": "source.effective_start_ts",
                "effective_end_ts": "source.effective_end_ts",
                "is_current": "source.is_current",
                "batch_id": "source.batch_id"
            }
        )
        .execute()
    )

    latest_metrics = (
        silver_delta.history(1)
        .select("operationMetrics")
        .collect()[0]["operationMetrics"]
    )
    inserted_rows = int(latest_metrics.get("numTargetRowsInserted", 0))
    updated_rows = int(latest_metrics.get("numTargetRowsUpdated", 0))
    source_rows = incoming_df.count()

    spark.sql(f"""
        INSERT INTO {catalog}.audit.pipeline_audit_log
        VALUES ('{batch_id}', 'SILVER_SCD2', {source_rows}, {inserted_rows}, {updated_rows}, 'PASS', current_timestamp())
    """)
    print(f"[SCD2 COMPLETE] Source Deduplicated: {source_rows} | Inserted: {inserted_rows} | Expired: {updated_rows}")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    catalog = spark.conf.get("bundle.catalog", "dev_fin_catalog")
    batch_id = dbutils.jobs.taskValues.get(taskKey="t1_api_to_volume", key="batch_id", debugValue="LOCAL_TEST")

    run_scd2_merge(spark, catalog, batch_id)
