from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils

def ingest_volume_to_bronze(spark: SparkSession, catalog: str, batch_id: str, expected_count: int):
    volume_path = f"/Volumes/{catalog}/landing/raw_api_json/payloads"
    schema_location = f"/Volumes/{catalog}/landing/raw_api_json/schemas/bronze_customer"
    checkpoint_location = f"/Volumes/{catalog}/landing/raw_api_json/checkpoints/bronze_customer"
    bronze_table = f"{catalog}.bronze.customer_events_raw"

    raw_stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(volume_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_bronze_load_ts", F.current_timestamp())
    )

    query = (
        raw_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )
    query.awaitTermination()

    bronze_batch_count = (
        spark.read.table(bronze_table)
        .filter(F.col("_batch_id") == batch_id)
        .count()
    )

    status = "PASS" if bronze_batch_count == expected_count else "FAIL"
    spark.sql(f"""
        INSERT INTO {catalog}.audit.pipeline_audit_log
        VALUES ('{batch_id}', 'BRONZE', {expected_count}, {bronze_batch_count}, 0, '{status}', current_timestamp())
    """)

    if status == "FAIL":
        raise RuntimeError(
            f"[ABC CONTROL FAILURE] Batch {batch_id}: Expected {expected_count} rows from API, "
            f"but found {bronze_batch_count} in {bronze_table}."
        )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    catalog = spark.conf.get("bundle.catalog", "dev_fin_catalog")

    batch_id = dbutils.jobs.taskValues.get(taskKey="t1_api_to_volume", key="batch_id", debugValue="LOCAL_TEST")
    expected_count = dbutils.jobs.taskValues.get(taskKey="t1_api_to_volume", key="api_landed_count", debugValue=2)

    ingest_volume_to_bronze(spark, catalog, batch_id, expected_count)
